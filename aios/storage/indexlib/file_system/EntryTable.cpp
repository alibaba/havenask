/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/file_system/EntryTable.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <map>
#include <ostream>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/EntryMeta.h"
#include "indexlib/file_system/EntryTableJsonizable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, EntryTable);

EntryTable::EntryTable(const std::string& name, const std::string& outputRoot, bool isRootEntryMetaLazy)
    : _name(name)
    , _outputRoot(outputRoot)
    , _patchRoot(GetPatchRoot(name))
    , _rootEntryMeta("", "", &_outputRoot)
{
    _rootEntryMeta.SetLazy(isRootEntryMetaLazy);
    _rootEntryMeta.SetDir();
    _rootEntryMeta.SetOwner(true);
    _rootEntryMeta.SetMutable(true);
    AUTIL_LOG(INFO, "Create EntryTable [%s] [%p] in [%s]", _name.c_str(), this, _outputRoot.c_str());
}

EntryTable::~EntryTable() { Clear(); }

std::string EntryTable::GetEntryTableFileName(int version)
{
    return ENTRY_TABLE_FILE_NAME_PREFIX + std::string(".") + std::to_string(version);
}

void EntryTable::Init() { Clear(); }

ErrorCode EntryTable::ToString(const std::vector<std::string>& wishFileList,
                               const std::vector<std::string>& wishDirList,
                               const std::vector<std::string>& filterDirList, string* result)
{
    ScopedLock lock(_lock);
    RETURN_IF_FS_ERROR(OptimizeMemoryStructures(), "");
    EntryTableJsonizable json;
    RETURN_IF_FS_ERROR(json.CollectFromEntryTable(this, wishFileList, wishDirList, filterDirList), "");
    *result = autil::legacy::ToJsonString(json);
    return FSEC_OK;
}

ErrorCode EntryTable::OptimizeMemoryStructures()
{
    // TODO: optimize members describe roots
    ScopedLock lock(_lock);
    std::map<std::string, int64_t> newPackageFileLengths;
    std::unordered_set<std::string> usefulPackageGroups;
    for (const auto& iter : _entryMetaMap) {
        const EntryMeta& meta = iter.second;
        if (meta.IsInPackage()) {
            std::string physicalPath = meta.GetRawFullPhysicalPath();
            if (_packageFileLengths.count(physicalPath) == 1) {
                newPackageFileLengths[physicalPath] = _packageFileLengths.at(physicalPath);
                string packageFileGroup;
                RETURN_IF_FS_ERROR(PackageFileMeta::GetPackageFileGroup(physicalPath, &packageFileGroup), "");
                usefulPackageGroups.insert(packageFileGroup);
            }
        }
    }
    for (const auto& iter : _packageFileLengths) {
        std::string path = iter.first;
        if (path.find(PACKAGE_FILE_META_SUFFIX) == string::npos) {
            continue;
        }
        string packageFileGroup;
        RETURN_IF_FS_ERROR(PackageFileMeta::GetPackageFileGroup(path, &packageFileGroup), "");
        if (usefulPackageGroups.count(packageFileGroup)) {
            newPackageFileLengths[path] = iter.second;
        }
    }
    _packageFileLengths.swap(newPackageFileLengths);
    return FSEC_OK;
}

string EntryTable::DebugString() const
{
    ScopedLock lock(_lock);
    std::stringstream ss;
    ss << "Entry table: " << endl;
    ss << "output root: " << _outputRoot << endl;
    ss << "patch root: " << _patchRoot << endl;
    ss << "files: " << endl;
    for (const auto& pair : _entryMetaMap) {
        ss << "map key: " << pair.first << " meta: " << pair.second.DebugString() << endl;
    }
    ss << "package files: " << endl;
    for (const auto& pair : _packageFileLengths) {
        ss << pair.first << ": " << pair.second << endl;
    }
    return ss.str();
}

bool EntryTable::Commit(int versionId, const std::vector<std::string>& wishFileList,
                        const std::vector<std::string>& wishDirList, const std::vector<std::string>& filterDirList,
                        FenceContext* fenceContext)
{
    ScopedLock lock(_lock);
    if (!ValidatePackageFileLengths()) {
        assert(false);
        return false;
    }
    assert(_patchRoot == GetPatchRoot(_name));
    string oldPatchRoot = _patchRoot;
    string newPatchRoot = GetPatchRoot(autil::StringUtil::toString(versionId));
    auto [ec, exist] = FslibWrapper::IsExist(oldPatchRoot);
    if (unlikely(ec != FSEC_OK)) {
        AUTIL_LOG(ERROR, "EntryTable patch root [%s] IsExist test failed", oldPatchRoot.c_str());
        return false;
    }
    if (exist) {
        auto ec = FslibWrapper::Rename(oldPatchRoot, newPatchRoot /*no fence*/).Code();
        if (ec != FSEC_OK) {
            AUTIL_LOG(ERROR, "EntryTable dump failed. Rename patch root [%s] => [%s] failed, ec [%d]",
                      oldPatchRoot.c_str(), newPatchRoot.c_str(), ec);
            return false;
        }
        _patchRoot = newPatchRoot;
    }
    std::string jsonStr;
    ec = ToString(wishFileList, wishDirList, filterDirList, &jsonStr);
    if (ec != FSEC_OK) {
        return false;
    }
    std::string fileName = PathUtil::JoinPath(_outputRoot, EntryTable::GetEntryTableFileName(versionId));
    ec = FslibWrapper::AtomicStore(fileName, jsonStr.data(), jsonStr.size(), true, fenceContext).Code();
    if (unlikely(ec != FSEC_OK)) {
        AUTIL_LOG(ERROR, "EntryTable dump failed. Store file [%s] failed!, ec [%d]", fileName.c_str(), ec);
        // _patchRoot = oldPatchRoot;
        return false;
    }

    return true;
}

bool EntryTable::CommitPreloadDependence(FenceContext* fenceContext)
{
    ScopedLock lock(_lock);
    std::string jsonStr;
    if (ToString({}, {}, {}, &jsonStr) != FSEC_OK) {
        return false;
    }
    std::string fileName = PathUtil::JoinPath(_outputRoot, ENTRY_TABLE_PRELOAD_FILE_NAME);
    auto ec = FslibWrapper::AtomicStore(fileName, jsonStr.data(), jsonStr.size(), /*removeIfExist=*/false, fenceContext)
                  .Code();
    if (unlikely(ec != FSEC_OK)) {
        AUTIL_LOG(ERROR, "PreloadDependence EntryTable dump failed. Store file [%s] failed!, ec [%d]", fileName.c_str(),
                  ec);
        return false;
    }
    return true;
}

void EntryTable::UpdateAfterCommit(const std::vector<string>& wishFileList, const std::vector<string>& wishDirList)
{
    ScopedLock lock(_lock);
    for (const string& file : wishFileList) {
        auto it = _entryMetaMap.find(file);
        if (it != _entryMetaMap.end()) {
            EntryMeta& meta = it->second;
            if (!meta.IsMemFile()) {
                meta.SetOwner(false);
                meta.SetMutable(false);
            }
        }
    }
    for (const string& dir : wishDirList) {
        string addSlashDirPath = dir + "/";
        auto it = _entryMetaMap.find(dir);
        if (it != _entryMetaMap.end()) {
            EntryMeta& meta = it->second;
            if (!meta.IsMemFile()) {
                meta.SetOwner(false);
                meta.SetMutable(false);
            }
            ++it;

            while (it != _entryMetaMap.end() && (it->first.compare(0, addSlashDirPath.size(), addSlashDirPath) == 0)) {
                if (!it->second.IsMemFile()) {
                    it->second.SetMutable(false);
                    it->second.SetOwner(false);
                }
                ++it;
            }
        }
    }
}

ErrorCode EntryTable::MakeDirectory(const string& path, bool recursive, vector<EntryMeta>* metas)
{
    ScopedLock lock(_lock);
    // TODO: some problems exist here
    metas->clear();
    if (IsExist(path) && (!recursive || !IsDir(path))) {
        return FSEC_EXIST;
    }

    if (!recursive) {
        if (unlikely(!IsExist(PathUtil::GetParentDirPath(path)))) {
            AUTIL_LOG(DEBUG, "make dir [%s] failed, parent dir not exist", path.c_str());
            return FSEC_NOENT;
        }
        const string* root = FindPhysicalRootForWrite(path);
        EntryMeta meta = EntryMeta::Dir(path, path, root);
        meta.SetMutable(root == &_outputRoot);
        meta.SetOwner(true);
        metas->push_back(meta);
        return FSEC_OK;
    }

    string tmpPath = path;
    while (!tmpPath.empty() && _entryMetaMap.count(tmpPath) == 0) {
        const string* root = FindPhysicalRootForWrite(path);
        EntryMeta meta = EntryMeta::Dir(tmpPath, tmpPath, root);
        meta.SetMutable(root == &_outputRoot);
        meta.SetOwner(true);
        metas->push_back(meta);
        tmpPath = PathUtil::GetParentDirPath(tmpPath);
    }
    std::reverse(metas->begin(), metas->end());
    return FSEC_OK;
}

FSResult<EntryMeta> EntryTable::CreateFile(const string& path)
{
    ScopedLock lock(_lock);
    assert(!path.empty() && path[0] != '/' && path[path.size() - 1] != '/');

    if (IsExist(path)) {
        return {FSEC_EXIST, GetEntryMeta(path).result};
    }

    auto parent = PathUtil::GetParentDirPath(path);
    if (!parent.empty()) {
        FSResult<EntryMeta> parentDirMetaRet = GetEntryMeta(parent);
        const EntryMeta& parentDirMeta = parentDirMetaRet.result;
        if (parentDirMetaRet.ec != FSEC_OK || !parentDirMeta.IsDir()) {
            static std::string emptyLogicalPath = "";
            static std::string emptyPhysicalPath = "";
            static std::string emptyPhysicalRoot = "";
            return {FSEC_NOENT, EntryMeta::NoEnt(emptyLogicalPath, emptyPhysicalPath, &emptyPhysicalRoot)};
        }
    }

    EntryMeta meta;
    meta.SetPhysicalRoot(FindPhysicalRootForWrite(path));
    meta.SetPhysicalPath(path);
    meta.SetLogicalPath(path);
    meta.SetMutable(true);
    meta.SetOwner(true);

    return {FSEC_OK, meta};
}

const std::string* EntryTable::FindPhysicalRootForWrite(const std::string& path)
{
    ScopedLock lock(_lock);
    auto pos = path.find('/');
    std::string rootDir = (pos == std::string::npos ? path : path.substr(0, pos));
    FSResult<EntryMeta> rootEntryMetaRet = GetEntryMeta(rootDir);
    const EntryMeta& rootEntryMeta = rootEntryMetaRet.result;
    if (rootEntryMetaRet.ec == FSEC_OK) {
        if (rootEntryMeta.IsDir()) {
            return (rootEntryMeta.IsMutable() || rootEntryMeta.GetPhysicalRoot() != _outputRoot) ? &_outputRoot
                                                                                                 : &_patchRoot;
        }
        assert(false);
    }
    return &_outputRoot;
}

bool EntryTable::UpdateFileSize(const std::string& path, int64_t length)
{
    ScopedLock lock(_lock);
    auto it = _entryMetaMap.find(path);
    if (it == _entryMetaMap.end()) {
        return false;
    }
    it->second.SetLength(length);
    return true;
}

bool EntryTable::IsExist(const string& path, bool* isDir) const
{
    ScopedLock lock(_lock);
    if (path.empty()) // Root Dir
    {
        return true;
    }

    auto it = _entryMetaMap.find(path);
    if (it == _entryMetaMap.end()) {
        return false;
    }

    if (isDir) {
        *isDir = it->second.IsDir();
    }
    return true;
}

bool EntryTable::IsDir(const string& path) const
{
    if (path.empty()) {
        return true;
    }
    ScopedLock lock(_lock);
    FSResult<EntryMeta> metaRet = GetEntryMeta(path);
    const EntryMeta& meta = metaRet.result;
    return (metaRet.ec == FSEC_OK && meta.IsDir());
}
// @param logicalPath: eg. segment_1/index/name/data
FSResult<EntryMeta> EntryTable::GetAncestorLazyEntryMeta(const std::string& logicalPath) const
{
    assert(_entryMetaMap.count(logicalPath) == 0);
    // assert(!logicalPath.empty());
    ScopedLock lock(_lock);
    std::string parentPath = logicalPath;
    while (!parentPath.empty()) {
        FSResult<EntryMeta> parentMetaRet = GetEntryMeta(parentPath);
        const EntryMeta& parentMeta = parentMetaRet.result;
        if (parentMetaRet.ec == FSEC_OK) {
            return parentMeta.IsLazy() ? parentMetaRet : FSResult<EntryMeta>({FSEC_NOENT, EntryMeta()});
        }
        parentPath = PathUtil::GetParentDirPath(parentPath);
    }
    return _rootEntryMeta.IsLazy() ? FSResult<EntryMeta>({FSEC_OK, _rootEntryMeta})
                                   : FSResult<EntryMeta>({FSEC_NOENT, EntryMeta()});
}

std::vector<EntryMeta> EntryTable::ListDir(const string& dirPath, bool recursive) const
{
    ScopedLock lock(_lock);

    assert(dirPath.empty() || dirPath[0] != '/');
    assert(PathUtil::NormalizePath(dirPath) == dirPath);

    std::vector<EntryMeta> entryMetas;
    string addSlashDirPath = dirPath + "/";
    bool isRoot = dirPath.empty();
    auto it = _entryMetaMap.begin();
    if (!isRoot) {
        it = _entryMetaMap.find(dirPath);
        if (unlikely(it == _entryMetaMap.end())) {
            return entryMetas;
        }
        it = _entryMetaMap.lower_bound(addSlashDirPath);
    }

    while (it != _entryMetaMap.end() &&
           (isRoot || it->first.compare(0, addSlashDirPath.size(), addSlashDirPath) == 0)) {
        const string& path = it->first;
        if (recursive || path.find("/", addSlashDirPath.size()) == string::npos) {
            entryMetas.emplace_back(it->second);
        }
        ++it;
    }
    return entryMetas;
}

FSResult<EntryMeta> EntryTable::AddWithoutCheck(const EntryMeta& entryMeta)
{
    AUTIL_LOG(DEBUG, "AddEntryMeta: %s", entryMeta.DebugString().c_str());
    // check it is really exist on disk when add a version file via mount
    // because old ut may remove version file but leave entry_meta file there.
    // now a version file will always along with a entry_table file beacuse LogicaFS support commit with finalDumpFile
    // assert(entryMeta.IsMutable() || entryMeta.GetLogicalPath().find("version.") == string::npos ||
    //        FslibWrapper::IsExist(entryMeta.GetFullPhysicalPath()).GetOrThrow());

    assert(!entryMeta.GetLogicalPath().empty());
    assert(entryMeta.GetLogicalPath()[0] != '/');
    assert(PathUtil::NormalizePath(entryMeta.GetLogicalPath()) == entryMeta.GetLogicalPath());

    ScopedLock lock(_lock);
    auto ret = _entryMetaMap.emplace(entryMeta.GetLogicalPath(), entryMeta);
    bool isSuccess = ret.second;
    EntryMeta& existEntryMeta = ret.first->second;
    if (isSuccess) {
        return {FSEC_OK, existEntryMeta};
    }
    AUTIL_LOG(DEBUG, "already exist, replace [%s] to [%s]", existEntryMeta.DebugString().c_str(),
              entryMeta.DebugString().c_str());
    if (_rootEntryMeta.IsLazy()) {
        // offline
        existEntryMeta = entryMeta;
    } else {
        // online, master will dp index to local and commit entryTable with orginal root
        existEntryMeta.Accept(entryMeta); // should not change _rawPhysicalRoot;
    }
    return {FSEC_OK, existEntryMeta};
}

FSResult<EntryMeta> EntryTable::AddEntryMeta(const EntryMeta& entryMeta) { return AddWithoutCheck(entryMeta); }

void EntryTable::Delete(const EntryMeta& meta)
{
    ScopedLock lock(_lock);
    Delete(meta.GetLogicalPath());
}

void EntryTable::Delete(const std::string& path)
{
    ScopedLock lock(_lock);
    _entryMetaMap.erase(path);
}

// lazy loaded file may not be found
FSResult<EntryMeta> EntryTable::GetEntryMeta(const std::string& logicalPath) const
{
    if (logicalPath.empty()) {
        return {FSEC_OK, _rootEntryMeta};
    }
    ScopedLock lock(_lock);
    auto it = _entryMetaMap.find(logicalPath);
    if (it == _entryMetaMap.end()) {
        return {FSEC_NOENT, EntryMeta()};
    }

    return {FSEC_OK, it->second};
}

FSResult<EntryMeta> EntryTable::GetEntryMetaMayLazy(const std::string& logicalPath)
{
    FSResult<EntryMeta> entryMetaRet = GetEntryMeta(logicalPath);
    if (entryMetaRet.ec == FSEC_OK) {
        return entryMetaRet;
    }

    FSResult<EntryMeta> ancestorLazyEntryMetaRet = GetAncestorLazyEntryMeta(logicalPath);
    if (ancestorLazyEntryMetaRet.ec != FSEC_OK) {
        return ancestorLazyEntryMetaRet;
    }
    const auto& ancestorLazyEntryMeta = ancestorLazyEntryMetaRet.result;
    assert(ancestorLazyEntryMeta.IsDir());
    assert(ancestorLazyEntryMeta.IsLazy());
    const std::string& relativeLogicalPath =
        ancestorLazyEntryMeta.GetLogicalPath().empty()
            ? logicalPath
            : logicalPath.substr(ancestorLazyEntryMeta.GetLogicalPath().size() + 1);
    std::string physicalFullPath = PathUtil::JoinPath(ancestorLazyEntryMeta.GetFullPhysicalPath(), relativeLogicalPath);
    FSResult<fslib::PathMeta> pathMetaRet = FslibWrapper::GetPathMeta(physicalFullPath);
    if (pathMetaRet.ec != FSEC_OK) {
        return {pathMetaRet.ec, EntryMeta()};
    }

    // complte the path from ancestorLazyEntryMeta tp lo
    // eg: logicalPath = s0/index/name/data
    std::string path = "";
    std::vector<std::string> dirNames = StringUtil::split(relativeLogicalPath, "/"); // eg. {"index", "name", "data"}
    for (int32_t i = 0; i < (int32_t)dirNames.size() - 1; ++i) {                     // eg. "index", "index/name"
        path = PathUtil::JoinPath(path, dirNames[i]);                                // eg. "index", "index/name"
        EntryMeta dirMeta = ancestorLazyEntryMeta;
        dirMeta.SetLogicalPath(
            PathUtil::JoinPath(ancestorLazyEntryMeta.GetLogicalPath(), path)); // "s0/index", "s0/index/name"
        dirMeta.SetPhysicalPath(
            PathUtil::JoinPath(ancestorLazyEntryMeta.GetPhysicalPath(), path)); // "s0/index", "s0/index/name"
        ErrorCode ec = AddEntryMeta(dirMeta).ec;
        if (unlikely(ec != FSEC_OK)) {
            return {FSEC_NOENT, EntryMeta()};
        }
    }

    EntryMeta entryMeta = ancestorLazyEntryMeta;
    entryMeta.SetLogicalPath(logicalPath);
    entryMeta.SetPhysicalPath(PathUtil::JoinPath(ancestorLazyEntryMeta.GetPhysicalPath(), relativeLogicalPath));
    if (pathMetaRet.result.isFile) {
        entryMeta.SetLength(pathMetaRet.result.length);
        entryMeta.SetMutable(false);
    } else {
        entryMeta.SetDir();
    }
    entryMetaRet = AddEntryMeta(entryMeta);
    assert(entryMetaRet.ec == FSEC_OK);
    return entryMetaRet;
}

bool EntryTable::SetEntryMetaMutable(const std::string& logicalPath, bool isMutable)
{
    ScopedLock lock(_lock);
    auto it = _entryMetaMap.find(logicalPath);
    if (it == _entryMetaMap.end()) {
        return false;
    }
    it->second.SetMutable(isMutable);
    return true;
}

bool EntryTable::SetEntryMetaIsMemFile(const std::string& logicalPath, bool isMemFile)
{
    ScopedLock lock(_lock);
    auto it = _entryMetaMap.find(logicalPath);
    if (it == _entryMetaMap.end()) {
        return false;
    }
    it->second.SetIsMemFile(isMemFile);
    return true;
}

bool EntryTable::SetEntryMetaLazy(const std::string& logicalPath, bool isLazy)
{
    ScopedLock lock(_lock);
    auto it = _entryMetaMap.find(logicalPath);
    if (unlikely(it == _entryMetaMap.end())) {
        return false;
    }
    it->second.SetLazy(isLazy);
    return true;
}

ErrorCode EntryTable::Rename(const std::string& src, const std::string& dest, FenceContext* fenceContext,
                             bool inRecoverPhase)
{
    ScopedLock lock(_lock);
    if (IsExist(dest)) {
        AUTIL_LOG(ERROR, "Rename failed, dest path [%s] already exist", dest.c_str());
        return FSEC_EXIST;
    }
    auto it = _entryMetaMap.find(src);
    if (it == _entryMetaMap.end()) {
        AUTIL_LOG(ERROR, "Rename failed, src path [%s] not exists!", src.c_str());
        return FSEC_NOENT;
    }
    EntryMeta& srcMeta = it->second;
    if (unlikely(srcMeta.IsInPackage())) {
        AUTIL_LOG(ERROR, "Rename failed, not support rename in package file [%s]", src.c_str());
        return FSEC_NOTSUP;
    }
    if (unlikely(!srcMeta.IsOwner())) {
        AUTIL_LOG(ERROR, "Rename failed, not support rename non-owner file [%s]", src.c_str());
        return FSEC_NOTSUP;
    }

    // TODO: refactor, EntryTable should not change physical file, @qingran
    std::string srcFullPhysicalPath = srcMeta.GetFullPhysicalPath(); // dfs://part/inst_0/seg_1
    const std::string* destPhysicalRoot = FindPhysicalRootForWrite(dest);
    std::string destFullPhysicalPath = PathUtil::JoinPath(*destPhysicalRoot, dest); // dfs://part/seg_2
    auto ec = RenamePhysicalPath(srcFullPhysicalPath, destFullPhysicalPath, srcMeta.IsFile(), fenceContext);
    if (!(ec == FSEC_OK || (inRecoverPhase && (ec == FSEC_NOENT || ec == FSEC_EXIST)))) {
        return ec;
    }

    if (srcMeta.IsDir()) {
        for (const EntryMeta& oldMeta : ListDir(src, true)) {
            EntryMeta meta = oldMeta;
            std::string oldLogicalPath = meta.GetLogicalPath();
            std::string relativePath = "";
            if (!PathUtil::GetRelativePath(src, oldLogicalPath, relativePath)) {
                assert(false);
                AUTIL_LOG(ERROR, "lsit dir find [%s] not subpath of [%s]", oldLogicalPath.c_str(), src.c_str());
                return FSEC_UNKNOWN;
            }
            std::string newLogicalPath = PathUtil::JoinPath(dest, relativePath); // seg_2/attr/data
            meta.SetLogicalPath(newLogicalPath);
            meta.SetPhysicalRoot(destPhysicalRoot); // dfs://part/inst_0 -> dfs://part
            if (meta.IsInPackage()) {
                // std::string oldPhysicalPath = meta.GetPhysicalPath();              // seg_1/pack.data
                std::string oldFullPhysicalPath = oldMeta.GetFullPhysicalPath(); // dfs://part/inst_0/seg_1/pack.data
                std::string relativePath;                                        // pack.data
                if (!PathUtil::GetRelativePath(srcFullPhysicalPath, oldMeta.GetFullPhysicalPath(), relativePath)) {
                    AUTIL_LOG(ERROR, "rename failed, file [%s]'s old fullPhysicalPath [%s] is not subpath of [%s]",
                              oldMeta.GetLogicalPath().c_str(), oldMeta.GetFullPhysicalPath().c_str(),
                              srcFullPhysicalPath.c_str());
                    return FSEC_UNKNOWN;
                }

                std::string newFullPhysicalPath = PathUtil::JoinPath(destFullPhysicalPath, relativePath);
                if (!PathUtil::GetRelativePath(*destPhysicalRoot, newFullPhysicalPath, relativePath)) {
                    AUTIL_LOG(ERROR, "rename failed, file [%s]'s new fullPhysicalPath [%s] is not subpath of [%s]",
                              oldMeta.GetLogicalPath().c_str(), newFullPhysicalPath.c_str(), destPhysicalRoot->c_str());
                    return FSEC_UNKNOWN;
                }
                meta.SetPhysicalPath(relativePath); // seg_2/pack.data
            } else {
                meta.SetPhysicalPath(newLogicalPath);
            }
            _entryMetaMap[newLogicalPath] = std::move(meta);
            _entryMetaMap.erase(oldLogicalPath);
        }
    }
    srcMeta.SetLogicalPath(dest);
    srcMeta.SetPhysicalRoot(destPhysicalRoot);
    srcMeta.SetPhysicalPath(dest); // can do like this because not support rename file in package
    _entryMetaMap[dest] = std::move(srcMeta);
    _entryMetaMap.erase(src);
    return FSEC_OK;
}

void EntryTable::Clear()
{
    ScopedLock lock(_lock);
    _packageFileLengths.clear();
    _packageFileLengthsCache.clear();
    _entryMetaMap.clear();
    _physicalRoots.clear();
    AUTIL_LOG(INFO, "Clear EntryTable [%s] [%p] in [%s]", _name.c_str(), this, _outputRoot.c_str());
}

const std::string* EntryTable::GetPhysicalRootPointer(const std::string& physicalRoot)
{
    ScopedLock lock(_lock);
    assert(PathUtil::NormalizePath(physicalRoot) == physicalRoot);
    auto it = _physicalRoots.find(physicalRoot);
    if (it == _physicalRoots.end()) {
        return &(*_physicalRoots.emplace(physicalRoot).first);
    }
    return &(*it);
}

bool EntryTable::TEST_GetPhysicalPath(const std::string& logicalPath, std::string& physicalPath) const
{
    ScopedLock lock(_lock);
    FSResult<EntryMeta> metaRet = GetEntryMeta(logicalPath);
    if (metaRet.ec != FSEC_OK) {
        return false;
    }
    const EntryMeta& meta = metaRet.result;
    physicalPath = meta.GetFullPhysicalPath();
    return true;
}

bool EntryTable::TEST_GetPhysicalInfo(const std::string& logicalPath, std::string& physicalRoot,
                                      std::string& relativePath, bool& inPackage, bool& isDir) const
{
    ScopedLock lock(_lock);
    FSResult<EntryMeta> metaRet = GetEntryMeta(logicalPath);
    if (metaRet.ec != FSEC_OK) {
        return false;
    }
    const EntryMeta& meta = metaRet.result;
    isDir = meta.IsDir();
    inPackage = meta.IsInPackage();
    physicalRoot = meta.GetPhysicalRoot();
    relativePath = meta.GetPhysicalPath();
    if (physicalRoot.find(FILE_SYSTEM_PATCH_DOT_PREFIX) != std::string::npos) {
        auto parent = PathUtil::GetParentDirPath(physicalRoot);
        std::string suffix;
        PathUtil::GetRelativePath(parent, physicalRoot, suffix);
        relativePath = PathUtil::JoinPath(suffix, relativePath);
        physicalRoot = parent;
    }
    return true;
}

std::string EntryTable::GetPatchRoot(const std::string& name) const
{
    return PathUtil::JoinPath(_outputRoot, string(FILE_SYSTEM_PATCH_DOT_PREFIX) + name + FILE_SYSTEM_INNER_SUFFIX);
}

void EntryTable::UpdatePackageDataFile(const std::string& logicalPath, const std::string& physicalPath, int64_t offset)
{
    ScopedLock lock(_lock);
    auto it = _entryMetaMap.find(logicalPath);
    if (it == _entryMetaMap.end()) {
        return;
    }
    it->second.SetOffset(offset);

    // get relative path
    auto physicalRoot = it->second.GetPhysicalRoot();
    assert(physicalPath.find(physicalRoot) == 0);
    auto relativePath = physicalPath.substr(physicalRoot.size() + 1);
    it->second.SetPhysicalPath(relativePath);
}

void EntryTable::UpdatePackageMetaFile(const std::string& physicalPath, int64_t fileLength)
{
    ScopedLock lock(_lock);
    assert(fileLength >= 0);
    _packageFileLengths[physicalPath] = fileLength;
}

void EntryTable::SetPackageFileLength(const std::string& physicalPath, int64_t fileLength)
{
    ScopedLock lock(_lock);
    _packageFileLengths[physicalPath] = fileLength;
}

void EntryTable::UpdatePackageFileLengthsCache(const EntryMeta& entryMeta)
{
    if (!entryMeta.IsInPackage()) {
        return;
    }
    const std::string& fullPhysicalPath = entryMeta.GetFullPhysicalPath();
    const std::string& rawFullPhysicalPath = entryMeta.GetRawFullPhysicalPath();
    assert(fullPhysicalPath != rawFullPhysicalPath);
    ScopedLock lock(_lock);
    if (auto it = _packageFileLengthsCache.find(fullPhysicalPath); it != _packageFileLengthsCache.end()) {
        // already in _packageFileLengthsCache, cached
        assert(it->second == _packageFileLengths.at(rawFullPhysicalPath));
        return;
    } else if (auto it = _packageFileLengths.find(rawFullPhysicalPath); it != _packageFileLengths.end()) {
        // cache
        _packageFileLengthsCache[fullPhysicalPath] = it->second;
    } else {
        AUTIL_LOG(ERROR, "Can not get package file length for EntryMeta[%s] ", entryMeta.DebugString().c_str());
    }
}

ErrorCode EntryTable::Freeze(const std::vector<std::string>& logicalFileNames)
{
    ScopedLock lock(_lock);
    ErrorCode ec = FSEC_OK;
    for (const std::string& logicalFileName : logicalFileNames) {
        auto tmpEc = Freeze(logicalFileName);
        if (unlikely(FSEC_OK != tmpEc)) {
            ec = tmpEc;
        }
    }
    return ec;
}

ErrorCode EntryTable::Freeze(const std::string& logicalFileName)
{
    ScopedLock lock(_lock);
    auto it = _entryMetaMap.find(logicalFileName);
    if (unlikely(it == _entryMetaMap.end())) {
        AUTIL_LOG(ERROR, "Entry meta [%s] not exists", logicalFileName.c_str());
        return FSEC_NOENT;
    }
    it->second.SetMutable(false);
    it->second.SetIsMemFile(false);
    return FSEC_OK;
}

FSResult<int64_t> EntryTable::GetPackageFileLength(const std::string& physicalPath)
{
    ScopedLock lock(_lock);
    if (auto it = _packageFileLengths.find(physicalPath); it != _packageFileLengths.end()) {
        return {FSEC_OK, it->second};
    }
    if (auto it = _packageFileLengthsCache.find(physicalPath); it != _packageFileLengthsCache.end()) {
        return {FSEC_OK, it->second};
    }
    AUTIL_LOG(INFO, "Can not find package file [%s]", physicalPath.c_str());
    int64_t fileLength = -1;
    auto ec = FslibWrapper::GetFileLength(physicalPath, fileLength).Code();
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "Get package file [%s] length failed, ec[%d]", physicalPath.c_str(), ec);
        return {ec, -1};
    }
    _packageFileLengthsCache[physicalPath] = fileLength;
    return {FSEC_OK, fileLength};
}

ErrorCode EntryTable::RenamePhysicalPath(const std::string& srcPath, const std::string& destPath, bool isFile,
                                         FenceContext* fenceContext)
{
    if (isFile) {
        if (_packageFileLengths.count(srcPath) == 1) {
            _packageFileLengths[destPath] = _packageFileLengths.at(srcPath);
            _packageFileLengths.erase(srcPath);
            return FslibWrapper::RenameWithFenceContext(srcPath, destPath, fenceContext).Code();
        }
    }
    // isDir
    std::map<std::string, std::string> renameMap;
    for (auto it = _packageFileLengths.lower_bound(srcPath + "/"); it != _packageFileLengths.end(); ++it) {
        std::string relativePath;
        if (!PathUtil::GetRelativePath(srcPath, it->first, relativePath)) {
            break;
        }
        renameMap[it->first] = PathUtil::JoinPath(destPath, relativePath);
    }
    for (const auto& it : renameMap) {
        assert(_packageFileLengths.count(it.first) == 1);
        _packageFileLengths[it.second] = _packageFileLengths.at(it.first);
        _packageFileLengths.erase(it.first);
    }
    return FslibWrapper::RenameWithFenceContext(srcPath, destPath, fenceContext).Code();
}

bool EntryTable::ValidatePackageFileLengths()
{
    ScopedLock lock(_lock);
    std::set<std::string> packageMetaFileGroups;
    std::set<std::string> packageDataFileGroups;
    for (const auto& it : _packageFileLengths) {
        std::string path = it.first;
        assert(it.second >= 0);
        std::string group;
        auto ec = PackageFileMeta::GetPackageFileGroup(path, &group);
        if (ec != FSEC_OK) {
            return false;
        }
        if (path.find(PACKAGE_FILE_DATA_SUFFIX) != string::npos) {
            packageDataFileGroups.insert(group);
        } else {
            assert(path.find(PACKAGE_FILE_META_SUFFIX) != string::npos);
            packageMetaFileGroups.insert(group);
        }
    }
    for (const std::string& packageDataFileGroup : packageDataFileGroups) {
        if (packageMetaFileGroups.count(packageDataFileGroup) == 0) {
            AUTIL_LOG(ERROR, "package data file group [%s] can not find package meta file",
                      packageDataFileGroup.c_str());
            return false;
        }
    }
    return true;
}

bool EntryTable::ExtractVersionId(const std::string& fileName, int& version)
{
    std::string prefix = std::string(ENTRY_TABLE_FILE_NAME_PREFIX) + ".";
    if (fileName.length() <= prefix.length()) {
        return false;
    }
    return autil::StringUtil::startsWith(fileName, prefix) &&
           autil::StringUtil::fromString<int>(fileName.substr(prefix.size(), fileName.size() - prefix.size()), version);
}
/*
  当前entryTable中的files block中可能存在package file相关的信息,这些是非预期的
  因此下面将这两类(innerFile+packageFile) 加入到filter中
  对于package_files中的信息, 会加上outputRoot, 因此需要substr
**/
size_t EntryTable::CalculateFileSize() const
{
    std::unordered_set<std::string> fileFilter;
    for (const auto& [path, entryMeta] : _entryMetaMap) {
        if (entryMeta.IsInPackage()) {
            fileFilter.insert(path);
        }
    }
    size_t totalFileSize = 0;
    for (const auto& [path, packageFileSize] : _packageFileLengths) {
        if (path.find(_outputRoot) != std::string::npos) {
            fileFilter.insert(path.substr(_outputRoot.size() + 1));
        } else {
            fileFilter.insert(path);
        }

        if (packageFileSize > 0) {
            totalFileSize += packageFileSize;
        }
    }

    for (const auto& [path, entryMeta] : _entryMetaMap) {
        if (entryMeta.IsInPackage()) {
            continue;
        }
        if (entryMeta.IsFile()) {
            auto it = fileFilter.find(path);
            if (it != fileFilter.end()) {
                continue;
            }
            totalFileSize += entryMeta.GetLength();
            fileFilter.insert(it, path);
        }
    }
    return totalFileSize;
}
}} // namespace indexlib::file_system
