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
#include "indexlib/file_system/EntryTableJsonizable.h"

#include <assert.h>
#include <cstdint>
#include <sstream>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, EntryTableJsonizable);

EntryTableJsonizable::EntryTableJsonizable() {}
EntryTableJsonizable::~EntryTableJsonizable() {}

void EntryTableJsonizable::PackageMetaInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("length", length, length);
    if (json.GetMode() == FROM_JSON) {
        json.Jsonize("files", files, files);
    } else {
        if (!files.empty()) {
            json.Jsonize("files", files, files);
        }
    }
}

string EntryTableJsonizable::PackageMetaInfo::DebugString() const
{
    stringstream ss;
    ss << "length: " << length << endl;
    ss << "files: " << endl;
    for (const auto& pair : files) {
        ss << "path: " << pair.first << " meta: " << pair.second.DebugString() << endl;
    }
    return ss.str();
}

string SimplifyPhysicalRoot(const std::string& physicalRoot, const EntryTable* entryTable)
{
    return physicalRoot == entryTable->GetOutputRoot() ? "" : physicalRoot;
}

ErrorCode EntryTableJsonizable::CollectEntryMeta(const EntryTable* entryTable, const EntryMeta& entryMeta)
{
    if (entryMeta.IsMemFile()) {
        return FSEC_OK;
    }
    const string& logicalPath = entryMeta.GetLogicalPath();
    if (!entryMeta.IsInPackage()) {
        string simplifiedPhysicalRoot = SimplifyPhysicalRoot(entryMeta.GetRawPhysicalRoot(), entryTable);
        _files[simplifiedPhysicalRoot][logicalPath] = entryMeta;
    } else {
        const std::string& physicalRoot = entryMeta.GetRawPhysicalRoot();
        const std::string& relativePath = entryMeta.GetPhysicalPath();
        const std::string& fullPath = entryMeta.GetRawFullPhysicalPath();
        assert(PathUtil::JoinPath(physicalRoot, relativePath) == fullPath);

        std::string packageFileGroup;
        RETURN_IF_FS_ERROR(PackageFileMeta::GetPackageFileGroup(fullPath, &packageFileGroup), "");
        if (_packageFileRoots.count(packageFileGroup) == 0) {
            _packageFileRoots[packageFileGroup] = physicalRoot;
        } else if (unlikely(_packageFileRoots[packageFileGroup] != physicalRoot)) {
            AUTIL_LOG(ERROR, "package file group [%s] with [%s] can not match [%s] on entry [%s]",
                      packageFileGroup.c_str(), _packageFileRoots[packageFileGroup].c_str(), physicalRoot.c_str(),
                      entryMeta.DebugString().c_str());
            return FSEC_ERROR;
        }
        string simplifiedPhysicalRoot = SimplifyPhysicalRoot(physicalRoot, entryTable);
        _packageFiles[simplifiedPhysicalRoot][relativePath].files[logicalPath] = entryMeta;
        if (entryMeta.GetPhysicalPath() == relativePath) {
            _packageFiles[simplifiedPhysicalRoot][relativePath].files[logicalPath].SetPhysicalPath("");
        }
        if (entryMeta.IsFile() && entryTable->_packageFileLengths.count(fullPath) == 0) {
            AUTIL_LOG(ERROR, "file [%s] can not find package data file", entryMeta.DebugString().c_str());
            return FSEC_ERROR;
        }
    }
    return FSEC_OK;
}

ErrorCode EntryTableJsonizable::CompletePackageFileLength(const EntryTable* entryTable, bool collectAll)
{
    // complete _packageFiles file length
    for (const auto& pair : entryTable->_packageFileLengths) {
        std::string fullPath = pair.first;
        std::string group;
        RETURN_IF_FS_ERROR(PackageFileMeta::GetPackageFileGroup(fullPath, &group), "");
        if (_packageFileRoots.count(group) == 0) {
            assert(!collectAll);
            continue;
        }
        std::string root = _packageFileRoots.at(group);
        std::string relativePath;
        if (!PathUtil::GetRelativePath(root, fullPath, relativePath)) {
            assert(false);
            AUTIL_LOG(ERROR, "package file [%s] not in root [%s]", fullPath.c_str(), root.c_str());
            return FSEC_ERROR;
        }
        _packageFiles[SimplifyPhysicalRoot(root, entryTable)][relativePath].length = pair.second;
    }
    return FSEC_OK;
}

ErrorCode EntryTableJsonizable::CollectAll(const EntryTable* entryTable, const std::vector<std::string>& filterDirList)
{
    // To find the correct root for package meta file: packageFileGroup => root
    std::unordered_set<std::string> filteredLogicPaths;
    for (const auto& dir : filterDirList) {
        filteredLogicPaths.insert(dir);
        std::vector<EntryMeta> childEntryMetas = entryTable->ListDir(dir, /*recursive=*/true);
        for (auto& entryMeta : childEntryMetas) {
            filteredLogicPaths.insert(entryMeta.GetLogicalPath());
        }
    }
    _packageFileRoots.clear();
    // generate _files & _packageFiles
    for (const auto& pair : entryTable->_entryMetaMap) {
        const EntryMeta& entryMeta = pair.second;
        auto path = entryMeta.GetLogicalPath();
        if (filteredLogicPaths.find(path) != filteredLogicPaths.end()) {
            AUTIL_LOG(INFO, "filter path [%s]", path.c_str());
            continue;
        }
        assert(/*logicalPath=*/pair.first == entryMeta.GetLogicalPath());
        RETURN_IF_FS_ERROR(CollectEntryMeta(entryTable, entryMeta), "");
    }
    return CompletePackageFileLength(entryTable, /*collectAll=*/true);
}

ErrorCode EntryTableJsonizable::CollectParentEntryMetas(const EntryTable* entryTable, const EntryMeta& entryMeta)
{
    string parentDirPath = PathUtil::GetParentDirPath(entryMeta.GetLogicalPath());
    while (!parentDirPath.empty()) {
        FSResult<EntryMeta> parentEntryMetaRet = entryTable->GetEntryMeta(parentDirPath);
        const EntryMeta& parentEntryMeta = parentEntryMetaRet.result;
        if (unlikely(!parentEntryMetaRet.OK())) {
            AUTIL_LOG(ERROR, "parentEntryMeta [%s] not in entry table", parentDirPath.c_str());
            return FSEC_ERROR;
        }
        RETURN_IF_FS_ERROR(CollectEntryMeta(entryTable, parentEntryMeta), "");
        parentDirPath = PathUtil::GetParentDirPath(parentDirPath);
    }
    return FSEC_OK;
}

ErrorCode EntryTableJsonizable::CollectFile(const EntryTable* entryTable, const string& file)
{
    FSResult<EntryMeta> entryMetaRet = entryTable->GetEntryMeta(file);
    const EntryMeta& entryMeta = entryMetaRet.result;
    if (unlikely(entryMetaRet.ec != FSEC_OK)) {
        AUTIL_LOG(INFO, "file [%s] not in entry table", file.c_str());
        return FSEC_OK;
    }
    RETURN_IF_FS_ERROR(CollectParentEntryMetas(entryTable, entryMeta), "");
    RETURN_IF_FS_ERROR(CollectEntryMeta(entryTable, entryMeta), "");
    return FSEC_OK;
}

ErrorCode EntryTableJsonizable::CollectDir(const EntryTable* entryTable, const string& dir,
                                           std::unordered_set<std::string> filteredLogicPaths)
{
    if (filteredLogicPaths.find(dir) != filteredLogicPaths.end()) {
        AUTIL_LOG(INFO, "dir [%s] filtered", dir.c_str());
        return FSEC_OK;
    }
    FSResult<EntryMeta> entryMetaRet = entryTable->GetEntryMeta(dir);
    if (unlikely(entryMetaRet.ec != FSEC_OK)) {
        AUTIL_LOG(INFO, "dir [%s] not in entry table", dir.c_str());
        return FSEC_OK;
    }
    const EntryMeta& entryMeta = entryMetaRet.result;
    // Parent entry metas are necessary, otherwise e.g. only /root/a/b/c exists in EntryTable, but /root/a/b does not.
    RETURN_IF_FS_ERROR(CollectParentEntryMetas(entryTable, entryMeta), "");
    RETURN_IF_FS_ERROR(CollectEntryMeta(entryTable, entryMeta), "");
    std::vector<EntryMeta> childEntryMetas = entryTable->ListDir(dir, /*recursive=*/true);
    for (const EntryMeta& childEntryMeta : childEntryMetas) {
        auto path = childEntryMeta.GetLogicalPath();
        if (filteredLogicPaths.find(path) != filteredLogicPaths.end()) {
            AUTIL_LOG(INFO, "dir [%s] is filtered", path.c_str());
            continue;
        }
        RETURN_IF_FS_ERROR(CollectEntryMeta(entryTable, childEntryMeta), "");
    }
    return FSEC_OK;
}

// This step is used to collect all the entry-metas necessary from entryTable to make files in
// wishFileList and dirs in wishDirList work correctly.
ErrorCode EntryTableJsonizable::CollectFromEntryTable(const EntryTable* entryTable,
                                                      const std::vector<std::string>& wishFileList,
                                                      const std::vector<std::string>& wishDirList,
                                                      const std::vector<std::string>& filterDirList)
{
    if (wishFileList.empty() && wishDirList.empty()) {
        return CollectAll(entryTable, filterDirList);
    }
    std::unordered_set<std::string> filteredLogicPaths;
    for (const auto& dir : filterDirList) {
        filteredLogicPaths.insert(dir);
        std::vector<EntryMeta> childEntryMetas = entryTable->ListDir(dir, /*recursive=*/true);
        for (auto& entryMeta : childEntryMetas) {
            filteredLogicPaths.insert(entryMeta.GetLogicalPath());
        }
    }
    _packageFileRoots.clear();
    for (const auto& dir : wishDirList) {
        RETURN_IF_FS_ERROR(CollectDir(entryTable, dir, filteredLogicPaths), "");
    }

    for (const auto& file : wishFileList) {
        if (filteredLogicPaths.find(file) != filteredLogicPaths.end()) {
            AUTIL_LOG(INFO, "file [%s] is filtered by filter list", file.c_str());
            continue;
        }
        RETURN_IF_FS_ERROR(CollectFile(entryTable, file), "");
    }

    return CompletePackageFileLength(entryTable, /*collectAll=*/false);
}

void EntryTableJsonizable::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("files", _files);
    json.Jsonize("package_files", _packageFiles, _packageFiles);
}

std::map<std::string, std::map<std::string, EntryMeta>>& EntryTableJsonizable::GetFiles() { return _files; }

std::map<std::string, std::map<std::string, EntryTableJsonizable::PackageMetaInfo>>&
EntryTableJsonizable::GetPackageFiles()
{
    return _packageFiles;
}

}} // namespace indexlib::file_system
