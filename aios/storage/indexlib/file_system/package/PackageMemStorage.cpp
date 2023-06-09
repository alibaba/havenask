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
#include "indexlib/file_system/package/PackageMemStorage.h"

#include <assert.h>
#include <ext/alloc_traits.h>
#include <functional>
#include <iterator>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <unistd.h>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/file/DirectoryFileNodeCreator.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/flush/FileFlushOperation.h"
#include "indexlib/file_system/package/InnerFileMeta.h"
#include "indexlib/file_system/package/PackageFileFlushOperation.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {

AUTIL_LOG_SETUP(indexlib.file_system, PackageMemStorage);

PackageMemStorage::PackageMemStorage(bool isReadonly, const util::BlockMemoryQuotaControllerPtr& memController,
                                     std::shared_ptr<EntryTable> entryTable) noexcept
    : MemStorage(isReadonly, memController, entryTable)
{
}

PackageMemStorage::~PackageMemStorage() noexcept {}

ErrorCode PackageMemStorage::MakeDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath,
                                           bool recursive, bool packageHint) noexcept
{
    assert(_options->needFlush);
    ScopedLock lock(_lock);
    auto it = _innerDirs.find(logicalDirPath);
    if (it != _innerDirs.end()) {
        if (recursive && physicalDirPath == it->second.first) {
            return FSEC_OK;
        }
        AUTIL_LOG(ERROR, "Make directory [%s]:[%s] [%s] failed, directory exists with physical path [%s]",
                  logicalDirPath.c_str(), physicalDirPath.c_str(), recursive ? "recursive" : "non-recursive",
                  it->second.first.c_str());
        return FSEC_EXIST;
    }
    _innerDirs.insert({logicalDirPath, {physicalDirPath, packageHint}});
    _storageMetrics.IncreaseFile(FSMG_LOCAL, FSFT_DIRECTORY, 0);
    if (packageHint) {
        _innerInMemFileMap.insert({logicalDirPath, {}});
    }

    if (recursive) {
        auto logicalParent = PathUtil::GetParentDirPath(logicalDirPath);
        auto physicalParent = PathUtil::GetParentDirPath(physicalDirPath);
        while (!logicalParent.empty() && _innerDirs.find(logicalParent) == _innerDirs.end()) {
            _innerDirs.insert({logicalParent, {physicalParent, false}});
            _storageMetrics.IncreaseFile(FSMG_LOCAL, FSFT_DIRECTORY, 0);
            logicalParent = PathUtil::GetParentDirPath(logicalParent);
            physicalParent = PathUtil::GetParentDirPath(physicalParent);
        }
    }

    return FSEC_OK;
}

ErrorCode PackageMemStorage::RemoveDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath,
                                             FenceContext* fenceContext) noexcept
{
    ScopedLock lock(_lock);
    auto prefix = logicalDirPath + "/";
    auto it = _innerDirs.find(logicalDirPath);

    if (it == _innerDirs.end()) {
        return MemStorage::RemoveDirectory(logicalDirPath, physicalDirPath, fenceContext);
    }

    if (unlikely(!it->second.second)) {
        AUTIL_LOG(ERROR, "package mem storage not support remove un-package directory [%s]", logicalDirPath.c_str());
        return FSEC_NOTSUP;
    }
    if (unlikely(physicalDirPath != it->second.first)) {
        AUTIL_LOG(ERROR, "Inconsident state, different physical path [%s] : [%s] for dir [%s]", physicalDirPath.c_str(),
                  it->second.first.c_str(), logicalDirPath.c_str());
        return FSEC_ERROR;
    }
    it = _innerDirs.erase(it);
    _innerInMemFileMap.erase(logicalDirPath);
    while (it != _innerDirs.end() && it->first.find(prefix) == 0) {
        if (it->second.second) {
            _innerInMemFileMap.erase(it->first);
        }
        it = _innerDirs.erase(it);
    }
    return MemStorage::RemoveDirectory(logicalDirPath, physicalDirPath, fenceContext);
}

ErrorCode PackageMemStorage::StoreFile(const std::shared_ptr<FileNode>& fileNode,
                                       const WriterOption& writerOption) noexcept
{
    ScopedLock lock(_lock);
    if (writerOption.notInPackage) {
        return MemStorage::StoreFile(fileNode, writerOption);
    }

    auto parentPath = PathUtil::GetParentDirPath(fileNode->GetLogicalPath());
    std::list<InnerInMemoryFile>* listNodes = nullptr;
    while (!parentPath.empty() && parentPath != "/") {
        auto it = _innerInMemFileMap.find(parentPath);
        if (it != _innerInMemFileMap.end()) {
            listNodes = &it->second;
            break;
        }

        parentPath = PathUtil::GetParentDirPath(parentPath);
    }

    if (listNodes == nullptr) {
        // Not in Package, direct dump
        return MemStorage::StoreFile(fileNode, writerOption);
    }

    if (_entryTable) {
        [[maybe_unused]] bool ret = _entryTable->SetEntryMetaIsMemFile(fileNode->GetLogicalPath(), false);
    }
    fileNode->SetInPackage(true);

    auto flushFileNode = fileNode;
    if (writerOption.copyOnDump) {
        AUTIL_LOG(INFO, "CopyFileNode for flush %s", fileNode->DebugString().c_str());
        flushFileNode.reset(fileNode->Clone());
        // TODO: need accumulate memory use of cloned file node, see also PackageFileFlushOperation::Init
        fileNode->SetDirty(false);
    }
    auto ec = StoreWhenNonExist(fileNode);
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "store filenode [%s] failed, ec [%d]", fileNode->GetLogicalPath().c_str(), ec);
        return ec;
    }
    listNodes->emplace_back(flushFileNode, writerOption);
    return FSEC_OK;
}

ErrorCode PackageMemStorage::FlushPackage(const std::string& logicalDirPath) noexcept
{
    ScopedLock lock(_lock);
    AUTIL_LOG(DEBUG, "start flush [%s]", logicalDirPath.c_str());
    std::vector<std::string> subLogicalDirPaths;
    for (auto it = _innerDirs.rbegin(); it != _innerDirs.rend(); ++it) {
        const std::string& tmpLogicalDirPath = it->first;
        bool isPackage = it->second.second;
        // only package directory in logicalDirPath will be flushed
        if (isPackage && PathUtil::IsInRootPath(tmpLogicalDirPath, logicalDirPath)) {
            subLogicalDirPaths.push_back(tmpLogicalDirPath);
        }
    }
    for (const std::string& path : subLogicalDirPaths) {
        autil::ScopedTime2 timer;
        RETURN_IF_FS_ERROR(DoFlushPackage(path), "DoFlushPackage [%s] failed for [%s]", path.c_str(),
                           logicalDirPath.c_str());
        AUTIL_LOG(INFO, "flush path [%s] in [%s], used[%.3f]", path.c_str(), logicalDirPath.c_str(), timer.done_sec());
    }
    AUTIL_LOG(DEBUG, "finish flush [%s]", logicalDirPath.c_str());
    return FSEC_OK;
}

ErrorCode PackageMemStorage::DoFlushPackage(const std::string& logicalDirPath) noexcept
{
    std::string packageDirPath;
    if (!GetPackageDirPath(logicalDirPath, packageDirPath)) {
        AUTIL_LOG(ERROR, "Directory [%s] is not in package", logicalDirPath.c_str());
        return FSEC_OK;
    }
    std::vector<std::string> logicalDirs;
    std::vector<std::string> physicalDirs;
    std::vector<std::shared_ptr<FileNode>> innerFiles;
    std::vector<WriterOption> writerOptionVec;
    RETURN_IF_FS_ERROR(
        CollectInnerFileAndDumpParam(logicalDirPath, logicalDirs, physicalDirs, innerFiles, writerOptionVec),
        "CollectInnerFileAndDumpParam for [%s] failed", logicalDirPath.c_str());

    auto [ec, packFileMeta] = GeneratePackageFileMeta(logicalDirPath, packageDirPath, physicalDirs, innerFiles);
    RETURN_IF_FS_ERROR(ec, "GeneratePackageFileMeta failed");
    std::string packageMetaPath =
        PathUtil::JoinPath(packageDirPath, std::string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_META_SUFFIX);
    std::string packageDataPath =
        PathUtil::JoinPath(packageDirPath, PackageFileMeta::GetPackageFileDataPath(PACKAGE_FILE_PREFIX, "", 0));
    if (_entryTable) {
        for (InnerFileMeta::Iterator iter = packFileMeta->Begin(); iter != packFileMeta->End(); ++iter) {
            _entryTable->UpdatePackageDataFile(PathUtil::JoinPath(logicalDirPath, iter->GetFilePath()),
                                               iter->IsDir() ? packageMetaPath : packageDataPath, iter->GetOffset());
        }
        for (size_t i = 0; i < packFileMeta->GetPhysicalFileNames().size(); ++i) {
            _entryTable->UpdatePackageMetaFile(PathUtil::JoinPath(packageDirPath, packFileMeta->GetPhysicalFileName(i)),
                                               packFileMeta->GetPhysicalFileLength(i));
        }
    }
    size_t packageMetaStrLength = 0;
    RETURN_IF_FS_ERROR(packFileMeta->GetMetaStrLength(&packageMetaStrLength),
                       "Get package meta file [%s] length failed", packageMetaPath.c_str());
    if (_entryTable) {
        _entryTable->UpdatePackageMetaFile(packageMetaPath, packageMetaStrLength);
    }

    FileFlushOperationPtr flushOperation(new PackageFileFlushOperation(_options->flushRetryStrategy, packageDirPath,
                                                                       packFileMeta, innerFiles, writerOptionVec));
    RETURN_IF_FS_ERROR(AddFlushOperation(flushOperation), "AddFlushOperation for [%s] failed", logicalDirPath.c_str());
    return FSEC_OK;
}

ErrorCode PackageMemStorage::CollectInnerFileAndDumpParam(const std::string& logicalDirPath,
                                                          std::vector<std::string>& logicalDirs,
                                                          std::vector<std::string>& innerDirs,
                                                          std::vector<std::shared_ptr<FileNode>>& fileNodeVec,
                                                          std::vector<WriterOption>& writerOptions) noexcept
{
    fileNodeVec.clear();
    writerOptions.clear();

    fileNodeVec.reserve(_innerDirs.size() + _innerInMemFileMap.size());
    writerOptions.reserve(_innerDirs.size() + _innerInMemFileMap.size());

    auto it = _innerDirs.find(logicalDirPath);
    assert(it != _innerDirs.end());
    if (it == _innerDirs.end() || !it->second.second) {
        AUTIL_LOG(ERROR, "Directory [%s] is not in package", logicalDirPath.c_str());
        return FSEC_ERROR;
    }

    it = _innerDirs.erase(it); // Remove self

    // Fill dirs
    auto prefix = logicalDirPath + "/";
    while (it != _innerDirs.end() && (it->first.find(prefix) == 0 || it->first == logicalDirPath)) {
        if (!it->second.second) {
            logicalDirs.emplace_back(it->first);
            innerDirs.emplace_back(it->second.first);
            it = _innerDirs.erase(it);
        } else {
            ++it;
        }
    }

    auto iter = _innerInMemFileMap.find(logicalDirPath);
    assert(iter != _innerInMemFileMap.end());
    if (iter == _innerInMemFileMap.end()) {
        AUTIL_LOG(ERROR, "Directory [%s] is not in package", logicalDirPath.c_str());
        return FSEC_ERROR;
    }

    auto& listNodes = iter->second;
    for (auto& listNode : listNodes) {
        if (listNode.first->GetType() == FSFT_RESOURCE) {
            continue;
        }
        fileNodeVec.emplace_back(listNode.first);
        writerOptions.emplace_back(listNode.second);
    }
    _innerInMemFileMap.erase(iter);
    return FSEC_OK;
}

// Return physical package dir path
// e.g. /physical_dir/
bool PackageMemStorage::GetPackageDirPath(const std::string& logicalDirPath, std::string& packageDirPath) noexcept
{
    auto it = _innerDirs.find(logicalDirPath);
    if (it == _innerDirs.end() || !it->second.second) {
        return false;
    }

    packageDirPath = it->second.first;
    return true;
}

FSResult<PackageFileMetaPtr>
PackageMemStorage::GeneratePackageFileMeta(const std::string& logicalDirPath, const string& physicalDirPath,
                                           const std::vector<string>& innerDirs,
                                           const vector<std::shared_ptr<FileNode>>& innerFiles) noexcept
{
    vector<std::shared_ptr<FileNode>> fileNodes;
    fileNodes.reserve(innerDirs.size() + innerFiles.size());

    DirectoryFileNodeCreator dirNodeCreator;

    // add inner dir file nodes
    auto iter = innerDirs.begin();
    for (; iter != innerDirs.end(); iter++) {
        const string& absDirPath = *iter;
        fileNodes.push_back(dirNodeCreator.CreateFileNode(FSOT_UNKNOWN, true, ""));
        RETURN2_IF_FS_ERROR(fileNodes.back()->Open(absDirPath, absDirPath, FSOT_UNKNOWN, 0), PackageFileMetaPtr(),
                            "open file node [%s] failed", absDirPath.c_str());
    }

    // add inner file nodes
    fileNodes.insert(fileNodes.end(), innerFiles.begin(), innerFiles.end());

    PackageFileMetaPtr fileMeta(new PackageFileMeta);
    size_t pageSize = getpagesize();
    ErrorCode ec = fileMeta->InitFromFileNode(logicalDirPath, PathUtil::JoinPath(physicalDirPath, PACKAGE_FILE_PREFIX),
                                              fileNodes, pageSize);
    RETURN2_IF_FS_ERROR(ec, PackageFileMetaPtr(), "Load package file meta [%s] [%s] failed", logicalDirPath.c_str(),
                        physicalDirPath.c_str());
    return {FSEC_OK, fileMeta};
}

string PackageMemStorage::DebugString() const noexcept
{
    ScopedLock lock(_lock);
    stringstream ss;
    for (const auto& pair : _innerInMemFileMap) {
        ss << "logical path: " << pair.first << endl;
        for (const auto& f : pair.second) {
            ss << "file logical path: " << f.first->GetLogicalPath()
               << " physical path: " << f.first->GetPhysicalPath();
        }
    }
    return ss.str();
}
}} // namespace indexlib::file_system
