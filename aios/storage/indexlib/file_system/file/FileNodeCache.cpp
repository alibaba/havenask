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
#include "indexlib/file_system/file/FileNodeCache.h"

#include <cstddef>
#include <map>
#include <memory>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FileNodeCache);

FileNodeCache::FileNodeCache(StorageMetrics* metrics) noexcept : _metrics(metrics) {}

FileNodeCache::~FileNodeCache() noexcept {}

void FileNodeCache::Insert(const std::shared_ptr<FileNode>& fileNode) noexcept
{
    string normalPath = fileNode->GetLogicalPath();

    ScopedLock lock(_lock);

    FileNodeMap::const_iterator it = _fileNodeMap.find(normalPath);
    if (it != _fileNodeMap.end()) {
        // assert(it->second.use_count() == 1);
        // TODO(xiuchen) add toDelVec, DecreaseMetrics there for memory report called in cleanCache
        const std::shared_ptr<FileNode>& cacheFileNode = it->second;
        _toDelFileNodeVec.emplace_back(cacheFileNode);

        AUTIL_LOG(TRACE1, "replace in cache file [%s], type [%d] len [%lu]", cacheFileNode->DebugString().c_str(),
                  cacheFileNode->GetType(), cacheFileNode->GetLength());
    }

    // replace when exist
    _fileNodeMap[normalPath] = fileNode;
    FileNodeMap::iterator iter = _fileNodeMap.find(normalPath);
    std::shared_ptr<FileNode>& inMapNode = iter->second;
    _fastFileNodeMap[normalPath] = &inMapNode;
    IncreaseMetrics(fileNode->GetMetricGroup(), fileNode->GetType(), fileNode->GetLength());

    AUTIL_LOG(TRACE1, "Insert file [%s], type [%d] len [%lu]", fileNode->DebugString().c_str(), fileNode->GetType(),
              fileNode->GetLength());
}

ErrorCode FileNodeCache::RemoveFile(const string& filePath) noexcept
{
    assert(!filePath.empty());
    assert(*(filePath.rbegin()) != '/');
    assert(PathUtil::NormalizePath(filePath) == filePath);
    const string& normalPath = filePath;

    ScopedLock lock(_lock);

    FileNodeMap::const_iterator it = _fileNodeMap.find(normalPath);
    if (it != _fileNodeMap.end()) {
        const std::shared_ptr<FileNode>& fileNode = it->second;

        if (fileNode->GetType() == FSFT_DIRECTORY) {
            AUTIL_LOG(ERROR, "Path [%s] is a directory, remove failed", normalPath.c_str());
            return FSEC_ISDIR;
        }

        if (fileNode.use_count() > 1) {
            AUTIL_LOG(ERROR, "File [%s] remove fail, because the use count[%ld] >1", normalPath.c_str(),
                      fileNode.use_count());
            return FSEC_ERROR;
        }
        RemoveFileNode(fileNode);
        _fastFileNodeMap.erase(normalPath); // fastFileNodeMap should erase first
        _fileNodeMap.erase(normalPath);
        return FSEC_OK;
    }

    AUTIL_LOG(DEBUG, "File [%s] not in cache,", normalPath.c_str());
    return FSEC_NOENT;
}

ErrorCode FileNodeCache::RemoveDirectory(const std::string& dirPath) noexcept
{
    assert(PathUtil::NormalizePath(dirPath) == dirPath);
    const string& normalPath = dirPath;

    ScopedLock lock(_lock);
    ErrorCode ec = CheckRemoveDirectoryPath(normalPath);
    if (ec != FSEC_OK) {
        return ec;
    }

    DirectoryIterator iterator(_fileNodeMap, normalPath);
    // eg.: inmem, inmem/1, inmem/2, rm inmem
    // rm : inmem/1, inmem/2
    while (iterator.HasNext()) {
        Iterator curIt = iterator.Next();
        const std::shared_ptr<FileNode>& fileNode = curIt->second;
        RemoveFileNode(fileNode);
        _fastFileNodeMap.erase(fileNode->GetLogicalPath());
        iterator.Erase(curIt);
    }

    // rm : inmem
    FileNodeMap::iterator it = _fileNodeMap.find(normalPath);
    if (it != _fileNodeMap.end()) {
        RemoveFileNode(it->second);
        _fastFileNodeMap.erase(it->second->GetLogicalPath());
        _fileNodeMap.erase(it);
    }

    AUTIL_LOG(TRACE1, "Directory [%s] removed", dirPath.c_str());
    return FSEC_OK;
}

void FileNodeCache::ListDir(const string& dirPath, FileList& fileList, bool recursive, bool physical) const noexcept
{
    assert(PathUtil::NormalizePath(dirPath) == dirPath);
    const string& normalPath = dirPath;

    ScopedLock lock(_lock);
    DirectoryIterator iterator(_fileNodeMap, normalPath);
    while (iterator.HasNext()) {
        Iterator it = iterator.Next();
        const string& path = it->first;
        if (!recursive && path.find("/", normalPath.size() + 1) != string::npos) {
            continue;
        }
        if (physical && it->second->IsInPackage()) {
            continue;
        }
        string relativePath = path.substr(normalPath.size() + 1);
        if (recursive && it->second->GetType() == FSFT_DIRECTORY) {
            // for index deploy
            relativePath += "/";
        }
        fileList.push_back(relativePath);
    }
}

void FileNodeCache::Truncate(const string& filePath, size_t newLength) noexcept
{
    assert(PathUtil::NormalizePath(filePath) == filePath);
    const string& normalPath = filePath;

    ScopedLock lock(_lock);

    FileNodeMap::const_iterator it = _fileNodeMap.find(normalPath);
    if (it != _fileNodeMap.end()) {
        const std::shared_ptr<FileNode>& cacheFileNode = it->second;
        DecreaseMetrics(cacheFileNode->GetMetricGroup(), cacheFileNode->GetType(), cacheFileNode->GetLength());
        IncreaseMetrics(cacheFileNode->GetMetricGroup(), cacheFileNode->GetType(), newLength);
    }
}

void FileNodeCache::Clean() noexcept
{
    ScopedLock lock(_lock);

    for (FileNodeMap::iterator it = _fileNodeMap.begin(); it != _fileNodeMap.end();) {
        const std::shared_ptr<FileNode>& fileNode = it->second;
        fileNode->UpdateFileNodeUseCount(fileNode.use_count());
        // IsDirty for no sync
        if (fileNode->IsDirty() || fileNode.use_count() > 1) {
            ++it;
            continue;
        }
        RemoveFileNode(fileNode);
        _fastFileNodeMap.erase(fileNode->GetLogicalPath());
        _fileNodeMap.erase(it++);
    }

    for (auto it = _toDelFileNodeVec.begin(); it != _toDelFileNodeVec.end();) {
        (*it)->UpdateFileNodeUseCount(it->use_count());
        if (it->use_count() != 1) {
            ++it;
            continue;
        }
        DecreaseMetrics((*it)->GetMetricGroup(), (*it)->GetType(), (*it)->GetLength());
        it = _toDelFileNodeVec.erase(it);
    }
    AUTIL_LOG(TRACE1, "Cleaned");
}

void FileNodeCache::CleanFiles(const FileList& fileList) noexcept
{
    ScopedLock lock(_lock);

    for (size_t i = 0; i < fileList.size(); i++) {
        const FileNodeMap::iterator it = _fileNodeMap.find(fileList[i]);
        if (it != _fileNodeMap.end()) {
            const std::shared_ptr<FileNode>& fileNode = it->second;
            if (!fileNode->IsDirty() && fileNode.use_count() == 1) {
                RemoveFileNode(fileNode);
                _fastFileNodeMap.erase(fileNode->GetLogicalPath());
                _fileNodeMap.erase(it);
            }
        }
    }
    AUTIL_LOG(TRACE1, "Cleaned");
}

ErrorCode FileNodeCache::CheckRemoveDirectoryPath(const string& normalPath) const noexcept
{
    FileNodeMap::const_iterator it = _fileNodeMap.find(normalPath);
    if (it != _fileNodeMap.end()) {
        std::shared_ptr<FileNode> fileNode = it->second;
        if (fileNode->GetType() != FSFT_DIRECTORY) {
            AUTIL_LOG(ERROR, "Path [%s] is a file, remove failed", normalPath.c_str());
            return FSEC_NOTDIR;
        }
    } else {
        AUTIL_LOG(DEBUG, "Directory [%s] does not exist", normalPath.c_str());
    }
    DirectoryIterator iterator(_fileNodeMap, normalPath);
    while (iterator.HasNext()) {
        Iterator it = iterator.Next();
        const std::shared_ptr<FileNode>& fileNode = it->second;
        if (fileNode.use_count() > 1) {
            AUTIL_LOG(ERROR, "File [%s] in directory [%s] remove fail, because the use count [%ld]>1",
                      fileNode->DebugString().c_str(), normalPath.c_str(), fileNode.use_count());
            return FSEC_ERROR;
        }
    }

    return FSEC_OK;
}

void FileNodeCache::RemoveFileNode(const std::shared_ptr<FileNode>& fileNode) noexcept
{
    AUTIL_LOG(TRACE1, "Remove file [%s], type [%d] len [%lu]", fileNode->DebugString().c_str(), fileNode->GetType(),
              fileNode->GetLength());
    DecreaseMetrics(fileNode->GetMetricGroup(), fileNode->GetType(), fileNode->GetLength());
    IncreaseRemoveMetrics(fileNode->GetType());
}

uint64_t FileNodeCache::GetUseCount(const string& filePath) const noexcept
{
    autil::ScopedLock lock(_lock);
    FastFileNodeMap::const_iterator it = _fastFileNodeMap.find(filePath);
    if (it == _fastFileNodeMap.end()) {
        return 0;
    }
    return (it->second)->use_count();
}
}} // namespace indexlib::file_system
