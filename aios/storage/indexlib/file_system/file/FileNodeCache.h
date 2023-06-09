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
#pragma once

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/file/DirectoryMapIterator.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace file_system {

// ASSUME: directory(in mem only)  always is dirty
class FileNodeCache
{
public:
    typedef std::unordered_map<std::string, std::shared_ptr<FileNode>*> FastFileNodeMap;

    typedef DirectoryMapIterator<std::shared_ptr<FileNode>> DirectoryIterator;
    typedef DirectoryIterator::DirectoryMap FileNodeMap;
    typedef FileNodeMap::iterator Iterator;
    typedef FileNodeMap::const_iterator ConstIterator;

public:
    FileNodeCache(StorageMetrics* metrics) noexcept;
    ~FileNodeCache() noexcept;

public:
    void Insert(const std::shared_ptr<FileNode>& fileNode) noexcept;
    ErrorCode RemoveFile(const std::string& filePath) noexcept;
    ErrorCode RemoveDirectory(const std::string& dirPath) noexcept;
    bool IsExist(const std::string& path) const noexcept;
    // physical=true: list on disk files only
    void ListDir(const std::string& dirPath, fslib::FileList& fileList, bool recursive = false,
                 bool physical = false) const noexcept;
    std::shared_ptr<FileNode> Find(const std::string& path) const noexcept;
    void Truncate(const std::string& filePath, size_t newLength) noexcept;
    void Clean() noexcept;
    void CleanFiles(const fslib::FileList& fileList) noexcept;

    size_t GetFileCount() { return _fileNodeMap.size(); }
    uint64_t GetUseCount(const std::string& filePath) const noexcept;

public:
    autil::RecursiveThreadMutex* GetCacheLock() noexcept { return &_lock; }

    ConstIterator Begin() const noexcept { return _fileNodeMap.begin(); }
    ConstIterator End() const noexcept { return _fileNodeMap.end(); }

private:
    bool DoIsExist(const std::string& path) const noexcept;
    ErrorCode CheckRemoveDirectoryPath(const std::string& normalPath) const noexcept;
    void IncreaseMetrics(FSMetricGroup metricGroup, FSFileType type, size_t length) noexcept
    {
        if (_metrics) {
            _metrics->IncreaseFile(metricGroup, type, length);
        }
    }
    void DecreaseMetrics(FSMetricGroup metricGroup, FSFileType type, size_t length) noexcept
    {
        if (_metrics) {
            _metrics->DecreaseFile(metricGroup, type, length);
        }
    }
    void IncreaseRemoveMetrics(FSFileType type) noexcept;
    void RemoveFileNode(const std::shared_ptr<FileNode>& fileNode) noexcept;

private:
    mutable autil::RecursiveThreadMutex _lock;
    mutable FileNodeMap _fileNodeMap;
    mutable std::vector<std::shared_ptr<FileNode>> _toDelFileNodeVec;
    mutable FastFileNodeMap _fastFileNodeMap;
    StorageMetrics* _metrics;

private:
    AUTIL_LOG_DECLARE();
    friend class FileNodeCacheTest;
};

typedef std::shared_ptr<FileNodeCache> FileNodeCachePtr;

////////////////////////////////////////////////////////////
inline void FileNodeCache::IncreaseRemoveMetrics(FSFileType type) noexcept
{
    if (_metrics && type != FSFT_DIRECTORY) {
        _metrics->IncreaseFileCacheRemove();
    }
}

inline bool FileNodeCache::IsExist(const std::string& path) const noexcept
{
    assert(util::PathUtil::NormalizePath(path) == path);
    const std::string& normalPath = path;
    // const std::string& normalPath = path;
    autil::ScopedLock lock(_lock);
    return DoIsExist(normalPath);
}

inline bool FileNodeCache::DoIsExist(const std::string& path) const noexcept
{
    return _fastFileNodeMap.find(path) != _fastFileNodeMap.end();
}

inline std::shared_ptr<FileNode> FileNodeCache::Find(const std::string& path) const noexcept
{
    autil::ScopedLock lock(_lock);
    FastFileNodeMap::const_iterator it = _fastFileNodeMap.find(path);
    if (it == _fastFileNodeMap.end()) {
        return std::shared_ptr<FileNode>();
    }
    return *(it->second);
}
}} // namespace indexlib::file_system
