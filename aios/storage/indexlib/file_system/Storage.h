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
#include <future>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileNodeCache.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/ResourceFile.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

namespace indexlib { namespace file_system {
struct FileSystemOptions;

class Storage
{
public:
    static std::shared_ptr<Storage>
    CreateInputStorage(const std::shared_ptr<FileSystemOptions>& options,
                       const std::shared_ptr<util::BlockMemoryQuotaController>& memController,
                       const std::shared_ptr<EntryTable>& entryTable,
                       autil::RecursiveThreadMutex* fileSystemLock) noexcept;
    static std::shared_ptr<Storage>
    CreateOutputStorage(const std::string& outputRoot, const std::shared_ptr<FileSystemOptions>& options,
                        const std::shared_ptr<util::BlockMemoryQuotaController>& memController,
                        const std::string& fsName, const std::shared_ptr<EntryTable>& entryTable,
                        autil::RecursiveThreadMutex* fileSystemLock) noexcept;

public:
    Storage(bool isReadonly, const std::shared_ptr<util::BlockMemoryQuotaController>& memController,
            std::shared_ptr<EntryTable> entryTable) noexcept;
    virtual ~Storage() noexcept;

public:
    virtual ErrorCode Init(const std::shared_ptr<FileSystemOptions>& options) noexcept = 0;
    virtual FSResult<std::shared_ptr<FileReader>> CreateFileReader(const std::string& logicalFilePath,
                                                                   const std::string& physicalFilePath,
                                                                   const ReaderOption& readerOption) noexcept = 0;
    // TODO: replace @param physicalFilePath with "" or "patch.[xx].__fs__"
    virtual FSResult<std::shared_ptr<FileWriter>> CreateFileWriter(const std::string& logicalFilePath,
                                                                   const std::string& physicalFilePath,
                                                                   const WriterOption& writerOption) noexcept = 0;

    virtual ErrorCode MakeDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath,
                                    bool recursive = true, bool packageHint = false) noexcept = 0;
    virtual ErrorCode RemoveFile(const std::string& logicalFilePath, const std::string& physicalFilePath,
                                 FenceContext* fenceContext) noexcept = 0;
    virtual ErrorCode RemoveDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath,
                                      FenceContext* fenceContext) noexcept = 0;
    virtual ErrorCode StoreFile(const std::shared_ptr<FileNode>& fileNode,
                                const WriterOption& writerOption = WriterOption()) noexcept = 0;

    virtual FSResult<size_t> EstimateFileLockMemoryUse(const std::string& logicalPath, const std::string& physicalPath,
                                                       FSOpenType type, int64_t fileLength) noexcept = 0;
    virtual FSResult<size_t> EstimateFileMemoryUseChange(const std::string& logicalPath,
                                                         const std::string& physicalPath,
                                                         const std::string& oldTemperature,
                                                         const std::string& newTemperature,
                                                         int64_t fileLength) noexcept = 0;

    virtual FSResult<FSFileType> DeduceFileType(const std::string& logicalPath, const std::string& physicalPath,
                                                FSOpenType type) noexcept = 0;

    virtual FSResult<std::future<bool>> Sync() noexcept = 0;
    virtual ErrorCode WaitSyncFinish() noexcept = 0;

    virtual void CleanCache() noexcept = 0;
    virtual bool SetDirLifecycle(const std::string& logicalDirPath, const std::string& lifecycle) noexcept = 0;
    virtual FSStorageType GetStorageType() const noexcept = 0;

public:
    // PackageDiskStorage only
    virtual FSResult<int32_t> CommitPackage() noexcept = 0;
    virtual ErrorCode RecoverPackage(int32_t checkpoint, const std::string& logicalPath,
                                     const std::vector<std::string>& physicalFileListHint) noexcept = 0;
    // PackageMemStorage only
    virtual ErrorCode FlushPackage(const std::string& logicalDirPath) noexcept = 0;

public:
    ErrorCode StoreWhenNonExist(const std::shared_ptr<FileNode>& fileNode) noexcept;
    void Truncate(const std::string& filePath, size_t newLength) noexcept;
    const StorageMetrics& GetMetrics() const noexcept { return _storageMetrics; }

    FSResult<std::shared_ptr<FileWriter>> CreateMemFileWriter(const std::string& logicalFilePath,
                                                              const std::string& physicalFilePath,
                                                              const WriterOption& writerOption) noexcept;
    FSResult<std::shared_ptr<FileReader>> CreateSliceFileReader(const std::string& logicalFilePath,
                                                                const std::string& physicalFilePath,
                                                                const ReaderOption& readerOption) noexcept;
    FSResult<std::shared_ptr<FileWriter>> CreateSliceFileWriter(const std::string& logicalFilePath,
                                                                const std::string& physicalFilePath,
                                                                const WriterOption& writerOption) noexcept;

    FSResult<ResourceFilePtr> CreateResourceFile(const std::string& logicalFilePath,
                                                 const std::string& physicalFilePath,
                                                 FSMetricGroup metricGroup) noexcept;
    FSResult<ResourceFilePtr> GetResourceFile(const std::string& logicalFilePath) const noexcept;

    ErrorCode RemoveFileFromCache(const std::string& logicalPath) noexcept
    {
        autil::ScopedLock scopedLock(*_fileSystemLock);
        return _fileNodeCache->RemoveFile(logicalPath);
    }
    ErrorCode RemoveDirectoryFromCache(const std::string& logicalPath) noexcept
    {
        autil::ScopedLock scopedLock(*_fileSystemLock);
        return _fileNodeCache->RemoveDirectory(logicalPath);
    }
    bool IsExistInCache(const std::string& path) const noexcept { return _fileNodeCache->IsExist(path); }
    std::shared_ptr<FileNode> GetFileNode(const std::string& path) const noexcept { return _fileNodeCache->Find(path); }

public:
    virtual std::string DebugString() const noexcept = 0;
    void TEST_GetFileStat(const std::string& logicalFilePath, FileStat& fileStat) const noexcept;
    virtual void TEST_EnableSupportMmap(bool isEnable) { assert(false); }

protected:
    FSResult<std::shared_ptr<FileReader>> CreateSwapMmapFileReader(const std::string& logicalFilePath,
                                                                   const std::string& physicalFilePath,
                                                                   const ReaderOption& readerOption) noexcept;

protected:
    autil::RecursiveThreadMutex* _fileSystemLock = nullptr;
    StorageMetrics _storageMetrics;
    std::shared_ptr<FileNodeCache> _fileNodeCache;
    std::shared_ptr<util::BlockMemoryQuotaController> _memController;
    std::shared_ptr<EntryTable> _entryTable;
    std::shared_ptr<FileSystemOptions> _options;
    bool _isReadOnly;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Storage> StoragePtr;
}} // namespace indexlib::file_system
