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

#include <future>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/file_system/DiskStorage.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/Storage.h"
#include "indexlib/file_system/file/FileNodeCache.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/flush/DumpScheduler.h"
#include "indexlib/file_system/flush/FileFlushOperation.h"
#include "indexlib/file_system/flush/FlushOperationQueue.h"

namespace indexlib { namespace file_system {
struct FileSystemOptions;
struct WriterOption;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class MemStorage : public Storage
{
public:
    MemStorage(bool isReadonly, const util::BlockMemoryQuotaControllerPtr& memController,
               std::shared_ptr<EntryTable> entryTable) noexcept;
    ~MemStorage() noexcept;

public:
    ErrorCode Init(const std::shared_ptr<FileSystemOptions>& options) noexcept override;
    ErrorCode RecoverPackage(int32_t checkpoint, const std::string& logicalDir,
                             const std::vector<std::string>& physicalFileListHint) noexcept override;
    FSResult<std::shared_ptr<FileReader>> CreateFileReader(const std::string& logicalFilePath,
                                                           const std::string& physicalFilePath,
                                                           const ReaderOption& readerOption) noexcept override;
    FSResult<std::shared_ptr<FileWriter>> CreateFileWriter(const std::string& logicalFilePath,
                                                           const std::string& physicalFilePath,
                                                           const WriterOption& writerOption) noexcept override;
    ErrorCode MakeDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath, bool recursive,
                            bool packageHint) noexcept override;
    ErrorCode RemoveFile(const std::string& logicalFilePath, const std::string& physicalFilePath,
                         FenceContext* fenceContext) noexcept override;
    ErrorCode RemoveDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath,
                              FenceContext* fenceContext) noexcept override;
    ErrorCode StoreFile(const std::shared_ptr<FileNode>& fileNode,
                        const WriterOption& writerOption = WriterOption()) noexcept override;

    FSResult<size_t> EstimateFileLockMemoryUse(const std::string& logicalPath, const std::string& physicalPath,
                                               FSOpenType type, int64_t fileLength) noexcept override;
    FSResult<size_t> EstimateFileMemoryUseChange(const std::string& logicalPath, const std::string& physicalPath,
                                                 const std::string& oldTemperature, const std::string& newTemperature,
                                                 int64_t fileLength) noexcept override;

    FSResult<FSFileType> DeduceFileType(const std::string& logicalPath, const std::string& physicalPath,
                                        FSOpenType type) noexcept override;

    ErrorCode FlushPackage(const std::string& logicalDirPath) noexcept override;
    FSResult<std::future<bool>> Sync() noexcept override;
    ErrorCode WaitSyncFinish() noexcept override;
    FSResult<int32_t> CommitPackage() noexcept override;

    void CleanCache() noexcept override { _fileNodeCache->Clean(); }
    bool SetDirLifecycle(const std::string& logicalDirPath, const std::string& lifecycle) noexcept override
    {
        return true;
    }

    FSStorageType GetStorageType() const noexcept override { return FSST_MEM; }
    std::string DebugString() const noexcept override { return "not implemented"; }

    void TEST_EnableSupportMmap(bool isEnable) override
    { /*NOT_SUPPORTED */
        return;
    }

protected:
    FlushOperationQueuePtr CreateNewFlushOperationQueue() noexcept;
    bool NeedFlush() const noexcept { return _flushScheduler != NULL; }
    FSResult<std::shared_ptr<FileWriter>> CreateSwapMmapFileWriter(const std::string& logicalFilePath,
                                                                   const std::string& physicalFilePath,
                                                                   const WriterOption& writerOption) noexcept;
    ErrorCode AddFlushOperation(const FileFlushOperationPtr& fileFlushOperation) noexcept;

private:
    FlushOperationQueuePtr _operationQueue;
    DumpSchedulerPtr _flushScheduler;
    std::shared_ptr<FileNodeCreator> _memFileNodeCreator;
    std::shared_ptr<FileNodeCreator> _directoryFileNodeCreator;
    std::shared_ptr<FileNodeCache> _rootPathTable;
    autil::ThreadMutex _lock;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MemStorage> MemStoragePtr;
}} // namespace indexlib::file_system
