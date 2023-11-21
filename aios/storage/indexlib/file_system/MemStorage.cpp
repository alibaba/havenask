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
#include "indexlib/file_system/MemStorage.h"

#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/DirectoryFileNodeCreator.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileNodeCreator.h"
#include "indexlib/file_system/file/FileWriterImpl.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/file_system/file/MemFileWriter.h"
#include "indexlib/file_system/file/NormalFileReader.h"
#include "indexlib/file_system/file/SwapMmapFileNode.h"
#include "indexlib/file_system/file/SwapMmapFileWriter.h"
#include "indexlib/file_system/flush/AsyncDumpScheduler.h"
#include "indexlib/file_system/flush/MkdirFlushOperation.h"
#include "indexlib/file_system/flush/SimpleDumpScheduler.h"
#include "indexlib/file_system/flush/SingleFileFlushOperation.h"
#include "indexlib/file_system/fslib/DeleteOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, MemStorage);

MemStorage::MemStorage(bool isReadonly, const util::BlockMemoryQuotaControllerPtr& memController,
                       std::shared_ptr<EntryTable> entryTable) noexcept
    : Storage(isReadonly, memController, entryTable)
{
}

MemStorage::~MemStorage() noexcept {}

ErrorCode MemStorage::Init(const std::shared_ptr<FileSystemOptions>& options) noexcept
{
    _options = options;
    if (_options->needFlush) {
        if (_options->enableAsyncFlush) {
            _flushScheduler.reset(new AsyncDumpScheduler());
        } else {
            _flushScheduler.reset(new SimpleDumpScheduler());
        }
        if (!_flushScheduler->Init()) {
            AUTIL_LOG(ERROR, "init flush scheduler failed");
            return FSEC_UNKNOWN;
        }
    }

    _memFileNodeCreator.reset(new MemFileNodeCreator());
    _memFileNodeCreator->Init(LoadConfig(), _memController);

    _directoryFileNodeCreator.reset(new DirectoryFileNodeCreator());
    [[maybe_unused]] bool ret = _directoryFileNodeCreator->Init(LoadConfig(), _memController);
    assert(ret);
    _operationQueue = CreateNewFlushOperationQueue();
    return FSEC_OK;
}

FSResult<std::shared_ptr<FileWriter>> MemStorage::CreateFileWriter(const string& logicalFilePath,
                                                                   const string& physicalFilePath,
                                                                   const WriterOption& writerOption) noexcept
{
    AUTIL_LOG(DEBUG, "Create file writer on path [%s] => [%s], [%ld]", logicalFilePath.c_str(),
              physicalFilePath.c_str(), writerOption.fileLength);

    if (unlikely(_isReadOnly)) {
        assert(false);
        AUTIL_LOG(ERROR, "create file writer on readonly storage, path [%s] => [%s]", logicalFilePath.c_str(),
                  physicalFilePath.c_str());
        return {FSEC_ERROR, std::shared_ptr<FileWriter>()};
    }

    auto updateFileSizeFunc = [this, logicalFilePath](int64_t size) {
        _entryTable && _entryTable->UpdateFileSize(logicalFilePath, size);
    };
    if (writerOption.isSwapMmapFile) {
        return CreateSwapMmapFileWriter(logicalFilePath, physicalFilePath, writerOption);
    }

    assert(writerOption.openType == FSOT_UNKNOWN || writerOption.openType == FSOT_BUFFERED);

    std::shared_ptr<FileWriter> fileWriter;
    // TODO: how about _options->needFlush = false
    if (writerOption.openType == FSOT_BUFFERED) {
        fileWriter.reset(new BufferedFileWriter(writerOption, std::move(updateFileSizeFunc)));
        if (!_entryTable->SetEntryMetaMutable(logicalFilePath, false)) {
            AUTIL_LOG(ERROR, "Failed to create file writer on path[%s] => [%s]: Check entry table correctness.",
                      logicalFilePath.c_str(), physicalFilePath.c_str());
            return {FSEC_ERROR, std::shared_ptr<FileWriter>()};
        }
    } else {
        fileWriter.reset(new MemFileWriter(_memController, this, writerOption, std::move(updateFileSizeFunc)));
    }
    // delay check exist to store & flush phase
    auto ec = fileWriter->Open(logicalFilePath, physicalFilePath);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "FileWriter Open [%s] ==> [%s] failed",
                        logicalFilePath.c_str(), physicalFilePath.c_str());
    return {FSEC_OK, fileWriter};
}

FSResult<std::shared_ptr<FileReader>> MemStorage::CreateFileReader(const string& logicalFilePath,
                                                                   const string& physicalFilePath,
                                                                   const ReaderOption& readerOption) noexcept
{
    if (readerOption.isSwapMmapFile) {
        return CreateSwapMmapFileReader(logicalFilePath, physicalFilePath, readerOption);
    }

    auto openType = readerOption.openType;
    // old fs rewrite this by MemDirectory. eg: switch readerOption.openType
    //     case MMAP: ut SubDocPatchIteratorTest.TestSimpleProcess
    openType = FSOT_MEM;

    std::shared_ptr<FileNode> fileNode = _fileNodeCache->Find(logicalFilePath);
    if (!fileNode) {
        // now MemStorage can only be output storage, so it must be in fileNodeCache
        // this branch means: thread 1 create a file but not yet closed, thread 2 try read this file
        // see ut: OnlinePartitionTest.TestReclaimRtIndexNotEffectReader
        AUTIL_LOG(INFO, "File [%s] => [%s], not exist, open type[%d]", logicalFilePath.c_str(),
                  physicalFilePath.c_str(), openType);
        return {FSEC_NOENT, nullptr};
    } else {
        if (fileNode->GetOpenType() != openType) {
            AUTIL_LOG(ERROR, "File [%s] => [%s] open type[%d] not match with cache type[%d]", logicalFilePath.c_str(),
                      physicalFilePath.c_str(), openType, fileNode->GetOpenType());
            return {FSEC_ERROR, nullptr};
        }
        AUTIL_LOG(DEBUG, "File [%s] => [%s] cache hit, Open type [%d]", logicalFilePath.c_str(),
                  physicalFilePath.c_str(), openType);
        _storageMetrics.IncreaseFileCacheHit();
    }

    return {FSEC_OK, std::shared_ptr<FileReader>(new NormalFileReader(fileNode))};
}

ErrorCode MemStorage::MakeDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath,
                                    bool recursive, bool packageHint) noexcept
{
    AUTIL_LOG(DEBUG, "Make dir[%s], recursive[%d]", physicalDirPath.c_str(), recursive);
    assert(_directoryFileNodeCreator);

    std::shared_ptr<FileNode> fileNode = _fileNodeCache->Find(logicalDirPath);
    if (!fileNode) {
        std::shared_ptr<FileNode> dirNode = _directoryFileNodeCreator->CreateFileNode(FSOT_UNKNOWN, true, "");
        RETURN_IF_FS_ERROR(dirNode->Open(logicalDirPath, physicalDirPath, FSOT_UNKNOWN, -1), "");
        dirNode->SetDirty(true);
        dirNode->SetInPackage(false);
        _fileNodeCache->Insert(dirNode);
        if (NeedFlush()) {
            MkdirFlushOperationPtr flushOperatiron(new MkdirFlushOperation(_options->flushRetryStrategy, dirNode));
            assert(_operationQueue);
            ScopedLock lock(_lock);
            _operationQueue->PushBack(flushOperatiron);
        }
    }
    return FSEC_OK;
}

ErrorCode MemStorage::RemoveFile(const std::string& logicalFilePath, const std::string& physicalFilePath,
                                 FenceContext* fenceContext) noexcept
{
    ErrorCode ec1 = RemoveFileFromCache(logicalFilePath);
    ErrorCode ec2 = FslibWrapper::DeleteFile(physicalFilePath, DeleteOption::Fence(fenceContext, false)).Code();
    if (ec1 == FSEC_OK) {
        if (likely(ec2 == FSEC_OK || ec2 == FSEC_NOENT)) {
            return FSEC_OK;
        }
        return ec2;
    } else if (ec1 == FSEC_NOENT) {
        if (likely(ec2 == FSEC_OK)) {
            return FSEC_OK;
        } else if (ec2 == FSEC_NOENT) {
            return FSEC_NOENT;
        }
        return ec2;
    }
    return ec1;
}

ErrorCode MemStorage::RemoveDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath,
                                      FenceContext* fenceContext) noexcept
{
    ErrorCode ec1 = RemoveDirectoryFromCache(logicalDirPath);
    ErrorCode ec2 = FslibWrapper::DeleteDir(physicalDirPath, DeleteOption::Fence(fenceContext, true)).Code();
    if (ec1 == FSEC_OK) {
        if (likely(ec2 == FSEC_OK || ec2 == FSEC_NOENT)) {
            return FSEC_OK;
        }
        return ec2;
    } else if (ec1 == FSEC_NOENT) {
        if (likely(ec2 == FSEC_OK)) {
            return FSEC_OK;
        } else if (ec2 == FSEC_NOENT) {
            return FSEC_NOENT;
        }
        return ec2;
    }
    return ec1;
}

FSResult<std::future<bool>> MemStorage::Sync() noexcept
{
    auto flushPromise = unique_ptr<promise<bool>>(new promise<bool>());
    auto flushFuture = flushPromise->get_future();
    if (!NeedFlush()) {
        flushPromise->set_value(false);
        AUTIL_LOG(DEBUG, "Sync no need!");
    } else {
        assert(_operationQueue);
        int64_t startTime = autil::TimeUtility::currentTime();

        // TODO: (xingwo): CHECK THIS!!!
        auto ec = _flushScheduler->WaitDumpQueueEmpty();
        RETURN2_IF_FS_ERROR(ec, std::move(flushFuture), "WaitDumpQueueEmpty failed");
        if (_operationQueue->Size() == 0) {
            flushPromise->set_value(true);
        } else {
            _operationQueue->SetPromise(std::move(flushPromise));
            {
                ScopedLock lock(_lock);
                RETURN2_IF_FS_ERROR(_flushScheduler->Push(_operationQueue), std::move(flushFuture), "Push failed");
                _operationQueue = CreateNewFlushOperationQueue();
            }
        }
        AUTIL_LOG(DEBUG, "Sync finish, used [%ld] us", autil::TimeUtility::currentTime() - startTime);
    }
    return {FSEC_OK, std::move(flushFuture)};
}

ErrorCode MemStorage::WaitSyncFinish() noexcept
{
    if (_flushScheduler) {
        return _flushScheduler->WaitDumpQueueEmpty();
    }
    return FSEC_OK;
}

FSResult<int32_t> MemStorage::CommitPackage() noexcept
{
    assert(false);
    return {FSEC_NOTSUP, -1};
}

ErrorCode MemStorage::RecoverPackage(int32_t checkpoint, const std::string& logicalDir,
                                     const std::vector<std::string>& physicalFileListHint) noexcept
{
    assert(false);
    return FSEC_NOTSUP;
}

FlushOperationQueuePtr MemStorage::CreateNewFlushOperationQueue() noexcept
{
    if (!NeedFlush()) {
        return FlushOperationQueuePtr();
    }

    FlushOperationQueuePtr flushOperationQueue(new FlushOperationQueue(&_storageMetrics));
    if (_fileSystemLock) {
        flushOperationQueue->SetEntryMetaFreezeCallback(
            [this](std::shared_ptr<FileFlushOperation>&& fileFlushOperation) {
                std::vector<std::string> logicalFileNames;
                fileFlushOperation->GetFileNodePaths(logicalFileNames);
                ScopedLock lock(*_fileSystemLock);
                if (_entryTable) {
                    ErrorCode ec = _entryTable->Freeze(logicalFileNames);
                    if (ec != FSEC_OK) {
                        AUTIL_LOG(DEBUG, "Freeze [%lu] failed, ec[%d]", logicalFileNames.size(), ec);
                    }
                }
                fileFlushOperation.reset();
                if (_fileNodeCache) {
                    _fileNodeCache->CleanFiles(logicalFileNames);
                }
            });
    } else {
        // for test
        flushOperationQueue->SetEntryMetaFreezeCallback(
            [fileNodeCache = _fileNodeCache](std::shared_ptr<FileFlushOperation>&& fileFlushOperation) {
                if (fileNodeCache) {
                    std::vector<std::string> logicalFileNames;
                    fileFlushOperation->GetFileNodePaths(logicalFileNames);
                    fileFlushOperation.reset();
                    fileNodeCache->CleanFiles(logicalFileNames);
                }
            });
    }
    return flushOperationQueue;
}

FSResult<std::shared_ptr<FileWriter>> MemStorage::CreateSwapMmapFileWriter(const string& logicalFilePath,
                                                                           const string& physicalFilePath,
                                                                           const WriterOption& writerOption) noexcept
{
    AUTIL_LOG(DEBUG, "Create swap mmap file [%s] => [%s], fileSize[%lu]", logicalFilePath.c_str(),
              physicalFilePath.c_str(), writerOption.fileLength);

    if (IsExistInCache(logicalFilePath)) {
        // only in cache file (no one use it, remove will success)
        auto ec = RemoveFile(logicalFilePath, physicalFilePath, FenceContext::NoFence());
        AUTIL_LOG(INFO, "File [%s] already exist, ec [%d], remove it before createSwapMmapFileWriter!",
                  logicalFilePath.c_str(), ec);
    }

    std::shared_ptr<SwapMmapFileNode> swapMmapFileNode(new SwapMmapFileNode(writerOption.fileLength, _memController));
    RETURN2_IF_FS_ERROR(swapMmapFileNode->OpenForWrite(logicalFilePath, physicalFilePath, FSOT_MMAP),
                        std::shared_ptr<FileWriter>(), "open swap mmap file node failed, path [%s => %s]",
                        logicalFilePath.c_str(), physicalFilePath.c_str());
    assert(swapMmapFileNode->GetType() == FSFT_MMAP || swapMmapFileNode->GetType() == FSFT_MMAP_LOCK);

    // swap mmap file will be clean by CleanCache
    swapMmapFileNode->SetDirty(false);
    swapMmapFileNode->SetMetricGroup(writerOption.metricGroup);
    auto ec = StoreFile(swapMmapFileNode);
    if (ec != FSEC_OK) {
        return {ec, SwapMmapFileWriterPtr()};
    }
    auto updateFileSizeFunc = [this, logicalFilePath](int64_t size) {
        _entryTable && _entryTable->UpdateFileSize(logicalFilePath, size);
    };
    return {FSEC_OK,
            SwapMmapFileWriterPtr(new SwapMmapFileWriter(swapMmapFileNode, this, std::move(updateFileSizeFunc)))};
}

FSResult<FSFileType> MemStorage::DeduceFileType(const std::string& logicalPath, const std::string& physicalPath,
                                                FSOpenType openType) noexcept
{
    switch (openType) {
    case FSOT_RESOURCE:
        return {FSEC_OK, FSFT_RESOURCE};
    case FSOT_SLICE:
        return {FSEC_OK, FSFT_SLICE};
    default:
        return {FSEC_OK, FSFT_MEM};
    }
    assert(false);
    return {FSEC_UNKNOWN, FSFT_UNKNOWN};
}

FSResult<size_t> MemStorage::EstimateFileLockMemoryUse(const std::string& logicalPath, const std::string& physicalPath,
                                                       FSOpenType openType, int64_t fileLength) noexcept
{
    if (openType == FSOT_RESOURCE || openType == FSOT_SLICE) {
        return {FSEC_OK, fileLength >= 0 ? (size_t)fileLength : 0};
    }
    return {FSEC_OK, _memFileNodeCreator->EstimateFileLockMemoryUse(fileLength)};
}

FSResult<size_t> MemStorage::EstimateFileMemoryUseChange(const std::string& logicalPath,
                                                         const std::string& physicalPath,
                                                         const std::string& oldTemperature,
                                                         const std::string& newTemperature, int64_t fileLength) noexcept
{
    assert(false);
    AUTIL_LOG(ERROR, "MemStorage not support EstimateFileMemoryUseChange, [%s => %s]", logicalPath.c_str(),
              physicalPath.c_str());
    return {FSEC_NOTSUP, 0};
}

ErrorCode MemStorage::StoreFile(const std::shared_ptr<FileNode>& fileNode, const WriterOption& writerOption) noexcept
{
    assert(fileNode);

    RETURN_IF_FS_ERROR(StoreWhenNonExist(fileNode), "");
    if (fileNode->GetType() == FSFT_SLICE || fileNode->GetType() == FSFT_MMAP || fileNode->GetType() == FSFT_RESOURCE) {
        return FSEC_OK;
    }

    assert(fileNode->GetType() == FSFT_MEM);
    if (NeedFlush()) {
        if (_entryTable != nullptr) {
            [[maybe_unused]] bool ret = _entryTable->SetEntryMetaIsMemFile(fileNode->GetLogicalPath(), false);
        }
        FileFlushOperationPtr flushOperation(
            new SingleFileFlushOperation(_options->flushRetryStrategy, fileNode, writerOption));
        RETURN_IF_FS_ERROR(AddFlushOperation(flushOperation), "");
    }
    return FSEC_OK;
}

ErrorCode MemStorage::AddFlushOperation(const FileFlushOperationPtr& fileFlushOperation) noexcept
{
    assert(NeedFlush());
    if (_options->prohibitInMemDump) {
        RETURN_IF_FS_ERROR(fileFlushOperation->Execute(), "");
        FileList fileList;
        fileFlushOperation->GetFileNodePaths(fileList);
        [[maybe_unused]] auto ec = _entryTable->Freeze(fileList);
        _fileNodeCache->CleanFiles(fileList);
        return FSEC_OK;
    }
    ScopedLock lock(_lock);
    assert(_operationQueue);
    _operationQueue->PushBack(fileFlushOperation);
    return FSEC_OK;
}

ErrorCode MemStorage::FlushPackage(const string& logicalDirPath) noexcept
{
    assert(false);
    return FSEC_NOTSUP;
}
}} // namespace indexlib::file_system
