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
#include "indexlib/file_system/Storage.h"

#include <assert.h>
#include <cstddef>
#include <utility>

#include "alog/Logger.h"
#include "autil/Lock.h"
#include "indexlib/file_system/DiskStorage.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/MemStorage.h"
#include "indexlib/file_system/file/MemFileWriter.h"
#include "indexlib/file_system/file/SliceFileNode.h"
#include "indexlib/file_system/file/SliceFileReader.h"
#include "indexlib/file_system/file/SliceFileWriter.h"
#include "indexlib/file_system/file/SwapMmapFileNode.h"
#include "indexlib/file_system/file/SwapMmapFileReader.h"
#include "indexlib/file_system/package/PackageDiskStorage.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/file_system/package/PackageMemStorage.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, Storage);

#define UPDATE_FILE_SIZE_FUNC(logicalPath)                                                                             \
    [this, logicalPath](int64_t size) { _entryTable && _entryTable->UpdateFileSize(logicalPath, size); }

std::shared_ptr<Storage>
Storage::CreateInputStorage(const shared_ptr<FileSystemOptions>& options,
                            const std::shared_ptr<util::BlockMemoryQuotaController>& memController,
                            const std::shared_ptr<EntryTable>& entryTable,
                            autil::RecursiveThreadMutex* fileSystemLock) noexcept
{
    assert(fileSystemLock);
    std::shared_ptr<Storage> storage(new DiskStorage(true, memController, entryTable));
    storage->_fileSystemLock = fileSystemLock;
    auto ec = storage->Init(options);
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "init storage failed, ec[%d]", ec);
        return std::shared_ptr<Storage>();
    }
    return storage;
}

std::shared_ptr<Storage>
Storage::CreateOutputStorage(const string& outputRoot, const shared_ptr<FileSystemOptions>& options,
                             const std::shared_ptr<util::BlockMemoryQuotaController>& memController,
                             const string& fsName, const std::shared_ptr<EntryTable>& entryTable,
                             autil::RecursiveThreadMutex* fileSystemLock) noexcept
{
    assert(fileSystemLock);
    std::shared_ptr<Storage> storage;
    if (options->outputStorage == FSST_MEM) {
        storage.reset(new MemStorage(false, memController, entryTable));
    } else if (options->outputStorage == FSST_PACKAGE_MEM) {
        storage.reset(new PackageMemStorage(false, memController, entryTable));
    } else if (options->outputStorage == FSST_PACKAGE_DISK) {
        storage.reset(new PackageDiskStorage(false, outputRoot, fsName, memController, entryTable));
    } else {
        storage.reset(new DiskStorage(false, memController, entryTable));
    }

    storage->_fileSystemLock = fileSystemLock;
    auto ec = storage->Init(options);
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "init storage failed, ec[%d]", ec);
        return std::shared_ptr<Storage>();
    }
    return storage;
}

Storage::Storage(bool isReadonly, const std::shared_ptr<util::BlockMemoryQuotaController>& memController,
                 std::shared_ptr<EntryTable> entryTable) noexcept
    : _fileNodeCache(new FileNodeCache(&_storageMetrics))
    , _memController(memController)
    , _entryTable(entryTable)
    , _isReadOnly(isReadonly)
{
}

Storage::~Storage() noexcept {}

FSResult<ResourceFilePtr> Storage::CreateResourceFile(const string& logicalFilePath, const string& physicalFilePath,
                                                      FSMetricGroup metricGroup) noexcept
{
    AUTIL_LOG(DEBUG, "Create resource file [%s] => [%s]", logicalFilePath.c_str(), physicalFilePath.c_str());
    auto ret = GetResourceFile(logicalFilePath);
    if (ret.ec == FSEC_OK && ret.result) {
        return ret;
    }

    std::shared_ptr<ResourceFileNode> resourceFileNode(new ResourceFileNode(_memController));
    RETURN2_IF_FS_ERROR(resourceFileNode->Open(logicalFilePath, physicalFilePath, FSOT_RESOURCE, -1), ResourceFilePtr(),
                        "open [%s] failed", logicalFilePath.c_str());
    // resource file will be clean by CleanCache
    resourceFileNode->SetDirty(false);
    resourceFileNode->SetCleanCallback([this, logicalFilePath] {
        if (_fileSystemLock) {
            autil::ScopedLock lock(*_fileSystemLock);
            if (_entryTable) {
                _entryTable->Delete(logicalFilePath);
            }
            return;
        }
        if (_entryTable) {
            _entryTable->Delete(logicalFilePath);
        }
    });
    resourceFileNode->SetStorageMetrics(&_storageMetrics, metricGroup);
    // diff from other writer(buffered & inMem),
    // appears when open for read & write in same time
    // can not use StoreWhenNotExist for ut ParallelBuildInteTest.TestBeginMergeRestartForUpdateConfig
    auto ec = StoreFile(resourceFileNode);
    if (ec != FSEC_OK) {
        return {ec, ResourceFilePtr()};
    }
    return {FSEC_OK, ResourceFilePtr(new ResourceFile(resourceFileNode, UPDATE_FILE_SIZE_FUNC(logicalFilePath)))};
}

FSResult<ResourceFilePtr> Storage::GetResourceFile(const string& logicalFilePath) const noexcept
{
    std::shared_ptr<FileNode> fileNode = _fileNodeCache->Find(logicalFilePath);
    if (!fileNode) {
        return {FSEC_OK, ResourceFilePtr()}; // compitable with old code
    }

    auto resourceFileNode = std::dynamic_pointer_cast<ResourceFileNode>(fileNode);
    if (!resourceFileNode) {
        AUTIL_LOG(ERROR, "File[%s] is not resource file, actual type is [%d]", logicalFilePath.c_str(),
                  fileNode->GetType());
        return {FSEC_ERROR, ResourceFilePtr()};
    }
    return {FSEC_OK, ResourceFilePtr(new ResourceFile(resourceFileNode, UPDATE_FILE_SIZE_FUNC(logicalFilePath)))};
}

ErrorCode Storage::StoreWhenNonExist(const std::shared_ptr<FileNode>& fileNode) noexcept
{
    const string& filePath = fileNode->GetLogicalPath();
    if (IsExistInCache(filePath)) {
        std::shared_ptr<FileNode> nodeInCache = _fileNodeCache->Find(filePath);
        if (nodeInCache.get() == fileNode.get()) {
            return FSEC_OK;
        }
        AUTIL_LOG(ERROR, "store file node [%s] failed, different file node already exist. old len[%lu] new len[%lu] ",
                  filePath.c_str(), nodeInCache->GetLength(), fileNode->GetLength());
        return FSEC_EXIST;
    }
    _fileNodeCache->Insert(fileNode);
    return FSEC_OK;
}

void Storage::Truncate(const string& filePath, size_t newLength) noexcept
{
    if (IsExistInCache(filePath)) {
        _fileNodeCache->Truncate(filePath, newLength);
        return;
    }
    assert(false && "only support file in cache now");
}

void Storage::TEST_GetFileStat(const string& logicalFilePath, FileStat& fileStat) const noexcept
{
    // if filePath belongs to packagefile, fileStat.OnDisk always be false
    // todo: check innerfile's package file if on disk or not.
    fileStat.fileType = FSFT_UNKNOWN;
    fileStat.openType = FSOT_UNKNOWN;
    fileStat.inCache = IsExistInCache(logicalFilePath);
    if (fileStat.inCache) {
        const std::shared_ptr<FileNode>& fileNode = _fileNodeCache->Find(logicalFilePath);
        assert(fileNode);
        fileStat.fileType = fileNode->GetType();
        fileStat.openType = fileNode->GetOpenType();
    }
}

FSResult<std::shared_ptr<FileReader>> Storage::CreateSwapMmapFileReader(const string& logicalFilePath,
                                                                        const string& physicalFilePath,
                                                                        const ReaderOption& readerOption) noexcept
{
    std::shared_ptr<SwapMmapFileNode> swapMmapFileNode;
    std::shared_ptr<FileNode> fileNode = _fileNodeCache->Find(logicalFilePath);
    if (fileNode) {
        swapMmapFileNode = std::dynamic_pointer_cast<SwapMmapFileNode>(fileNode);
    }

    if (!swapMmapFileNode) {
        swapMmapFileNode.reset(new SwapMmapFileNode(readerOption.fileLength, _memController));
        RETURN2_IF_FS_ERROR(swapMmapFileNode->OpenForRead(logicalFilePath, physicalFilePath, FSOT_MMAP),
                            std::shared_ptr<FileReader>(), "OpenForRead failed, [%s => %s]", logicalFilePath.c_str(),
                            physicalFilePath.c_str());
        swapMmapFileNode->SetDirty(false);
        swapMmapFileNode->SetCleanCallback([this, logicalFilePath] {
            autil::ScopedLock lock(*_fileSystemLock);
            _entryTable->Delete(logicalFilePath);
        });
        _fileNodeCache->Insert(swapMmapFileNode);
        _storageMetrics.IncreaseFileCacheMiss();
    } else {
        _storageMetrics.IncreaseFileCacheHit();
    }
    return {FSEC_OK, SwapMmapFileReaderPtr(new SwapMmapFileReader(swapMmapFileNode))};
}

FSResult<std::shared_ptr<FileReader>> Storage::CreateSliceFileReader(const string& logicalFilePath,
                                                                     const string& physicalFilePath,
                                                                     const ReaderOption& readerOption) noexcept
{
    assert(readerOption.openType == FSOT_SLICE);
    std::shared_ptr<FileNode> fileNode = _fileNodeCache->Find(logicalFilePath);
    if (!fileNode || fileNode->GetType() != FSFT_SLICE) {
        return {FSEC_NOENT, std::shared_ptr<SliceFileReader>()};
    }

    std::shared_ptr<SliceFileNode> sliceFileNode = std::dynamic_pointer_cast<SliceFileNode>(fileNode);
    if (!sliceFileNode) {
        AUTIL_LOG(ERROR, "File[%s] is not slice file, actual type is [%d]", logicalFilePath.c_str(),
                  fileNode->GetType());
        return {FSEC_ERROR, std::shared_ptr<SliceFileReader>()};
    }
    return {FSEC_OK, std::shared_ptr<SliceFileReader>(new SliceFileReader(sliceFileNode))};
}

FSResult<std::shared_ptr<FileWriter>> Storage::CreateSliceFileWriter(const string& logicalFilePath,
                                                                     const string& physicalFilePath,
                                                                     const WriterOption& writerOption) noexcept
{
    std::shared_ptr<FileNode> fileNode = _fileNodeCache->Find(logicalFilePath);
    if (fileNode) {
        std::shared_ptr<SliceFileNode> sliceFileNode = std::dynamic_pointer_cast<SliceFileNode>(fileNode);
        if (!sliceFileNode) {
            AUTIL_LOG(ERROR, "File[%s] is not slice file, actual type is [%d]", logicalFilePath.c_str(),
                      fileNode->GetType());
            return {FSEC_ERROR, std::shared_ptr<FileWriter>()};
        }
        std::shared_ptr<SliceFileWriter> sliceFileWriter(
            new SliceFileWriter(sliceFileNode, UPDATE_FILE_SIZE_FUNC(logicalFilePath)));
        auto ec = sliceFileWriter->Open(logicalFilePath, physicalFilePath);
        RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "FileWriter Open [%s] ==> [%s] failed",
                            logicalFilePath.c_str(), physicalFilePath.c_str());
        return {FSEC_OK, sliceFileWriter};
    }

    std::shared_ptr<SliceFileNode> sliceFileNode(
        new SliceFileNode(writerOption.sliceLen, writerOption.sliceNum, _memController));
    RETURN2_IF_FS_ERROR(sliceFileNode->Open(logicalFilePath, physicalFilePath, FSOT_SLICE, writerOption.fileLength),
                        std::shared_ptr<FileWriter>(), "SliceFileNode Open failed, [%s => %s]", logicalFilePath.c_str(),
                        physicalFilePath.c_str());
    // slice file will be clean by CleanCache, so need auto rm it self from entryTable when ~SliceFileNode()
    sliceFileNode->SetDirty(false);
    sliceFileNode->SetCleanCallback([this, logicalFilePath] {
        autil::ScopedLock lock(*_fileSystemLock);
        _entryTable->Delete(logicalFilePath);
    });
    sliceFileNode->SetStorageMetrics(writerOption.metricGroup, &_storageMetrics);
    auto ec = StoreWhenNonExist(sliceFileNode);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "store file node [%s] ==> [%s] failed",
                        logicalFilePath.c_str(), physicalFilePath.c_str());
    std::shared_ptr<SliceFileWriter> sliceFileWriter(
        new SliceFileWriter(sliceFileNode, UPDATE_FILE_SIZE_FUNC(logicalFilePath)));
    ec = sliceFileWriter->Open(logicalFilePath, physicalFilePath);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "FileWriter Open [%s] ==> [%s] failed",
                        logicalFilePath.c_str(), physicalFilePath.c_str());
    return {FSEC_OK, sliceFileWriter};
}

FSResult<std::shared_ptr<FileWriter>> Storage::CreateMemFileWriter(const string& logicalFilePath,
                                                                   const string& physicalFilePath,
                                                                   const WriterOption& writerOption) noexcept
{
    assert(!_isReadOnly);
    std::shared_ptr<FileWriter> fileWriter(
        new MemFileWriter(_memController, this, writerOption, UPDATE_FILE_SIZE_FUNC(logicalFilePath)));
    auto ec = fileWriter->Open(logicalFilePath, physicalFilePath);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "FileWriter Open [%s] ==> [%s] failed",
                        logicalFilePath.c_str(), physicalFilePath.c_str());
    return {FSEC_OK, fileWriter};
}
}} // namespace indexlib::file_system
