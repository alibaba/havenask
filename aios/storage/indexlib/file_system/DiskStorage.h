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
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/LifecycleTable.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/Storage.h"
#include "indexlib/file_system/file/DirectoryMapIterator.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/package/PackageOpenMeta.h"

namespace indexlib::util {
class BlockMemoryQuotaController;
}

namespace indexlib { namespace file_system {

class BlockFileNodeCreator;
class FileNodeCreator;
class LoadConfig;
struct FileSystemOptions;
struct WriterOption;

class DiskStorage : public Storage
{
public:
    DiskStorage(bool isReadonly, const std::shared_ptr<util::BlockMemoryQuotaController>& memController,
                std::shared_ptr<EntryTable> entryTable) noexcept;
    ~DiskStorage() noexcept;

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
    ErrorCode WaitSyncFinish() noexcept override { return FSEC_OK; };
    FSResult<int32_t> CommitPackage() noexcept override;
    void CleanCache() noexcept override;
    bool SetDirLifecycle(const std::string& logicalDirPath, const std::string& lifecycle) noexcept override;

    FSStorageType GetStorageType() const noexcept override { return FSST_DISK; }
    std::string DebugString() const noexcept override { return "not implemented"; }
    void TEST_EnableSupportMmap(bool isEnable) override { TEST_mEnableSupportMmap = isEnable; }

private:
    typedef std::map<FSOpenType, std::shared_ptr<FileNodeCreator>> FileNodeCreatorMap;
    typedef std::vector<std::shared_ptr<FileNodeCreator>> FileNodeCreatorVec;

private:
    ErrorCode InitDefaultFileNodeCreator() noexcept;
    void DoInitDefaultFileNodeCreator(FileNodeCreatorMap& creatorMap, const LoadConfig& loadConfig) noexcept;
    FSResult<std::shared_ptr<FileNodeCreator>> CreateFileNodeCreator(const LoadConfig& loadConfig) noexcept;
    FSResult<std::shared_ptr<FileNodeCreator>> GetFileNodeCreator(const std::string& logicalFilePath,
                                                                  const std::string& physicalFilePath, FSOpenType type,
                                                                  const std::string lifeCycleInput = "") const noexcept;
    FSResult<std::shared_ptr<FileNodeCreator>>
    DeduceFileNodeCreator(const std::shared_ptr<FileNodeCreator>& creator, FSOpenType type,
                          const std::string& physicalFilePath) const noexcept;
    FSResult<std::shared_ptr<FileNode>> GetFileNode(const std::string& logicalFilePath,
                                                    const std::string& physicalFilePath,
                                                    const ReaderOption& readerOption) noexcept;
    FSResult<std::shared_ptr<FileNode>> CreateFileNode(const std::string& logicalFilePath,
                                                       const std::string& physicalFilePath,
                                                       const ReaderOption& readerOption) noexcept;
    ErrorCode ModifyPackageOpenMeta(PackageOpenMeta& packageOpenMeta) noexcept;
    bool SupportMmap(const std::string& physicalPath) const noexcept;
    ErrorCode GetLoadConfigOpenType(const std::string& logicalPath, const std::string& physicalPath,
                                    ReaderOption& option) const noexcept;

private:
    FileNodeCreatorMap _defaultCreatorMap;
    FileNodeCreatorVec _fileNodeCreators;
    LifecycleTable _lifecycleTable;

    typedef DirectoryMapIterator<FileCarrierPtr> PackageFileCarrierMapIter;
    typedef PackageFileCarrierMapIter::DirectoryMap PackageFileCarrierMap;
    PackageFileCarrierMap _packageFileCarrierMap;
    bool TEST_mEnableSupportMmap = true;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DiskStorage> DiskStoragePtr;
}} // namespace indexlib::file_system
