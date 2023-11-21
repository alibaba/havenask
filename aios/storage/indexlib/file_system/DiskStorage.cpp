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
#include "indexlib/file_system/DiskStorage.h"

#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <type_traits>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/file/BlockFileNodeCreator.h"
#include "indexlib/file_system/file/BufferedFileNodeCreator.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileCarrier.h"
#include "indexlib/file_system/file/FileNodeCache.h"
#include "indexlib/file_system/file/FileNodeCreator.h"
#include "indexlib/file_system/file/FileWriterImpl.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/file_system/file/MmapFileNode.h"
#include "indexlib/file_system/file/MmapFileNodeCreator.h"
#include "indexlib/file_system/file/NormalFileReader.h"
#include "indexlib/file_system/file/SessionFileCache.h"
#include "indexlib/file_system/fslib/DeleteOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/file_system/load_config/LoadStrategy.h"
#include "indexlib/file_system/package/InnerFileMeta.h"
#include "indexlib/file_system/package/PackageOpenMeta.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, DiskStorage);

DiskStorage::DiskStorage(bool isReadonly, const std::shared_ptr<util::BlockMemoryQuotaController>& memController,
                         std::shared_ptr<EntryTable> entryTable) noexcept
    : Storage(isReadonly, memController, entryTable)
{
}

DiskStorage::~DiskStorage() noexcept {}

ErrorCode DiskStorage::Init(const std::shared_ptr<FileSystemOptions>& options) noexcept
{
    _options = options;

    RETURN_IF_FS_ERROR(InitDefaultFileNodeCreator(), "InitDefaultFileNodeCreator failed");
    for (size_t i = 0; i < _options->loadConfigList.Size(); ++i) {
        const LoadConfig& loadConfig = _options->loadConfigList.GetLoadConfig(i);
        auto [ec, fileNodeCreator] = CreateFileNodeCreator(loadConfig);
        RETURN_IF_FS_ERROR(ec, "CreateFileNodeCreator failed");
        _fileNodeCreators.push_back(fileNodeCreator);
    }
    return FSEC_OK;
}

ErrorCode DiskStorage::InitDefaultFileNodeCreator() noexcept
{
    AUTIL_LOG(DEBUG, "Init default file node creator");

    SessionFileCachePtr fileCache;
    // online can not use SessionFileCache without SessionFileCache clean logic, in particular
    // fslib prefetech scene
    if (_options->isOffline || _options->TEST_useSessionFileCache) {
        fileCache.reset(new SessionFileCache);
    }

    std::shared_ptr<FileNodeCreator> inMemFileNodeCreator(new MemFileNodeCreator(fileCache));
    [[maybe_unused]] bool ret = inMemFileNodeCreator->Init(LoadConfig(), _memController);
    assert(ret);
    _defaultCreatorMap[FSOT_MEM] = inMemFileNodeCreator;

    std::shared_ptr<FileNodeCreator> mmapFileNodeCreator(new MmapFileNodeCreator());
    ret = mmapFileNodeCreator->Init(LoadConfig(), _memController);
    assert(ret);
    _defaultCreatorMap[FSOT_MMAP] = mmapFileNodeCreator;

    // default should not support cache file reader creator
    std::shared_ptr<FileNodeCreator> bufferedFileNodeCreator(new BufferedFileNodeCreator(fileCache));
    ret = bufferedFileNodeCreator->Init(LoadConfig(), _memController);
    assert(ret);
    _defaultCreatorMap[FSOT_BUFFERED] = bufferedFileNodeCreator;

    const auto& defaultLoadConfig = _options->loadConfigList.GetDefaultLoadConfig();
    auto [ec, defaultFileNodeCreateor] = CreateFileNodeCreator(defaultLoadConfig);
    RETURN_IF_FS_ERROR(ec, "CreateFileNodeCreator for defaultLoadConfig failed");
    _defaultCreatorMap[FSOT_LOAD_CONFIG] = defaultFileNodeCreateor;
    return FSEC_OK;
}

FSResult<std::shared_ptr<FileNodeCreator>> DiskStorage::CreateFileNodeCreator(const LoadConfig& loadConfig) noexcept
{
    AUTIL_LOG(DEBUG, "Create file node creator with LoadConfig[%s]", ToJsonString(loadConfig, true).c_str());

    std::shared_ptr<FileNodeCreator> fileNodeCreator;
    if (loadConfig.GetLoadStrategyName() == READ_MODE_MMAP) {
        fileNodeCreator.reset(new MmapFileNodeCreator());
    } else if (loadConfig.GetLoadStrategyName() == READ_MODE_MEM) {
        fileNodeCreator.reset(new MemFileNodeCreator());
    } else if (loadConfig.GetLoadStrategyName() == READ_MODE_CACHE) {
        fileNodeCreator.reset(
            new BlockFileNodeCreator(_options->fileBlockCacheContainer, _options->fileSystemIdentifier));
    } else if (loadConfig.GetLoadStrategyName() == READ_MODE_GLOBAL_CACHE) {
        fileNodeCreator.reset(
            new BlockFileNodeCreator(_options->fileBlockCacheContainer, _options->fileSystemIdentifier));
    } else {
        AUTIL_LOG(ERROR, "Unknown load config mode[%s]", loadConfig.GetLoadStrategyName().c_str());
        return {FSEC_NOTSUP, std::shared_ptr<FileNodeCreator>()};
    }

    assert(fileNodeCreator);
    if (!fileNodeCreator->Init(loadConfig, _memController)) {
        AUTIL_LOG(ERROR, "init fail with load config[%s]", ToJsonString(loadConfig, true).c_str());
        return {FSEC_BADARGS, std::shared_ptr<FileNodeCreator>()};
    }
    return {FSEC_OK, fileNodeCreator};
}

FSResult<std::shared_ptr<FileNodeCreator>>
DiskStorage::GetFileNodeCreator(const string& logicalFilePath, const string& physicalFilePath, FSOpenType type,
                                const std::string lifeCycleInput) const noexcept
{
    size_t matchedIdx = 0;
    string lifecycle = lifeCycleInput.empty() ? _lifecycleTable.GetLifecycle(logicalFilePath) : lifeCycleInput;
    for (; matchedIdx < _fileNodeCreators.size(); ++matchedIdx) {
        if (_fileNodeCreators[matchedIdx]->Match(logicalFilePath, lifecycle)) {
            AUTIL_LOG(DEBUG, "File [%s] => [%s] use load config [%d], default open type [%d]", logicalFilePath.c_str(),
                      physicalFilePath.c_str(), (int)matchedIdx, _fileNodeCreators[matchedIdx]->GetDefaultOpenType());
            break;
        }
    }

    if (matchedIdx < _fileNodeCreators.size()) {
        const auto& matchCreator = _fileNodeCreators[matchedIdx];
        AUTIL_LOG(DEBUG,
                  "File [%s] => [%s] check reduce: type [%d], MatchType [%d], GetDefaultOpenType [%d], "
                  "IsRemote [%d]",
                  logicalFilePath.c_str(), physicalFilePath.c_str(), type, matchCreator->MatchType(type),
                  matchCreator->GetDefaultOpenType(), matchCreator->IsRemote());
        if (matchCreator->MatchType(type) && !(matchCreator->GetDefaultOpenType() == FSOT_MMAP &&
                                               (matchCreator->IsRemote() || !SupportMmap(physicalFilePath)))) {
            return {FSEC_OK, matchCreator};
        }
        return DeduceFileNodeCreator(matchCreator, type, physicalFilePath);
    }
    return DeduceFileNodeCreator(_defaultCreatorMap.at(FSOT_LOAD_CONFIG), type, physicalFilePath);
}

bool DiskStorage::SupportMmap(const std::string& physicalPath) const noexcept
{
    return likely(TEST_mEnableSupportMmap) && PathUtil::SupportMmap(physicalPath);
}

FSResult<std::shared_ptr<FileNodeCreator>>
DiskStorage::DeduceFileNodeCreator(const FileNodeCreatorPtr& creator, FSOpenType type,
                                   const string& physicalFilePath) const noexcept
{
    if (SupportMmap(physicalFilePath) && creator->GetDefaultOpenType() == FSOT_MMAP && creator->MatchType(type)) {
        return {FSEC_OK, creator};
    }
    FSOpenType deducedType = type;
    if ((type == FSOT_LOAD_CONFIG || type == FSOT_MMAP) && !SupportMmap(physicalFilePath)) {
        deducedType = FSOT_MEM;
    }
    AUTIL_LOG(DEBUG, "File [%s] type [%d], use deduced open type [%d]", physicalFilePath.c_str(), type, deducedType);
    FileNodeCreatorMap::const_iterator it = _defaultCreatorMap.find(deducedType);
    if (it == _defaultCreatorMap.end()) {
        AUTIL_LOG(ERROR, "File [%s], Default not support Open deducedType [%d], use [%d] instead",
                  physicalFilePath.c_str(), deducedType, creator->GetDefaultOpenType());
        return {FSEC_NOTSUP, creator};
    }
    return {FSEC_OK, it->second};
}

FSResult<std::shared_ptr<FileWriter>> DiskStorage::CreateFileWriter(const string& logicalFilePath,
                                                                    const string& physicalFilePath,
                                                                    const WriterOption& writerOption) noexcept
{
    AUTIL_LOG(DEBUG, "Create file writer [%s] => [%s], length[%ld]", logicalFilePath.c_str(), physicalFilePath.c_str(),
              writerOption.fileLength);

    assert(!writerOption.isSwapMmapFile);
    assert(writerOption.openType == FSOT_UNKNOWN || writerOption.openType == FSOT_BUFFERED);

    if (unlikely(_isReadOnly)) {
        assert(false);
        AUTIL_LOG(ERROR, "create file writer on readonly storage, path [%s] => [%s]", logicalFilePath.c_str(),
                  physicalFilePath.c_str());
        return {FSEC_ERROR, std::shared_ptr<FileWriter>()};
    }
    auto updateFileSizeFunc = [this, logicalFilePath](int64_t size) {
        _entryTable && _entryTable->UpdateFileSize(logicalFilePath, size);
    };
    std::shared_ptr<FileWriter> fileWriter(new BufferedFileWriter(writerOption, std::move(updateFileSizeFunc)));
    auto ec = fileWriter->Open(logicalFilePath, physicalFilePath);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "FileWriter Open [%s] ==> [%s] failed",
                        logicalFilePath.c_str(), physicalFilePath.c_str());
    return {FSEC_OK, fileWriter};
}

FSResult<std::shared_ptr<FileReader>> DiskStorage::CreateFileReader(const string& logicalFilePath,
                                                                    const string& physicalFilePath,
                                                                    const ReaderOption& readerOption) noexcept
{
    assert(!(readerOption.ForceSkipCache() && readerOption.cacheFirst));

    if (readerOption.isSwapMmapFile) {
        return CreateSwapMmapFileReader(logicalFilePath, physicalFilePath, readerOption);
    }

    ReaderOption newReaderOption(readerOption);
    if (newReaderOption.openType == FSOT_MEM_ACCESS) {
        RETURN2_IF_FS_ERROR(GetLoadConfigOpenType(logicalFilePath, physicalFilePath, newReaderOption),
                            std::shared_ptr<FileReader>(), "GetLoadConfigOpenType failed");
        if (newReaderOption.openType == FSOT_CACHE) {
            newReaderOption.openType = FSOT_MEM;
        } else {
            assert(newReaderOption.openType == FSOT_MMAP || newReaderOption.openType == FSOT_MEM);
            newReaderOption.openType = FSOT_LOAD_CONFIG;
        }
    }
    // file open type may change by lifecycle
    if (newReaderOption.openType == FSOT_LOAD_CONFIG && !_lifecycleTable.IsEmpty()) {
        RETURN2_IF_FS_ERROR(GetLoadConfigOpenType(logicalFilePath, physicalFilePath, newReaderOption),
                            std::shared_ptr<FileReader>(), "GetLoadConfigOpenType failed");
    }

    auto [ec, fileNode] = newReaderOption.ForceSkipCache()
                              ? CreateFileNode(logicalFilePath, physicalFilePath, newReaderOption)
                              : GetFileNode(logicalFilePath, physicalFilePath, newReaderOption);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileReader>(), "get or create FileNode failed, [%s => %s]",
                        logicalFilePath.c_str(), physicalFilePath.c_str());
    assert(fileNode);
    if (fileNode->GetOpenType() == FSOT_BUFFERED) {
        return {FSEC_OK, BufferedFileReaderPtr(new BufferedFileReader(fileNode))};
    }
    return {FSEC_OK, NormalFileReaderPtr(new NormalFileReader(fileNode))};
}

FSResult<std::shared_ptr<FileNode>> DiskStorage::GetFileNode(const string& logicalFilePath,
                                                             const string& physicalFilePath,
                                                             const ReaderOption& readerOption) noexcept
{
    // TODO: make sure logical path match the fixed physical path. clean cache on remove & rename
    std::shared_ptr<FileNode> fileNode = _fileNodeCache->Find(logicalFilePath);
    if (fileNode && fileNode->GetPhysicalPath() == physicalFilePath) {
        FSOpenType cacheOpenType = fileNode->GetOpenType();
        FSFileType cacheFileType = fileNode->GetType();
        if (readerOption.cacheFirst ||
            fileNode->MatchType(readerOption.openType, readerOption.fileType, readerOption.isWritable)) {
            AUTIL_LOG(DEBUG, "File [%s] => [%s] cache hit, Open type [%d], Cache open type [%d], Cache file type [%d]",
                      logicalFilePath.c_str(), physicalFilePath.c_str(), readerOption.openType, cacheOpenType,
                      cacheFileType);
            _storageMetrics.IncreaseFileCacheHit();
            return {FSEC_OK, fileNode};
        }

        if (readerOption.openType == FSOT_BUFFERED) {
            auto [ec, fileNode] = CreateFileNode(logicalFilePath, physicalFilePath, readerOption);
            RETURN2_IF_FS_ERROR(ec, fileNode, "CreateFileNode failed");
            assert(fileNode);
            return {FSEC_OK, fileNode};
        }
        // replace it
        AUTIL_LOG(DEBUG, "File [%s] => [%s] cache replace, Open type [%d], Cache open type [%d], Cache file type [%d]",
                  logicalFilePath.c_str(), physicalFilePath.c_str(), readerOption.openType, cacheOpenType,
                  cacheFileType);
        if (fileNode->GetType() == FSFT_SLICE || fileNode->GetType() == FSFT_RESOURCE) {
            AUTIL_LOG(ERROR, "File[%s] => [%s] is slice/resource file, filetype[%d]", logicalFilePath.c_str(),
                      physicalFilePath.c_str(), fileNode->GetType());
            return {FSEC_ERROR, std::shared_ptr<FileNode>()};
        }
        _storageMetrics.IncreaseFileCacheReplace();
        fileNode.reset();
    } else {
        AUTIL_LOG(DEBUG, "File [%s] => [%s] cache miss, Open type [%d]", logicalFilePath.c_str(),
                  physicalFilePath.c_str(), readerOption.openType);
        _storageMetrics.IncreaseFileCacheMiss();
    }

    auto [ec, newFileNode] = CreateFileNode(logicalFilePath, physicalFilePath, readerOption);
    RETURN2_IF_FS_ERROR(ec, newFileNode, "CreateFileNode failed, [%s => %s]", logicalFilePath.c_str(),
                        physicalFilePath.c_str());
    assert(newFileNode);

    if (_options->useCache || readerOption.ForceUseCache()) {
        _fileNodeCache->Insert(newFileNode);
    }
    return {FSEC_OK, newFileNode};
}

FSResult<std::shared_ptr<FileNode>> DiskStorage::CreateFileNode(const string& logicalFilePath,
                                                                const string& physicalFilePath,
                                                                const ReaderOption& readerOption) noexcept
{
    FSOpenType type = readerOption.openType;
    bool readOnly = !readerOption.isWritable;
    int64_t fileLength = readerOption.fileLength;
    int64_t fileOffset = readerOption.fileOffset;

    if (unlikely(fileLength < 0)) {
        assert(fileOffset < 0);
        ErrorCode ec = FslibWrapper::GetFileLength(physicalFilePath, fileLength);
        RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileNode>(), "Get file [%s]=>[%s] length failed",
                            logicalFilePath.c_str(), physicalFilePath.c_str());
    }
    if (fileOffset < 0) {
        const auto& [ec, fileNodeCreator] = GetFileNodeCreator(logicalFilePath, physicalFilePath, type);
        RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileNode>(), "GetFileNodeCreator failed");
        std::shared_ptr<FileNode> fileNode = fileNodeCreator->CreateFileNode(type, readOnly, readerOption.linkRoot);
        RETURN2_IF_FS_ERROR(fileNode->Open(logicalFilePath, physicalFilePath, type, fileLength),
                            std::shared_ptr<FileNode>(), "open file node failed");
        return {FSEC_OK, fileNode};
    }

    PackageOpenMeta packageOpenMeta;
    packageOpenMeta.SetPhysicalFilePath(physicalFilePath);
    packageOpenMeta.innerFileMeta.Init(fileOffset, fileLength, 0);
    packageOpenMeta.logicalFilePath = logicalFilePath;
    // physicalFilePath may be a redirected path
    auto [ec, physicalFileLength] = _entryTable->GetPackageFileLength(physicalFilePath);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileNode>(), "Get package file [%s]=>[%s] length failed",
                        logicalFilePath.c_str(), physicalFilePath.c_str());
    packageOpenMeta.physicalFileLength = physicalFileLength;
    RETURN2_IF_FS_ERROR(ModifyPackageOpenMeta(packageOpenMeta), std::shared_ptr<FileNode>(),
                        "ModifyPackageOpenMeta failed");
    FSOpenType realType = type;
    if (packageOpenMeta.physicalFileCarrier && packageOpenMeta.physicalFileCarrier->GetMmapLockFileNode()) {
        AUTIL_LOG(DEBUG, "file[%s] in package[%s] change FSOpenType[%d] to FSOT_MMAP", logicalFilePath.c_str(),
                  packageOpenMeta.GetPhysicalFilePath().c_str(), type);
        realType = FSOT_MMAP;
    }

    const auto& [ec2, fileNodeCreator] = GetFileNodeCreator(logicalFilePath, physicalFilePath, realType);
    RETURN2_IF_FS_ERROR(ec2, std::shared_ptr<FileNode>(), "GetFileNodeCreator failed");
    std::shared_ptr<FileNode> fileNode = fileNodeCreator->CreateFileNode(realType, readOnly, readerOption.linkRoot);
    if (type != realType) {
        fileNode->SetOriginalOpenType(type);
    }
    RETURN2_IF_FS_ERROR(fileNode->Open(packageOpenMeta, realType), std::shared_ptr<FileNode>(), "FileNode Open failed");
    return {FSEC_OK, fileNode};
}

ErrorCode DiskStorage::ModifyPackageOpenMeta(PackageOpenMeta& packageOpenMeta) noexcept
{
    const string& packageFilePath = packageOpenMeta.GetPhysicalFilePath();
    auto it = _packageFileCarrierMap.find(packageFilePath);
    if (it != _packageFileCarrierMap.end()) {
        packageOpenMeta.physicalFileCarrier = it->second;
        return FSEC_OK;
    }
    assert(!packageOpenMeta.physicalFileCarrier);

    // use package file path as @param logical path to match this 'package.[tags].__data__' load config
    auto [ec, fileNodeCreator] = GetFileNodeCreator(packageFilePath, packageFilePath, FSOT_LOAD_CONFIG);
    RETURN_IF_FS_ERROR(ec, "GetFileNodeCreator failed");
    bool isMMapLock = false;
    if (fileNodeCreator->GetDefaultOpenType() == FSOT_MMAP) {
        MmapFileNodeCreatorPtr mmapFileNodeCreator = std::dynamic_pointer_cast<MmapFileNodeCreator>(fileNodeCreator);
        isMMapLock = mmapFileNodeCreator && mmapFileNodeCreator->IsLock();
    }
    if (!isMMapLock) {
        // only support mmap lock
        packageOpenMeta.physicalFileCarrier = FileCarrierPtr();
        _packageFileCarrierMap.insert(it, make_pair(packageFilePath, packageOpenMeta.physicalFileCarrier));
        AUTIL_LOG(INFO, "logical file[%s] in package[%s] do not use share, OpenType[%d]",
                  packageOpenMeta.GetLogicalFilePath().c_str(), packageFilePath.c_str(),
                  fileNodeCreator->GetDefaultOpenType());
        return FSEC_OK;
    }

    AUTIL_LOG(DEBUG, "logical file[%s] in package[%s] use share, OpenType[%d]",
              packageOpenMeta.GetLogicalFilePath().c_str(), packageFilePath.c_str(),
              fileNodeCreator->GetDefaultOpenType());

    std::shared_ptr<MmapFileNode> fileNode =
        std::dynamic_pointer_cast<MmapFileNode>(fileNodeCreator->CreateFileNode(FSOT_MMAP, false, ""));
    RETURN_IF_FS_ERROR(
        fileNode->Open(packageFilePath, packageFilePath, FSOT_MMAP, packageOpenMeta.GetPhysicalFileLength()),
        "FileNode Open failed");
    FileCarrierPtr fileCarrier(new FileCarrier());
    fileCarrier->SetMmapLockFileNode(fileNode);
    _packageFileCarrierMap.insert(it, make_pair(packageFilePath, fileCarrier));
    packageOpenMeta.physicalFileCarrier = _packageFileCarrierMap[packageFilePath];
    return FSEC_OK;
}

ErrorCode DiskStorage::StoreFile(const std::shared_ptr<FileNode>& fileNode, const WriterOption& writerOption) noexcept
{
    assert(fileNode);
    if (_options->useCache) {
        auto ec = StoreWhenNonExist(fileNode);
        RETURN_IF_FS_ERROR(ec, "store file [%s] failed", fileNode->DebugString().c_str());
    }

    if (fileNode->GetType() == FSFT_MEM) {
        BufferedFileWriter writer(writerOption, [](int64_t size) {});
        auto ec = writer.Open(fileNode->GetLogicalPath(), fileNode->GetPhysicalPath()).Code();
        RETURN_IF_FS_ERROR(ec, "writer open [%s] failed", fileNode->DebugString().c_str());
        ec = writer.Write(fileNode->GetBaseAddress(), fileNode->GetLength()).Code();
        if (unlikely(ec != FSEC_OK)) {
            AUTIL_LOG(INFO, "write file[%s] failed", fileNode->DebugString().c_str());
            return FSEC_ERROR;
        }
        RETURN_IF_FS_ERROR(writer.Close(), "writer close failed");
        ec = _entryTable->Freeze(fileNode->GetLogicalPath());
        RETURN_IF_FS_ERROR(ec, "freeze entry [%s] failed", fileNode->DebugString().c_str());
    }
    return FSEC_OK;
}

ErrorCode DiskStorage::MakeDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath,
                                     bool recursive, bool packageHint) noexcept
{
    AUTIL_LOG(DEBUG, "Make dir[%s]:[%s], recursive[%d]", logicalDirPath.c_str(), physicalDirPath.c_str(), recursive);
    if (recursive) {
        // recursive & ignore EXIST exception
        return FslibWrapper::MkDirIfNotExist(physicalDirPath).Code();
    } else {
        return FslibWrapper::MkDir(physicalDirPath, false).Code();
    }
}

ErrorCode DiskStorage::RemoveFile(const std::string& logicalFilePath, const string& physicalFilePath,
                                  FenceContext* fenceContext) noexcept
{
    AUTIL_LOG(DEBUG, "Remove file [%s]", physicalFilePath.c_str());
    _lifecycleTable.RemoveFile(logicalFilePath);
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

ErrorCode DiskStorage::RemoveDirectory(const std::string& logicalDirPath, const string& physicalDirPath,
                                       FenceContext* fenceContext) noexcept
{
    AUTIL_LOG(DEBUG, "Remove directory [%s]", physicalDirPath.c_str());
    _lifecycleTable.RemoveDirectory(logicalDirPath);
    if (!_packageFileCarrierMap.empty()) {
        PackageFileCarrierMapIter dirIter(_packageFileCarrierMap, physicalDirPath);
        while (dirIter.HasNext()) {
            PackageFileCarrierMapIter::Iterator it = dirIter.Next();
            AUTIL_LOG(INFO, "release package data file node[%s]", it->first.c_str());
            dirIter.Erase(it);
        }
        assert(_packageFileCarrierMap.count(physicalDirPath) == 0);
    }
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

ErrorCode DiskStorage::GetLoadConfigOpenType(const string& logicalPath, const string& physicalPath,
                                             ReaderOption& option) const noexcept
{
    const auto& [ec, fileNodeCreator] = GetFileNodeCreator(logicalPath, physicalPath, FSOT_LOAD_CONFIG);
    RETURN_IF_FS_ERROR(ec, "GetFileNodeCreator failed");
    assert(fileNodeCreator);
    option.openType = fileNodeCreator->GetDefaultOpenType();
    assert(option.openType == FSOT_CACHE || option.openType == FSOT_MMAP || option.openType == FSOT_MEM);
    if (option.openType == FSOT_MMAP) {
        MmapFileNodeCreatorPtr nodeCreate = std::dynamic_pointer_cast<MmapFileNodeCreator>(fileNodeCreator);
        if (nodeCreate) {
            option.fileType = nodeCreate->IsLock() ? FSFT_MMAP_LOCK : FSFT_MMAP;
        }
    }
    return FSEC_OK;
}

FSResult<FSFileType> DiskStorage::DeduceFileType(const std::string& logicalPath, const std::string& physicalPath,
                                                 FSOpenType openType) noexcept
{
    switch (openType) {
    case FSOT_CACHE:
        return {FSEC_OK, FSFT_BLOCK};
    case FSOT_MEM:
        return {FSEC_OK, FSFT_MEM};
    case FSOT_RESOURCE:
        return {FSEC_OK, FSFT_RESOURCE};
    case FSOT_SLICE:
        return {FSEC_OK, FSFT_SLICE};
    case FSOT_BUFFERED:
        return {FSEC_OK, FSFT_BUFFERED};
    case FSOT_UNKNOWN:
        return {FSEC_UNKNOWN, FSFT_UNKNOWN};
    default:
        assert(openType == FSOT_LOAD_CONFIG || openType == FSOT_MEM_ACCESS || openType == FSOT_MMAP);
    }

    ReaderOption option(openType);
    if (openType == FSOT_MEM_ACCESS) {
        RETURN2_IF_FS_ERROR(GetLoadConfigOpenType(logicalPath, physicalPath, option), FSFT_UNKNOWN,
                            "GetLoadConfigOpenType failed");
        if (option.openType == FSOT_CACHE) {
            AUTIL_LOG(DEBUG,
                      "open file [%s => %s] with FSOT_LOAD_CONFIG does not support getBaseAddress, rewrite to FSOT_MEM",
                      logicalPath.c_str(), physicalPath.c_str());
            option.openType = FSOT_MEM;
        } else {
            if (unlikely(option.openType != FSOT_MMAP && option.openType != FSOT_MEM)) {
                // 二级processor的远端路径是离线pangu://，不支持mmap，配置mmap时会被转写
                AUTIL_LOG(ERROR, "unexpect file [%s => %s] open type [%d]", logicalPath.c_str(), physicalPath.c_str(),
                          option.openType);
                assert(false);
            }
            option.openType = FSOT_LOAD_CONFIG;
        }
    }
    const auto& [ec, fileNodeCreator] = GetFileNodeCreator(logicalPath, physicalPath, option.openType);
    RETURN2_IF_FS_ERROR(ec, FSFT_UNKNOWN, "GetFileNodeCreator [%s => %s] failed", logicalPath.c_str(),
                        physicalPath.c_str());
    assert(fileNodeCreator);
    option.openType = fileNodeCreator->GetDefaultOpenType();
    assert(option.openType == FSOT_CACHE || option.openType == FSOT_MMAP || option.openType == FSOT_MEM);
    if (option.openType == FSOT_MMAP) {
        MmapFileNodeCreatorPtr nodeCreate = std::dynamic_pointer_cast<MmapFileNodeCreator>(fileNodeCreator);
        if (nodeCreate && nodeCreate->IsLock()) {
            return {FSEC_OK, FSFT_MMAP_LOCK};
        }
    }
    switch (option.openType) {
    case FSOT_CACHE:
        return {FSEC_OK, FSFT_BLOCK};
    case FSOT_MEM:
        return {FSEC_OK, FSFT_MEM};
    case FSOT_MMAP:
        return {FSEC_OK, FSFT_MMAP};
    default:
        return {FSEC_UNKNOWN, FSFT_UNKNOWN};
    }
    assert(false);
    return {FSEC_UNKNOWN, FSFT_UNKNOWN};
}

FSResult<size_t> DiskStorage::EstimateFileMemoryUseChange(const std::string& logicalPath,
                                                          const std::string& physicalPath,
                                                          const std::string& oldTemperature,
                                                          const std::string& newTemperature,
                                                          int64_t fileLength) noexcept
{
    const auto& [oldec, oldCreator] = GetFileNodeCreator(logicalPath, physicalPath, FSOT_LOAD_CONFIG, oldTemperature);
    RETURN2_IF_FS_ERROR(oldec, 0ul, "GetFileNodeCreator failed");
    const auto& [newec, newCreator] = GetFileNodeCreator(logicalPath, physicalPath, FSOT_LOAD_CONFIG, newTemperature);
    RETURN2_IF_FS_ERROR(newec, 0ul, "GetFileNodeCreator failed");
    if (oldCreator->EstimateFileLockMemoryUse(fileLength) != newCreator->EstimateFileLockMemoryUse(fileLength)) {
        return {FSEC_OK, newCreator->EstimateFileLockMemoryUse(fileLength)};
    }
    return {FSEC_OK, 0};
}

FSResult<size_t> DiskStorage::EstimateFileLockMemoryUse(const std::string& logicalPath, const std::string& physicalPath,
                                                        FSOpenType openType, int64_t fileLength) noexcept
{
    ReaderOption option(openType);
    if (openType == FSOT_MEM_ACCESS) {
        RETURN2_IF_FS_ERROR(GetLoadConfigOpenType(logicalPath, physicalPath, option), 0ul,
                            "GetLoadConfigOpenType failed");
        if (option.openType == FSOT_CACHE) {
            AUTIL_LOG(DEBUG,
                      "open file [%s => %s] with FSOT_LOAD_CONFIG does not support getBaseAddress, rewrite to FSOT_MEM",
                      logicalPath.c_str(), physicalPath.c_str());
            option.openType = FSOT_MEM;
        } else {
            if (unlikely(option.openType != FSOT_MMAP && option.openType != FSOT_MEM)) {
                // 二级processor的远端路径是离线pangu://，不支持mmap，配置mmap时会被转写
                AUTIL_LOG(ERROR, "unexpect file [%s => %s] open type [%d]", logicalPath.c_str(), physicalPath.c_str(),
                          option.openType);
                assert(false);
            }
            option.openType = FSOT_LOAD_CONFIG;
        }
    } else if (option.openType == FSOT_RESOURCE || option.openType == FSOT_SLICE) {
        return {FSEC_OK, (size_t)fileLength};
    }
    const auto& [ec, fileNodeCreator] = GetFileNodeCreator(logicalPath, physicalPath, option.openType);
    RETURN2_IF_FS_ERROR(ec, 0ul, "GetFileNodeCreator [%s => %s] failed", logicalPath.c_str(), physicalPath.c_str());
    return {FSEC_OK, fileNodeCreator->EstimateFileLockMemoryUse(fileLength)};
}

ErrorCode DiskStorage::FlushPackage(const string& logicalDirPath) noexcept
{
    assert(false);
    return FSEC_NOTSUP;
}

FSResult<std::future<bool>> DiskStorage::Sync() noexcept
{
    std::promise<bool> p;
    auto f = p.get_future();
    p.set_value(true);
    return {FSEC_OK, std::move(f)};
}

FSResult<int32_t> DiskStorage::CommitPackage() noexcept
{
    assert(false);
    return {FSEC_NOTSUP, -1};
}

ErrorCode DiskStorage::RecoverPackage(int32_t checkpoint, const std::string& logicalDir,
                                      const std::vector<std::string>& physicalFileListHint) noexcept
{
    assert(false);
    return FSEC_NOTSUP;
}

void DiskStorage::CleanCache() noexcept
{
    _fileNodeCache->Clean();
    for (auto it = _packageFileCarrierMap.begin(); it != _packageFileCarrierMap.end();) {
        if ((!it->second) || (it->second->GetMmapLockFileNodeUseCount() <= 1)) {
            it = _packageFileCarrierMap.erase(it);
        } else {
            ++it;
        }
    }
}

bool DiskStorage::SetDirLifecycle(const string& logicalDirPath, const string& lifecycle) noexcept
{
    return _lifecycleTable.AddDirectory(logicalDirPath, lifecycle);
}
}} // namespace indexlib::file_system
