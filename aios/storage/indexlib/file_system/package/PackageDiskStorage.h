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

#include <functional>
#include <future>
#include <map>
#include <memory>
#include <queue>
#include <set>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/Storage.h"
#include "indexlib/file_system/file/BufferedFileOutputStream.h"
#include "indexlib/file_system/file/FileBuffer.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/package/InnerFileMeta.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/file_system/package/VersionedPackageFileMeta.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace file_system {
struct FileSystemOptions;
struct WriterOption;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

// README: Package data file needs to be aligned to file align size, i.e. each file's offset within a package should be
// multiple of file align size. If file size is not multiple of file align size, padding is needed to append to the end
// of the file.
class PackageDiskStorage : public Storage
{
public:
    PackageDiskStorage(bool isReadonly, const std::string& rootPath, const std::string& fsName,
                       const util::BlockMemoryQuotaControllerPtr& memController,
                       std::shared_ptr<EntryTable> entryTable) noexcept;
    ~PackageDiskStorage() noexcept;

public:
    ErrorCode Init(const std::shared_ptr<FileSystemOptions>& options) noexcept override;
    FSResult<std::shared_ptr<FileReader>> CreateFileReader(const std::string& logicalFilePath,
                                                           const std::string& physicalFilePath,
                                                           const ReaderOption& readerOption) noexcept override;
    FSResult<std::shared_ptr<FileWriter>> CreateFileWriter(const std::string& logicalFilePath,
                                                           const std::string& physicalFilePath,
                                                           const WriterOption& writerOption) noexcept override;
    ErrorCode MakeDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath, bool recursive,
                            bool packageHint) noexcept override;
    ErrorCode StoreFile(const std::shared_ptr<FileNode>& fileNode, const WriterOption& writerOption) noexcept override;

    ErrorCode RecoverPackage(int32_t checkpoint, const std::string& logicalDir,
                             const std::vector<std::string>& physicalFileListHint) noexcept override;
    ErrorCode FlushPackage(const std::string& logicalDirPath) noexcept override;
    FSResult<std::future<bool>> Sync() noexcept override;
    ErrorCode WaitSyncFinish() noexcept override { return FSEC_OK; };
    // flush and seal all opened Physical File Writer
    FSResult<int32_t> CommitPackage() noexcept override;

    void CleanCache() noexcept override { _fileNodeCache->Clean(); }

    std::string DebugString() const noexcept override;

    // un-support interfaces
    ErrorCode RemoveFile(const std::string& logicalFilePath, const std::string& physicalFilePath,
                         FenceContext*) noexcept override;
    ErrorCode RemoveDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath,
                              FenceContext*) noexcept override;
    FSResult<size_t> EstimateFileLockMemoryUse(const std::string& logicalPath, const std::string& physicalPath,
                                               FSOpenType type, int64_t fileLength) noexcept override;
    FSResult<size_t> EstimateFileMemoryUseChange(const std::string& filePath, const std::string& physicalPath,
                                                 const std::string& oldTemperature, const std::string& newTemperature,
                                                 int64_t fileLength) noexcept override;

    FSResult<FSFileType> DeduceFileType(const std::string& logicalPath, const std::string& physicalPath,
                                        FSOpenType type) noexcept override;

    bool SetDirLifecycle(const std::string& logicalDirPath, const std::string& lifecycle) noexcept override
    {
        return true;
    }
    FSStorageType GetStorageType() const noexcept override { return FSST_PACKAGE_DISK; }

private:
    friend class BufferedPackageFileWriter;
    ErrorCode StoreLogicalFile(int32_t unitId, const std::string& logicalFilePath,
                               std::unique_ptr<FileBuffer> buffer) noexcept;
    ErrorCode StoreLogicalFile(int32_t unitId, const std::string& logicalFilePath, uint32_t physicalFileId,
                               size_t fileLength) noexcept;
    std::tuple<ErrorCode, uint32_t, std::shared_ptr<BufferedFileOutputStream>>
    AllocatePhysicalStream(int32_t unitId, const std::string& logicalFilePath) noexcept;
    // flush all small files which not yet allcate physical file writer
    ErrorCode FlushBuffers() noexcept;

private:
    struct UnflushedFile {
        std::string logicalFilePath;
        std::unique_ptr<FileBuffer> buffer;
    };

    // a Unit reference to a segment
    struct Unit {
        Unit(const std::string& _rootPath) : rootPath(_rootPath) {}

        std::unordered_map<std::string, std::queue<uint32_t>> freePhysicalFileIds;
        std::vector<std::tuple<std::string, std::shared_ptr<BufferedFileOutputStream>, std::string>> physicalStreams;
        std::vector<UnflushedFile> unflushedBuffers;
        // metas[KEY->VALUE]
        // FullPath ==
        // <PackageDiskStorage._rootPath> + KEY ==
        // <PackageDiskStorage._rootPath> + <Unit.rootPath> + "/" + <VALUE._filePath>
        std::map<std::string, InnerFileMeta> metas;
        std::string rootPath;
        int32_t nextMetaId = 0;
        // bool needCommit = false;
    };

private:
    bool IsExist(const std::string& path) const noexcept;
    void CleanFromEntryTable(const std::string& logicalDir, const std::set<std::string>& fileNames) noexcept;
    ErrorCode CleanData(const Unit& unit, const std::set<std::string>& fileNames, FenceContext* fenceContext) noexcept;
    ErrorCode MakeDirectoryRecursive(Unit& unit, const std::string& logicalDirPath) noexcept;
    ErrorCode DoMakeDirectory(Unit& unit, const std::string& logicalDirPath) noexcept;
    std::tuple<ErrorCode, uint32_t, std::shared_ptr<BufferedFileOutputStream>>
    DoAllocatePhysicalStream(Unit& unit, const std::string& logicalFilePath) noexcept;
    int32_t MatchUnit(const std::string& relativePath) const noexcept;
    std::string JoinRoot(const std::string& path) const noexcept { return util::PathUtil::JoinPath(_rootPath, path); }
    std::string ExtractTag(const std::string& logicalFilePath) const noexcept;
    ErrorCode RecoverEntryTable(const std::string& logicalDirPath, const std::string& physicalDirPath,
                                const VersionedPackageFileMeta& packageMeta) noexcept;

private:
    mutable autil::RecursiveThreadMutex _lock;
    std::function<std::string(const std::string&)> _tagFunction;
    std::vector<Unit> _units;
    std::string _description; // instanceId_threadId
    std::string _rootPath;    // physical root
    size_t _fileAlignSize;
    bool _isDirty;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PackageDiskStorage> PackageDiskStoragePtr;

//////////////////////////////////////////////////////////////////
inline int32_t PackageDiskStorage::MatchUnit(const std::string& relativePath) const noexcept
{
    int32_t matchUnitId = -1;
    size_t matchPathSize = 0;
    for (size_t i = 0; i < _units.size(); ++i) {
        if (util::PathUtil::IsInRootPath(relativePath, _units[i].rootPath) &&
            _units[i].rootPath.size() >= matchPathSize) {
            matchUnitId = i;
            matchPathSize = _units[i].rootPath.size();
        }
    }
    return matchUnitId;
}
}} // namespace indexlib::file_system
