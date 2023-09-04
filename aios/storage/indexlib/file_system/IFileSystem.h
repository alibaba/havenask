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
#include <string>
#include <vector>

#include "autil/NoCopyable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemMetrics.h"

namespace indexlib::util {
class MemoryReserver;
}

namespace indexlib::file_system {
struct FileInfo;
struct FileSystemOptions;
struct FenceContext;
struct DirectoryOption;
struct ListOption;
struct RemoveOption;
struct ReaderOption;
struct WriterOption;
struct MergeDirsOption;
struct MountOption;
class Storage;
class FileWriter;
class FileReader;
class ResourceFile;
class FileSystemMetricsReporter;
class LifecycleTable;

class IFileSystem : autil::NoMoveable
{
public:
    virtual ~IFileSystem() noexcept = default;

public:
    virtual FSResult<void> Init(const FileSystemOptions& fileSystemOptions) noexcept = 0;
    virtual FSResult<void> CommitSelectedFilesAndDir(versionid_t versionId, const std::vector<std::string>& fileList,
                                                     const std::vector<std::string>& dirList,
                                                     const std::vector<std::string>& filterDirList,
                                                     const std::string& finalDumpFileName,
                                                     const std::string& finalDumpFileContent,
                                                     FenceContext* fenceContext) noexcept = 0;
    virtual FSResult<void> CommitPreloadDependence(FenceContext* fenceContext) noexcept = 0;

    // physicalRoot is the root path of source
    // physicalPath is the relative path of source, physicalFullPath = physicalRoot + physicalPath
    // logicalPath is the full path of target in logical file system, logicalRoot is always ""
    // [physicalRoot]/... -> [logicalPath]/...
    virtual FSResult<void>
    MountVersion(const std::string& physicalRoot, versionid_t versionId, const std::string& logicalPath,
                 MountOption mountOption,
                 const std::shared_ptr<indexlib::file_system::LifecycleTable>& lifecycleTable) noexcept = 0;

    // [physicalRoot + physicalPath]/... -->  [logicalPath]/...
    virtual FSResult<void> MountDir(const std::string& physicalRoot, const std::string& physicalPath,
                                    const std::string& logicalPath, MountOption mountOption,
                                    bool enableLazyMount) noexcept = 0;
    // [physicalRoot + physicalPath] -->  [logicalPath]
    virtual FSResult<void> MountFile(const std::string& physicalRoot, const std::string& physicalPath,
                                     const std::string& logicalPath, FSMountType mountType, int64_t length,
                                     bool mayNonExist) noexcept = 0;
    virtual FSResult<void> MountSegment(const std::string& logicalPath) noexcept = 0;

    virtual FSResult<void> MergeDirs(const std::vector<std::string>& physicalDirs, const std::string& logicalPath,
                                     const MergeDirsOption& mergeDirsOption) noexcept = 0;
    // copy logicalFile to physicalRoot
    virtual FSResult<void> CopyToOutputRoot(const std::string& logicalPath, bool mayNonExist) noexcept = 0;

public:
    virtual FSResult<std::shared_ptr<FileWriter>> CreateFileWriter(const std::string& rawPath,
                                                                   const WriterOption& writerOption) noexcept = 0;
    virtual FSResult<std::shared_ptr<FileReader>> CreateFileReader(const std::string& rawPath,
                                                                   const ReaderOption& readerOption) noexcept = 0;

    virtual FSResult<std::shared_ptr<ResourceFile>> CreateResourceFile(const std::string& path) noexcept = 0;
    virtual FSResult<std::shared_ptr<ResourceFile>> GetResourceFile(const std::string& path) const noexcept = 0;

    virtual FSResult<void> MakeDirectory(const std::string& rawPath,
                                         const DirectoryOption& directoryOption) noexcept = 0;
    virtual FSResult<void> RemoveFile(const std::string& rawPath, const RemoveOption& removeOption) noexcept = 0;
    virtual FSResult<void> RemoveDirectory(const std::string& rawPath, const RemoveOption& removeOption) noexcept = 0;
    virtual FSResult<void> Rename(const std::string& rawSrcPath, const std::string& rawDestPath,
                                  FenceContext* fenceContext) noexcept = 0;
    virtual FSResult<bool> IsExist(const std::string& path) const noexcept = 0;
    virtual FSResult<bool> IsDir(const std::string& path) const noexcept = 0;
    virtual FSResult<size_t> GetFileLength(const std::string& filePath) const noexcept = 0;
    virtual FSResult<void> ListDir(const std::string& rawPath, const ListOption& listOption,
                                   std::vector<std::string>& fileList) const noexcept = 0;
    virtual FSResult<void> ListDir(const std::string& rawPath, const ListOption& listOption,
                                   std::vector<FileInfo>& fileInfos) const noexcept = 0;
    virtual FSResult<void> ListPhysicalFile(const std::string& path, const ListOption& listOption,
                                            std::vector<FileInfo>& fileInfos) noexcept = 0;

    virtual const std::string& GetOutputRootPath() const noexcept = 0;
    virtual const std::string& GetRootLinkPath() const noexcept = 0;
    virtual FSResult<std::string> GetPhysicalPath(const std::string& logicalPath) const noexcept = 0;
    virtual bool IsExistInCache(const std::string& path) const noexcept = 0;
    virtual const FileSystemOptions& GetFileSystemOptions() const noexcept = 0;
    virtual FileSystemMetrics GetFileSystemMetrics() const noexcept = 0;
    virtual FileSystemMetricsReporter* GetFileSystemMetricsReporter() noexcept = 0;
    virtual void ReportMetrics() noexcept = 0;
    virtual FSResult<size_t> EstimateFileLockMemoryUse(const std::string& filePath, FSOpenType type) noexcept = 0;
    virtual FSResult<size_t> EstimateFileMemoryUseChange(const std::string& filePath, const std::string& oldTemperature,
                                                         const std::string& newTemperature) noexcept = 0;
    virtual FSResult<FSFileType> DeduceFileType(const std::string& filePath, FSOpenType type) noexcept = 0;

    virtual size_t GetFileSystemMemoryUse() const noexcept = 0;
    virtual std::shared_ptr<util::MemoryReserver> CreateMemoryReserver(const std::string& name) noexcept = 0;
    virtual FSResult<FSStorageType> GetStorageType(const std::string& path) const noexcept = 0;

    virtual FSResult<void> Validate(const std::string& path) const noexcept = 0;
    virtual FSResult<void> CalculateVersionFileSize(const std::string& rawPhysicalRoot,
                                                    const std::string& rawLogicalPath,
                                                    versionid_t versionId) noexcept = 0;

public:
    // control api
    virtual void CleanCache() noexcept = 0;
    // sync all file to disk, future for swift memory topoic
    virtual FSResult<std::shared_future<bool>> Sync(bool waitFinish) noexcept = 0;
    virtual void SwitchLoadSpeedLimit(bool on) noexcept = 0; // true for switch on, false for switch off
    virtual bool SetDirLifecycle(const std::string& path, const std::string& lifecycle) noexcept = 0;
    virtual void SetDefaultRootPath(const std::string& defaultLocalPath,
                                    const std::string& defaultRemotePath) noexcept = 0;

public:
    // package
    // PackageDiskStorage ONLY, sync and commit a versioned package file meta, return checkpoint for RecoverPackage
    virtual FSResult<int32_t> CommitPackage() noexcept = 0;
    // PackageDiskStorage ONLY, recover from checkpoint from  CommitPackage and clean files not in package file meta
    virtual FSResult<void> RecoverPackage(int32_t checkpoint, const std::string& logicalPath,
                                          const std::vector<std::string>& fileListInPath) noexcept = 0;
    virtual FSResult<void> MergePackageFiles(const std::string& logicalPath, FenceContext* fenceContext) noexcept = 0;
    virtual FSResult<std::unique_ptr<IFileSystem>>
    CreateThreadOwnFileSystem(const std::string& name) const noexcept = 0;
    // PackageMemStorage ONLY,
    virtual FSResult<void> FlushPackage(const std::string& dirPath) noexcept = 0;

public:
    virtual std::string DebugString() const noexcept = 0;
    virtual FSResult<void> TEST_Recover(int32_t checkpoint, const std::string& logicalDirPath) noexcept = 0;
    virtual FSResult<void> TEST_Commit(int versionId, const std::string& finalDumpFileName = "",
                                       const std::string& finalDumpFileContent = "") noexcept = 0;
    virtual bool TEST_GetPhysicalInfo(const std::string& logicalPath, std::string& physicalRoot,
                                      std::string& relativePath, bool& inPackage, bool& isDir) const noexcept = 0;
    virtual void TEST_SetUseRootLink(bool useRootLink) noexcept = 0;
    virtual void TEST_MountLastVersion() noexcept(false) = 0;
    virtual std::shared_ptr<Storage> TEST_GetOutputStorage() noexcept = 0;
    virtual std::shared_ptr<Storage> TEST_GetInputStorage() noexcept = 0;
    virtual FSResult<void> TEST_Reset(int versionId) noexcept = 0;
    virtual FSResult<void> TEST_GetFileStat(const std::string& filePath, FileStat& fileStat) const noexcept = 0;
};
typedef std::shared_ptr<IFileSystem> IFileSystemPtr;

} // namespace indexlib::file_system
