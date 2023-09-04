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
#include <unordered_map>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/file_system/DiskStorage.h"
#include "indexlib/file_system/EntryMeta.h"
#include "indexlib/file_system/EntryTableBuilder.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemMetrics.h"
#include "indexlib/file_system/FileSystemMetricsReporter.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/MemoryReserver.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace file_system {
class Storage;
struct FileInfo;
struct FileSystemOptions;
struct WriterOption;
struct DirectoryOption;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class LogicalFileSystem : public IFileSystem
{
public:
    LogicalFileSystem(const std::string& name, const std::string& outputRoot,
                      util::MetricProviderPtr metricProvider) noexcept;
    ~LogicalFileSystem() noexcept;

public:
    FSResult<void> Init(const FileSystemOptions& fileSystemOptions) noexcept override;
    FSResult<void> CommitSelectedFilesAndDir(versionid_t versionId, const std::vector<std::string>& fileList,
                                             const std::vector<std::string>& dirList,
                                             const std::vector<std::string>& filterDirList,
                                             const std::string& finalDumpFileName,
                                             const std::string& finalDumpFileContent,
                                             FenceContext* fenceContext) noexcept override;
    // use for branch fs, only allow to be called once for a logical_file_system.
    // see more detail in entry_table.h.
    FSResult<void> CommitPreloadDependence(FenceContext* fenceContext) noexcept override;

    FSResult<void>
    MountVersion(const std::string& physicalRoot, versionid_t versionId, const std::string& logicalPath,
                 MountOption mountOption,
                 const std::shared_ptr<indexlib::file_system::LifecycleTable>& lifecycleTable) noexcept override;

    FSResult<void> MountDir(const std::string& physicalRoot, const std::string& physicalPath,
                            const std::string& logicalPath, MountOption mountOption,
                            bool enableLazyMount) noexcept override;
    FSResult<void> MountFile(const std::string& physicalRoot, const std::string& physicalPath,
                             const std::string& logicalPath, FSMountType mountType, int64_t length,
                             bool mayNonExist) noexcept override;
    FSResult<void> MountSegment(const std::string& logicalPath) noexcept override;

    // mv all files in the @physicalDirs to @logicalPath
    FSResult<void> MergeDirs(const std::vector<std::string>& physicalDirs, const std::string& logicalPath,
                             const MergeDirsOption& mergeDirsOption) noexcept override;
    FSResult<void> CopyToOutputRoot(const std::string& logicalPath, bool mayNonExist) noexcept override;

public:
    FSResult<std::shared_ptr<FileWriter>> CreateFileWriter(const std::string& rawPath,
                                                           const WriterOption& writerOption) noexcept override;
    FSResult<std::shared_ptr<FileReader>> CreateFileReader(const std::string& rawPath,
                                                           const ReaderOption& readerOption) noexcept override;
    FSResult<ResourceFilePtr> CreateResourceFile(const std::string& rawPath) noexcept override;
    FSResult<ResourceFilePtr> GetResourceFile(const std::string& rawPath) const noexcept override;

    FSResult<void> MakeDirectory(const std::string& rawPath, const DirectoryOption& directoryOption) noexcept override;
    FSResult<void> RemoveFile(const std::string& rawPath, const RemoveOption& removeOption) noexcept override;
    FSResult<void> RemoveDirectory(const std::string& rawPath, const RemoveOption& removeOption) noexcept override;
    FSResult<void> Rename(const std::string& rawSrcPath, const std::string& rawDestPath,
                          FenceContext* fenceContext) noexcept override;
    FSResult<bool> IsExist(const std::string& rawPath) const noexcept override;
    FSResult<bool> IsDir(const std::string& rawPath) const noexcept override;
    FSResult<size_t> GetFileLength(const std::string& rawPath) const noexcept override;
    // filePath returned relative to rawPath
    FSResult<void> ListDir(const std::string& rawPath, const ListOption& listOption,
                           std::vector<std::string>& fileList) const noexcept override;
    FSResult<void> ListDir(const std::string& rawPath, const ListOption& listOption,
                           std::vector<FileInfo>& fileInfos) const noexcept override;
    FSResult<void> ListPhysicalFile(const std::string& path, const ListOption& listOption,
                                    std::vector<FileInfo>& fileInfos) noexcept override;

    const std::string& GetOutputRootPath() const noexcept override { return _outputRoot; }
    const std::string& GetRootLinkPath() const noexcept override { return _rootLinkPath; }
    FSResult<std::string> GetPhysicalPath(const std::string& logicalPath) const noexcept override;
    bool IsExistInCache(const std::string& rawPath) const noexcept override;
    const FileSystemOptions& GetFileSystemOptions() const noexcept override { return *_options; }
    FileSystemMetrics GetFileSystemMetrics() const noexcept override;
    FileSystemMetricsReporter* GetFileSystemMetricsReporter() noexcept override { return _metricsReporter.get(); }
    void ReportMetrics() noexcept override;
    FSResult<size_t> EstimateFileLockMemoryUse(const std::string& filePath, FSOpenType type) noexcept override;
    FSResult<size_t> EstimateFileMemoryUseChange(const std::string& rawPath, const std::string& oldTemperature,
                                                 const std::string& newTemperature) noexcept override;
    FSResult<FSFileType> DeduceFileType(const std::string& filePath, FSOpenType type) noexcept override;

    size_t GetFileSystemMemoryUse() const noexcept override { return _memController->GetAllocatedQuota(); }
    util::MemoryReserverPtr CreateMemoryReserver(const std::string& name) noexcept override;
    FSResult<FSStorageType> GetStorageType(const std::string& path) const noexcept override;

    FSResult<void> Validate(const std::string& path) const noexcept override;
    FSResult<void> CalculateVersionFileSize(const std::string& rawPhysicalRoot, const std::string& rawLogicalPath,
                                            versionid_t versionId) noexcept override;

public:
    // control api
    void CleanCache() noexcept override;
    FSResult<std::shared_future<bool>> Sync(bool waitFinish) noexcept override;
    void SwitchLoadSpeedLimit(bool on) noexcept override;
    bool SetDirLifecycle(const std::string& path, const std::string& lifecycle) noexcept override;
    void SetDefaultRootPath(const std::string& defaultLocalPath,
                            const std::string& defaultRemotePath) noexcept override;

public:
    // PackageDiskStorage only
    FSResult<int32_t> CommitPackage() noexcept override;
    // @param physicalFileListHint: for optimize, user should be attention
    FSResult<void> RecoverPackage(int32_t checkpoint, const std::string& logicalPackageHintDirPath,
                                  const std::vector<std::string>& physicalFileListHint) noexcept override;
    FSResult<void> MergePackageFiles(const std::string& logicalPath, FenceContext* fenceContext) noexcept override;
    FSResult<std::unique_ptr<IFileSystem>> CreateThreadOwnFileSystem(const std::string& name) const noexcept override;

    // PackageMemStorage only
    FSResult<void> FlushPackage(const std::string& dirPath) noexcept override;

public:
    std::string DebugString() const noexcept override;
    FSResult<void> TEST_Recover(int32_t checkpoint, const std::string& logicalDirPath) noexcept override;
    FSResult<void> TEST_Commit(int versionId, const std::string& finalDumpFileName,
                               const std::string& finalDumpFileContent) noexcept override;
    bool TEST_GetPhysicalInfo(const std::string& logicalPath, std::string& physicalRoot, std::string& relativePath,
                              bool& inPackage, bool& isDir) const noexcept override;
    void TEST_SetUseRootLink(bool useRootLink) noexcept override;
    std::shared_ptr<Storage> TEST_GetOutputStorage() noexcept override { return _outputStorage; }
    std::shared_ptr<Storage> TEST_GetInputStorage() noexcept override { return _inputStorage; }
    FSResult<void> TEST_Reset(int versionId) noexcept override;
    FSResult<void> TEST_GetFileStat(const std::string& rawPath, FileStat& fileStat) const noexcept override;
    void TEST_MountLastVersion() noexcept(false) override;
    size_t GetVersionFileSize(versionid_t versionId) const noexcept;

private:
    // Create input/output storage
    // ouput storage can be MemStorage/DiskStorage/PackageMemStorage/PackageDiskStorage,
    // specified in FileSystemOPtions, usually in build stage we use
    // MemStorage/PackageMemStorage, in merge stage we use DiskStorage/PackageDiskStorage
    // InitStorage() need to be called after entry table Init()
    FSResult<void> InitStorage() noexcept;
    std::string NormalizeLogicalPath(const std::string& path, bool* isLink = nullptr) const noexcept;
    Storage* GetStorage(const EntryMeta& entryMeta, bool isLink) const noexcept;
    FSResult<EntryMeta> CreateEntryMeta(const std::string& path) noexcept;
    FSResult<EntryMeta> GetEntryMeta(const std::string& logicalPath) const noexcept;
    FSMetricGroup GetMetricGroup(const std::string& normPath) noexcept;
    FSResult<void> DoInit() noexcept;
    FSResult<void> ReInit(int versionId) noexcept;

    FSResult<std::shared_ptr<FileWriter>> CreateMemFileWriter(const std::string& path,
                                                              const WriterOption& writerOption) noexcept;
    FSResult<std::shared_ptr<FileWriter>> CreateSliceFileWriter(const std::string& path,
                                                                const WriterOption& writerOption) noexcept;
    FSResult<std::shared_ptr<FileReader>> CreateSliceFileReader(const std::string& path,
                                                                const ReaderOption& readerOption) noexcept;
    static std::string GetPackageMetaFilePath(const std::string& packageDataFileName) noexcept;

    void TEST_EnableSupportMmap(bool isEnable);

private:
    autil::ThreadMutex _syncLock;
    std::shared_ptr<autil::RecursiveThreadMutex> _lock; // TODO: make sure all relative funcs in lock
    std::string _name;
    std::string _outputRoot;
    EntryTableBuilderPtr _entryTableBuilder;
    std::shared_ptr<EntryTable> _entryTable;
    std::shared_ptr<FileSystemOptions> _options;
    std::shared_ptr<FileSystemMetricsReporter> _metricsReporter;
    util::BlockMemoryQuotaControllerPtr _memController;
    std::shared_ptr<Storage> _inputStorage;
    std::shared_ptr<Storage> _outputStorage;
    std::string _rootLinkPath;
    std::shared_future<bool> _future;
    std::unordered_map<versionid_t, size_t> _versionFileSizes;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<LogicalFileSystem> LogicalFileSystemPtr;

}} // namespace indexlib::file_system
