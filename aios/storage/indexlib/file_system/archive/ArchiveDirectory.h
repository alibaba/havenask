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

#include "autil/Log.h"
#include "indexlib/file_system/IDirectory.h"

namespace indexlib::file_system {
class archiveFolder;

class ArchiveDirectory final : public IDirectory
{
public:
    ArchiveDirectory(const std::shared_ptr<ArchiveFolder>& archiveFolder, const std::string& relativePath);
    ~ArchiveDirectory();

public:
    FSResult<std::shared_ptr<IDirectory>> MakeDirectory(const std::string& dirPath,
                                                        const DirectoryOption& directoryOption) noexcept override;
    FSResult<std::shared_ptr<IDirectory>> GetDirectory(const std::string& dirPath) noexcept override;

    FSResult<std::shared_ptr<FileWriter>> CreateFileWriter(const std::string& filePath,
                                                           const WriterOption& writerOption) noexcept override;
    FSResult<std::shared_ptr<FileReader>> CreateFileReader(const std::string& filePath,
                                                           const ReaderOption& readerOption) noexcept override;
    FSResult<void> Close() noexcept override;

    FSResult<bool> IsExist(const std::string& path) const noexcept override;
    FSResult<bool> IsDir(const std::string& path) const noexcept override;
    FSResult<void> ListDir(const std::string& path, const ListOption& listOption,
                           std::vector<std::string>& fileList) const noexcept override;

    const std::string& GetLogicalPath() const noexcept override;
    std::string GetPhysicalPath(const std::string& path) const noexcept override;
    const std::string& GetRootDir() const noexcept override;
    std::string DebugString(const std::string& path) const noexcept override;

public:
    FSResult<void> Load(const std::string& fileName, const ReaderOption& readerOption,
                        std::string& fileContent) noexcept override;
    FSResult<void> Store(const std::string& fileName, const std::string& fileContent,
                         const WriterOption& writerOption) noexcept override;
    FSResult<std::shared_ptr<ResourceFile>> CreateResourceFile(const std::string& path) noexcept override;
    FSResult<std::shared_ptr<ResourceFile>> GetResourceFile(const std::string& path) noexcept override;
    FSResult<void> RemoveDirectory(const std::string& dirPath, const RemoveOption& removeOption) noexcept override;
    FSResult<void> RemoveFile(const std::string& filePath, const RemoveOption& removeOption) noexcept override;
    FSResult<void> Rename(const std::string& srcPath, const std::shared_ptr<IDirectory>& destDirectory,
                          const std::string& destPath) noexcept override;
    FSResult<void> ListPhysicalFile(const std::string& path, const ListOption& listOption,
                                    std::vector<FileInfo>& fileInfos) noexcept override;
    FSResult<size_t> GetFileLength(const std::string& filePath) const noexcept override;
    FSResult<size_t> GetDirectorySize(const std::string& path) noexcept override;

    std::string GetOutputPath() const noexcept override;
    const std::string& GetRootLinkPath() const noexcept override;
    FSResult<size_t> EstimateFileMemoryUse(const std::string& filePath, FSOpenType type) noexcept override;
    FSResult<size_t> EstimateFileMemoryUseChange(const std::string& filePath, const std::string& oldTemperature,
                                                 const std::string& newTemperature) noexcept override;
    FSResult<FSFileType> DeduceFileType(const std::string& filePath, FSOpenType type) noexcept override;

public:
    FSResult<std::shared_future<bool>> Sync(bool waitFinish) noexcept override;
    FSResult<void> MountPackage() noexcept override;
    FSResult<void> FlushPackage() noexcept override;
    FSResult<void> Validate(const std::string& path) const noexcept override;
    bool IsExistInCache(const std::string& path) noexcept override;
    const std::shared_ptr<IFileSystem>& GetFileSystem() const noexcept override;
    FSResult<FSStorageType> GetStorageType(const std::string& path) const noexcept override;
    std::shared_ptr<FenceContext> GetFenceContext() const noexcept override;
    FSResult<std::shared_ptr<CompressFileInfo>> GetCompressFileInfo(const std::string& filePath) noexcept override;
    std::shared_ptr<IDirectory> CreateLinkDirectory() const noexcept override;
    std::shared_ptr<ArchiveFolder> LEGACY_CreateArchiveFolder(bool legacyMode,
                                                              const std::string& suffix) noexcept override;
    FSResult<std::shared_ptr<IDirectory>> CreateArchiveDirectory(const std::string& suffix) noexcept override;
    bool SetLifecycle(const std::string& lifecycle) noexcept final override;

private:
    std::string PathToArchiveKey(const std::string& path) const noexcept;

private:
    std::shared_ptr<ArchiveFolder> _archiveFolder;
    std::string _relativePath;
    std::string _logicalPath;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::file_system
