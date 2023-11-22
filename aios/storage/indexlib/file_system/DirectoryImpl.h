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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/fslib/FenceContext.h"

namespace indexlib { namespace file_system {

class DirectoryImpl : public IDirectory
{
public:
    DirectoryImpl() noexcept;
    virtual ~DirectoryImpl() noexcept {}

public:
    FSResult<std::shared_ptr<FileWriter>> CreateFileWriter(const std::string& filePath,
                                                           const WriterOption& writerOption) noexcept override;
    FSResult<std::shared_ptr<FileReader>> CreateFileReader(const std::string& filePath,
                                                           const ReaderOption& readerOption) noexcept override;
    FSResult<std::shared_ptr<ResourceFile>> CreateResourceFile(const std::string& path) noexcept final override;
    FSResult<std::shared_ptr<ResourceFile>> GetResourceFile(const std::string& path) noexcept final override;

    FSResult<void> RemoveDirectory(const std::string& dirPath,
                                   const RemoveOption& removeOption) noexcept final override;
    FSResult<void> RemoveFile(const std::string& filePath, const RemoveOption& removeOption) noexcept final override;
    FSResult<void> Rename(const std::string& srcPath, const std::shared_ptr<IDirectory>& destDirectory,
                          const std::string& destPath) noexcept final override;

    FSResult<bool> IsExist(const std::string& path) const noexcept override;
    FSResult<bool> IsDir(const std::string& path) const noexcept override;
    FSResult<void> ListDir(const std::string& path, const ListOption& listOption,
                           std::vector<std::string>& fileList) const noexcept override;
    FSResult<void> ListPhysicalFile(const std::string& path, const ListOption& listOption,
                                    std::vector<FileInfo>& fileInfos) noexcept final override;
    FSResult<size_t> GetFileLength(const std::string& filePath) const noexcept override;
    FSResult<size_t> GetDirectorySize(const std::string& path) noexcept override;

    FSResult<void> Load(const std::string& fileName, const ReaderOption& readerOption,
                        std::string& fileContent) noexcept override;
    FSResult<void> Store(const std::string& fileName, const std::string& fileContent,
                         const WriterOption& writerOption) noexcept override;

    std::string GetOutputPath() const noexcept override;
    const std::string& GetLogicalPath() const noexcept final override;
    std::string GetPhysicalPath(const std::string& path) const noexcept override;
    const std::string& GetRootLinkPath() const noexcept final override;

public:
    FSResult<std::shared_future<bool>> Sync(bool waitFinish) noexcept override;
    FSResult<void> MountPackage() noexcept final override;
    FSResult<void> FlushPackage() noexcept final override;
    FSResult<void> Validate(const std::string& path) const noexcept final override;

    bool IsExistInCache(const std::string& path) noexcept final override;
    FSResult<FSStorageType> GetStorageType(const std::string& path) const noexcept final override;
    FSResult<std::shared_ptr<CompressFileInfo>>
    GetCompressFileInfo(const std::string& filePath) noexcept final override;
    std::shared_ptr<IDirectory> CreateLinkDirectory() const noexcept final override;
    std::shared_ptr<ArchiveFolder> LEGACY_CreateArchiveFolder(bool legacyMode,
                                                              const std::string& suffix) noexcept final override;
    FSResult<std::shared_ptr<IDirectory>> CreateArchiveDirectory(const std::string& suffix) noexcept final override;
    bool SetLifecycle(const std::string& lifecycle) noexcept final override;

public:
    std::string DebugString(const std::string& path) const noexcept override;

protected:
    virtual FSResult<std::shared_ptr<FileReader>> DoCreateFileReader(const std::string& filePath,
                                                                     const ReaderOption& readerOption) noexcept = 0;
    virtual FSResult<std::shared_ptr<FileWriter>> DoCreateFileWriter(const std::string& filePath,
                                                                     const WriterOption& writerOption) noexcept = 0;
    virtual FSResult<void> InnerRemoveDirectory(const std::string& dirPath,
                                                const RemoveOption& removeOption) noexcept = 0;
    virtual FSResult<void> InnerRemoveFile(const std::string& filePath, const RemoveOption& removeOption) noexcept = 0;
    virtual FSResult<void> InnerRename(const std::string& srcPath, const std::shared_ptr<IDirectory>& destDirectory,
                                       const std::string& destPath, FenceContext* fenceContext) noexcept = 0;

private:
    FSResult<bool> LoadCompressFileInfo(const std::string& filePath,
                                        std::shared_ptr<CompressFileInfo>& compressInfo) noexcept;
    FSResult<std::shared_ptr<FileWriter>> DoCreateCompressFileWriter(const std::string& filePath,
                                                                     const WriterOption& writerOption) noexcept;

private:
    friend class FenceDirectory;
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::file_system
