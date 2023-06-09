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

#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/DirectoryImpl.h"

namespace indexlib { namespace file_system {

class MemDirectory final : public DirectoryImpl
{
public:
    MemDirectory(const std::string& path, const std::shared_ptr<IFileSystem>& fileSystem) noexcept;
    ~MemDirectory() noexcept;

public:
    FSResult<std::shared_ptr<IDirectory>> MakeDirectory(const std::string& dirPath,
                                                        const DirectoryOption& directoryOption) noexcept override;
    FSResult<std::shared_ptr<IDirectory>> GetDirectory(const std::string& dirPath) noexcept override;

    FSResult<size_t> EstimateFileMemoryUse(const std::string& filePath, FSOpenType type) noexcept final override;
    FSResult<size_t> EstimateFileMemoryUseChange(const std::string& filePath, const std::string& oldTemperature,
                                                 const std::string& newTemperature) noexcept final override;

    FSResult<FSFileType> DeduceFileType(const std::string& filePath, FSOpenType type) noexcept final override;

    const std::shared_ptr<IFileSystem>& GetFileSystem() const noexcept final override;
    const std::string& GetRootDir() const noexcept final override;
    std::shared_ptr<FenceContext> GetFenceContext() const noexcept final override;
    FSResult<void> Close() noexcept final override;

private:
    FSResult<std::shared_ptr<FileReader>> DoCreateFileReader(const std::string& filePath,
                                                             const ReaderOption& readerOption) noexcept final override;
    FSResult<std::shared_ptr<FileWriter>> DoCreateFileWriter(const std::string& filePath,
                                                             const WriterOption& writerOption) noexcept override;
    FSResult<void> InnerRemoveDirectory(const std::string& dirPath,
                                        const RemoveOption& removeOption) noexcept final override;
    FSResult<void> InnerRemoveFile(const std::string& filePath,
                                   const RemoveOption& removeOption) noexcept final override;
    FSResult<void> InnerRename(const std::string& srcPath, const std::shared_ptr<IDirectory>& destDirectory,
                               const std::string& destPath, FenceContext* fenceContext) noexcept final override;

private:
    std::string _rootDir;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MemDirectory> MemDirectoryPtr;
}} // namespace indexlib::file_system
