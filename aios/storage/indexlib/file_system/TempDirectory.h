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
#include "indexlib/file_system/DirectoryImpl.h"

namespace indexlib::file_system {

class TempDirectory : public DirectoryImpl
{
public:
    using CloseFunc = std::function<FSResult<void>(const std::string&)>;
    TempDirectory(std::shared_ptr<IDirectory>& directory, CloseFunc&& closeFunc, const std::string& dirName,
                  FileSystemMetricsReporter* reporter) noexcept;
    ~TempDirectory();

    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;
    TempDirectory(TempDirectory&&) = delete;
    TempDirectory& operator=(TempDirectory&&) = delete;

public:
    /*
      this will call fence dir closeTempPath, it will remove temp dir file to root path
    */
    FSResult<void> Close() noexcept override;
    FSResult<std::shared_ptr<FileWriter>> CreateFileWriter(const std::string& filePath,
                                                           const WriterOption& writerOption) noexcept override;
    FSResult<std::shared_ptr<FileReader>> CreateFileReader(const std::string& filePath,
                                                           const ReaderOption& readerOption) noexcept override;

    // for write, make directory recursively & ignore exist error
    FSResult<std::shared_ptr<IDirectory>> MakeDirectory(const std::string& dirPath,
                                                        const DirectoryOption& directoryOption) noexcept override;
    // for rea
    FSResult<std::shared_ptr<IDirectory>> GetDirectory(const std::string& dirPath) noexcept override;

    FSResult<bool> IsExist(const std::string& path) const noexcept override;
    FSResult<bool> IsDir(const std::string& path) const noexcept override;
    FSResult<void> ListDir(const std::string& path, const ListOption& listOption,
                           std::vector<std::string>& fileList) const noexcept override;
    FSResult<void> Load(const std::string& fileName, const ReaderOption& readerOption,
                        std::string& fileContent) noexcept override;
    FSResult<size_t> GetFileLength(const std::string& filePath) const noexcept override;
    std::string GetOutputPath() const noexcept override;
    std::string GetPhysicalPath(const std::string& path) const noexcept override;

    std::string DebugString(const std::string& path) const noexcept override;
    FSResult<std::shared_future<bool>> Sync(bool waitFinish) noexcept override;

    FSResult<size_t> EstimateFileMemoryUse(const std::string& filePath, FSOpenType type) noexcept override;
    FSResult<size_t> EstimateFileMemoryUseChange(const std::string& filePath, const std::string& oldTemperature,
                                                 const std::string& newTemperature) noexcept override;
    FSResult<FSFileType> DeduceFileType(const std::string& filePath, FSOpenType type) noexcept override;
    const std::shared_ptr<IFileSystem>& GetFileSystem() const noexcept override;
    std::shared_ptr<FenceContext> GetFenceContext() const noexcept override;

protected:
    FSResult<std::shared_ptr<FileReader>> DoCreateFileReader(const std::string& filePath,
                                                             const ReaderOption& readerOption) noexcept override;
    FSResult<std::shared_ptr<FileWriter>> DoCreateFileWriter(const std::string& filePath,
                                                             const WriterOption& writerOption) noexcept override;
    const std::string& GetRootDir() const noexcept override;
    FSResult<void> InnerRemoveDirectory(const std::string& dirPath, const RemoveOption& removeOption) noexcept override;
    FSResult<void> InnerRemoveFile(const std::string& filePath, const RemoveOption& removeOption) noexcept override;
    FSResult<void> InnerRename(const std::string& srcPath, const std::shared_ptr<IDirectory>& destDirectory,
                               const std::string& destPath, FenceContext* fenceContext) noexcept override;

private:
    bool _isClosed;
    std::shared_ptr<IDirectory> _directory;
    CloseFunc _closeFunc;
    std::string _dirName;
    FileSystemMetricsReporter* _reporter;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TempDirectory> TempDirectoryPtr;
} // namespace indexlib::file_system
