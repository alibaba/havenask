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
#include "indexlib/file_system/DirectoryImpl.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FenceContext.h"

namespace indexlib::file_system {

/*
  // pangu scene
  root/
    fence.inline
    pre.__tmp__/
      version.0.epochId
    segment_0/
*/
class FenceDirectory final : public DirectoryImpl
{
public:
    FenceDirectory(const std::shared_ptr<IDirectory>& rootDirectory, const std::shared_ptr<FenceContext>& fenceContext,
                   FileSystemMetricsReporter* reporter);
    ~FenceDirectory();

    FenceDirectory(const FenceDirectory&) = delete;
    FenceDirectory& operator=(const FenceDirectory&) = delete;
    FenceDirectory(FenceDirectory&&) = delete;
    FenceDirectory& operator=(FenceDirectory&&) = delete;

public:
    /*
       root is fence directory
       create file root/filePath will create fileWriter root/pre.__tmp__/filePath.epochId
       not write to root, but write in a pre dir
       after file is close, it will rename root/pre.__tmp__/filePath.epochId to root/filePath
     */
    FSResult<std::shared_ptr<FileWriter>> CreateFileWriter(const std::string& filePath,
                                                           const WriterOption& writerOption) noexcept override;
    FSResult<std::shared_ptr<FileReader>> CreateFileReader(const std::string& filePath,
                                                           const ReaderOption& readerOption) noexcept override;
    /*
       root is fence directory
       create dir root/dirPath will create dir root/pre.__tmp__/dirPath.epochId
       not write to root, but write in a pre dir
       after dir is close, it will rename root/pre.__tmp__/dirPath.epochId to root/dirPath
       attention!: this directory is tempDirectory, need close after finish write
     */
    FSResult<std::shared_ptr<IDirectory>> MakeDirectory(const std::string& dirPath,
                                                        const DirectoryOption& directoryOption) noexcept override;
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

    FSResult<void> Close() noexcept override;

    /*
      this will rename root/pre.__tmp__/filePath.epochId to root/filePath
    */
    FSResult<void> CloseTempPath(const std::string& name) noexcept;

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

    /*
      fence dir remove dir/file will first check worker's epochid is bigger or equal epochid in fence file
      if it's smaller than it, it cannot operate delete action
    */
    FSResult<void> InnerRemoveDirectory(const std::string& dirPath, const RemoveOption& removeOption) noexcept override;
    FSResult<void> InnerRemoveFile(const std::string& filePath, const RemoveOption& removeOption) noexcept override;
    FSResult<void> InnerRename(const std::string& srcPath, const std::shared_ptr<IDirectory>& destDirectory,
                               const std::string& destPath, FenceContext* fenceContext) noexcept override;

private:
    FSResult<void> PreparePreDirectory() noexcept;
    FSResult<void> CheckPreDirectory() noexcept;
    FSResult<void> ClearPreDirectory() noexcept;

private:
    static const std::string PRE_DIRECTORY;
    std::shared_ptr<DirectoryImpl> _directory;
    std::shared_ptr<DirectoryImpl> _preDirectory;
    std::shared_ptr<FenceContext> _fenceContext;
    FileSystemMetricsReporter* _reporter;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FenceDirectory> FenceDirectoryPtr;
} // namespace indexlib::file_system
