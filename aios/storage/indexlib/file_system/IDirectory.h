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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/WriterOption.h"

namespace indexlib { namespace file_system {

struct FileInfo;
struct WriterOption;
class ArchiveDirectory;
class ArchiveFolder;
class CompressFileInfo;
class Directory;
class IDirectory;
class FileReader;
class FileWriter;
class ResourceFile;
class IFileSystem;
class FileSystemMetricsReporter;

class IDirectory
{
public:
    // static function's impl in DirectoryImpl.cpp
    // ATTENTION: this a ISOLATED file system, may be a bug begin...
    static std::shared_ptr<IDirectory> GetPhysicalDirectory(const std::string& path) noexcept;
    static std::shared_ptr<IDirectory> Get(const std::shared_ptr<IFileSystem>& fs) noexcept;
    static std::shared_ptr<IDirectory> ConstructByFenceContext(const std::shared_ptr<IDirectory>& directory,
                                                               const std::shared_ptr<FenceContext>& fenceContext,
                                                               FileSystemMetricsReporter* reporter) noexcept;
    static std::shared_ptr<Directory> ToLegacyDirectory(std::shared_ptr<IDirectory>) noexcept;

public:
    virtual ~IDirectory() noexcept = default;

public:
    virtual FSResult<std::shared_ptr<FileWriter>> CreateFileWriter(const std::string& filePath,
                                                                   const WriterOption& writerOption) noexcept = 0;
    virtual FSResult<std::shared_ptr<FileReader>> CreateFileReader(const std::string& filePath,
                                                                   const ReaderOption& readerOption) noexcept = 0;
    virtual FSResult<std::shared_ptr<ResourceFile>> CreateResourceFile(const std::string& path) noexcept = 0;
    virtual FSResult<std::shared_ptr<ResourceFile>> GetResourceFile(const std::string& path) noexcept = 0;
    // for write, make directory recursively & ignore exist error
    virtual FSResult<std::shared_ptr<IDirectory>> MakeDirectory(const std::string& dirPath,
                                                                const DirectoryOption& directoryOption) noexcept = 0;
    // for read
    virtual FSResult<std::shared_ptr<IDirectory>> GetDirectory(const std::string& dirPath) noexcept = 0;

    virtual FSResult<void> RemoveDirectory(const std::string& dirPath, const RemoveOption& removeOption) noexcept = 0;
    virtual FSResult<void> RemoveFile(const std::string& filePath, const RemoveOption& removeOption) noexcept = 0;
    // destPath = "" means use same name
    virtual FSResult<void> Rename(const std::string& srcPath, const std::shared_ptr<IDirectory>& destDirectory,
                                  const std::string& destPath) noexcept = 0;
    virtual FSResult<void> Close() noexcept = 0;
    virtual const std::string& GetRootDir() const noexcept = 0;

    virtual FSResult<bool> IsExist(const std::string& path) const noexcept = 0;
    // false for not a dir, may not exist or is a file
    virtual FSResult<bool> IsDir(const std::string& path) const noexcept = 0;
    virtual FSResult<void> ListDir(const std::string& path, const ListOption& listOption,
                                   std::vector<std::string>& fileList) const noexcept = 0;
    virtual FSResult<void> ListPhysicalFile(const std::string& path, const ListOption& listOption,
                                            std::vector<FileInfo>& fileInfos) noexcept = 0;

    virtual FSResult<size_t> GetFileLength(const std::string& filePath) const noexcept = 0;
    virtual FSResult<size_t> GetDirectorySize(const std::string& path) noexcept = 0;

    // readerOption: ReaderOption::PutIntoCache(FSOT_MEM) or ReaderOption(FSOT_MEM)
    virtual FSResult<void> Load(const std::string& fileName, const ReaderOption& readerOption,
                                std::string& fileContent) noexcept = 0;
    virtual FSResult<void> Store(const std::string& fileName, const std::string& fileContent,
                                 const WriterOption& writerOption) noexcept = 0;
    virtual std::string GetOutputPath() const noexcept = 0;
    virtual const std::string& GetLogicalPath() const noexcept = 0;
    virtual std::string GetPhysicalPath(const std::string& path) const noexcept = 0;
    virtual const std::string& GetRootLinkPath() const noexcept = 0; // empty means not use root link

    virtual bool IsExistInCache(const std::string& path) noexcept = 0;
    virtual FSResult<size_t> EstimateFileMemoryUse(const std::string& filePath, FSOpenType type) noexcept = 0;
    virtual FSResult<size_t> EstimateFileMemoryUseChange(const std::string& filePath, const std::string& oldTemperature,
                                                         const std::string& newTemperature) noexcept = 0;
    virtual FSResult<FSFileType> DeduceFileType(const std::string& filePath, FSOpenType type) noexcept = 0;

public:
    virtual FSResult<std::shared_future<bool>> Sync(bool waitFinish) noexcept = 0;
    virtual FSResult<void> MountPackage() noexcept = 0;
    virtual FSResult<void> FlushPackage() noexcept = 0;
    virtual FSResult<void> Validate(const std::string& path) const noexcept = 0;

    virtual const std::shared_ptr<IFileSystem>& GetFileSystem() const noexcept = 0;
    virtual FSResult<FSStorageType> GetStorageType(const std::string& path) const noexcept = 0;
    virtual std::shared_ptr<FenceContext> GetFenceContext() const noexcept = 0;
    virtual FSResult<std::shared_ptr<CompressFileInfo>> GetCompressFileInfo(const std::string& filePath) noexcept = 0;
    virtual std::shared_ptr<IDirectory> CreateLinkDirectory() const noexcept = 0;
    virtual std::shared_ptr<ArchiveFolder> LEGACY_CreateArchiveFolder(bool legacyMode,
                                                                      const std::string& suffix) noexcept = 0;
    virtual FSResult<std::shared_ptr<IDirectory>> CreateArchiveDirectory(const std::string& suffix) noexcept = 0;
    virtual bool SetLifecycle(const std::string& lifecycle) noexcept = 0;

public:
    virtual std::string DebugString(const std::string& path = "") const noexcept = 0;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::file_system
