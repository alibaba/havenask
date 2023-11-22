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
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/fslib/FenceContext.h"

namespace indexlib { namespace file_system {

struct FileInfo;
class ArchiveFolder;
class CompressFileInfo;
class IDirectory;
class FileReader;
class FileWriter;
class ResourceFile;
class IFileSystem;
class FileSystemMetricsReporter;

using FileList = std::vector<std::string>;

class Directory
{
public:
    // ATTENTION: this a ISOLATED file system, may be a bug begin...
    static std::shared_ptr<Directory> GetPhysicalDirectory(const std::string& path);
    static std::shared_ptr<Directory> Get(const std::shared_ptr<IFileSystem>& fs);
    static std::shared_ptr<Directory> ConstructByFenceContext(const std::shared_ptr<Directory>& directory,
                                                              const std::shared_ptr<FenceContext>& fenceContext,
                                                              FileSystemMetricsReporter* reporter);

public:
    Directory();
    Directory(const std::shared_ptr<IDirectory>& directory);
    ~Directory();

public:
    std::shared_ptr<FileWriter> CreateFileWriter(const std::string& filePath,
                                                 const WriterOption& writerOption = WriterOption());
    std::shared_ptr<FileReader> CreateFileReader(const std::string& filePath, const ReaderOption& readerOption);
    // for write, make directory recursively & ignore exist error
    std::shared_ptr<Directory> MakeDirectory(const std::string& dirPath,
                                             const DirectoryOption& directoryOption = DirectoryOption());
    // for read
    std::shared_ptr<Directory> GetDirectory(const std::string& dirPath, bool throwExceptionIfNotExist);

    void RemoveDirectory(const std::string& dirPath, const RemoveOption& removeOption = RemoveOption());
    void RemoveFile(const std::string& filePath, const RemoveOption& removeOption = RemoveOption());
    void Rename(const std::string& srcPath, const std::shared_ptr<Directory>& destDirectory,
                const std::string& destPath = "");
    size_t EstimateFileMemoryUse(const std::string& filePath, FSOpenType type);
    size_t EstimateFileMemoryUseChange(const std::string& filePath, const std::string& oldTemperature,
                                       const std::string& newTemperature);
    FSFileType DeduceFileType(const std::string& filePath, FSOpenType type);
    void Close();
    const std::string& GetRootDir() const;

public:
    bool IsExist(const std::string& path);
    bool IsDir(const std::string& path); // false for not a dir, may not exist or file
    void ListDir(const std::string& path, FileList& fileList, bool recursive = false) const;
    void Load(const std::string& fileName, std::string& fileContent, bool insertToCacheIfNotExist = false);
    void Store(const std::string& fileName, const std::string& fileContent,
               const WriterOption& writerOption = WriterOption());
    size_t GetFileLength(const std::string& filePath) const;
    size_t GetDirectorySize(const std::string& path);
    std::string GetOutputPath() const;
    std::string GetPhysicalPath(const std::string& path) const;
    std::string DebugString(const std::string& path = "") const noexcept; // for log

    std::shared_future<bool> Sync(bool waitFinish);

public:
    const std::shared_ptr<IFileSystem>& GetFileSystem() const;
    std::shared_ptr<FenceContext> GetFenceContext() const;
    const std::string& GetLogicalPath() const;
    void MountPackage();
    void FlushPackage();
    ErrorCode Validate(const std::string& path) const;

    std::shared_ptr<CompressFileInfo> GetCompressFileInfo(const std::string& filePath);

    std::shared_ptr<ResourceFile> CreateResourceFile(const std::string& path);
    std::shared_ptr<ResourceFile> GetResourceFile(const std::string& path);

    const std::string& GetRootLinkPath() const; // empty means not use root link
    std::shared_ptr<Directory> CreateLinkDirectory() const;
    std::shared_ptr<ArchiveFolder> CreateArchiveFolder(bool legacyMode, const std::string& suffix);

public:
    bool LoadMayNonExist(const std::string& fileName, std::string& fileContent, bool cacheFileNode = false);
    void ListPhysicalFile(const std::string& path, std::vector<FileInfo>& fileInfos, bool recursive = false);
    bool IsExistInCache(const std::string& path);
    FSStorageType GetStorageType(const std::string& path) const;
    bool SetLifecycle(const std::string& lifecycle);
    std::shared_ptr<IDirectory> GetIDirectory() const;

private:
    std::shared_ptr<IDirectory> _directory;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Directory> DirectoryPtr;
typedef std::vector<std::shared_ptr<Directory>> DirectoryVector;

}} // namespace indexlib::file_system
