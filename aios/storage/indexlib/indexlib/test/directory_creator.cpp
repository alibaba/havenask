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
#include "indexlib/test/directory_creator.h"

#include "autil/StringUtil.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/LocalDirectory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, DirectoryCreator);

DirectoryCreator::DirectoryCreator() {}

DirectoryCreator::~DirectoryCreator() {}
namespace {
std::string GetDirPath(const std::string& basePath, const std::string& dirPath)
{
    if (autil::StringUtil::startsWith(dirPath, basePath)) {
        if (dirPath.size() != basePath.size()) {
            return dirPath.substr(basePath.size() + 1);
        } else {
            return "";
        }
    } else {
        return dirPath;
    }
}
}; // namespace

DirectoryPtr DirectoryCreator::Get(const IFileSystemPtr& fileSystem, const string& rawPath,
                                   bool throwExceptionIfNotExist, bool isAbsPath)
{
    string path = PathUtil::NormalizePath(rawPath);
    std::string relativePath = isAbsPath ? GetDirPath(fileSystem->GetOutputRootPath(), path) : path;
    relativePath = relativePath == "/" ? "" : relativePath;
    if (!fileSystem->IsDir(relativePath).GetOrThrow()) {
        if (throwExceptionIfNotExist) {
            if (fileSystem->IsExist(relativePath).GetOrThrow()) {
                INDEXLIB_FATAL_ERROR(InconsistentState, "Get directory [%s] failed, is file", path.c_str());
            } else {
                INDEXLIB_FATAL_ERROR(NonExist, "Get directory [%s] failed, not exist", path.c_str());
            }
        }
        return DirectoryPtr();
    }
    return CreateDirectory(fileSystem, relativePath, false);
}

DirectoryPtr DirectoryCreator::Create(const std::string& absPath, bool useCache, bool isOffline)
{
    bool ret = false;
    if (FslibWrapper::IsExist(absPath, ret) != FSEC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "IsExist failed: [%s]", absPath.c_str());
    }

    if (!ret) {
        if (FslibWrapper::MkDir(absPath).Code() != FSEC_OK) {
            INDEXLIB_FATAL_ERROR(FileIO, "Make directory failed: [%s]", absPath.c_str());
        }
    }

    FileSystemOptions options;
    options.enableAsyncFlush = false;
    options.useCache = useCache;
    options.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    options.isOffline = isOffline;
    IFileSystemPtr fileSystem = FileSystemCreator::Create(/*name=*/"uninitialized-name",
                                                          /*rootPath=*/PathUtil::NormalizePath(absPath), options,
                                                          std::shared_ptr<util::MetricProvider>(),
                                                          /*isOverride=*/false)
                                    .GetOrThrow();
    return CreateDirectory(fileSystem, absPath, true);
}

DirectoryPtr DirectoryCreator::Create(const IFileSystemPtr& fileSystem, const std::string& path, bool isAbsPath)
{
    return CreateDirectory(fileSystem, path, isAbsPath);
}

DirectoryPtr DirectoryCreator::CreateDirectory(const IFileSystemPtr& fileSystem, const string& path, bool isAbsPath)
{
    if (isAbsPath) {
        std::string dirPath = GetDirPath(fileSystem->GetOutputRootPath(), PathUtil::NormalizePath(path));
        dirPath = (dirPath == "/" ? "" : dirPath);
        THROW_IF_FS_ERROR(fileSystem->MakeDirectory(dirPath, DirectoryOption()), "mkdir [%s] failed", dirPath.c_str());
        return IDirectory::ToLegacyDirectory(std::make_shared<LocalDirectory>(dirPath, fileSystem));
    }
    THROW_IF_FS_ERROR(fileSystem->MakeDirectory(path, DirectoryOption()), "mkdir [%s] failed", path.c_str());
    return IDirectory::ToLegacyDirectory(std::make_shared<LocalDirectory>(path, fileSystem));
}

DirectoryPtr DirectoryCreator::Create(const std::string& absPath, const FileSystemOptions& fsOptions)
{
    bool ret = false;
    if (FslibWrapper::IsExist(absPath, ret) != FSEC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "IsExist failed: [%s]", absPath.c_str());
    }

    if (!ret) {
        if (FslibWrapper::MkDir(absPath).Code() != FSEC_OK) {
            INDEXLIB_FATAL_ERROR(FileIO, "Make directory failed: [%s]", absPath.c_str());
        }
    }

    FileSystemOptions options(fsOptions);
    if (!options.memoryQuotaController) {
        options.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    }
    IFileSystemPtr fileSystem = FileSystemCreator::Create("DirectoryCreator", absPath, options).GetOrThrow();
    return CreateDirectory(fileSystem, absPath, true);
}

DirectoryPtr DirectoryCreator::GetDirectory(const IFileSystemPtr& fileSystem, const string& dirPath)
{
    return IDirectory::ToLegacyDirectory(std::make_shared<LocalDirectory>(dirPath, fileSystem));
}

DirectoryPtr DirectoryCreator::CreateOfflineRootDir(const std::string& rootPath)
{
    FileSystemOptions options;
    options.isOffline = true;
    auto fs = FileSystemCreator::Create("offline", rootPath, options).GetOrThrow();
    return Directory::Get(fs);
}
}} // namespace indexlib::test
