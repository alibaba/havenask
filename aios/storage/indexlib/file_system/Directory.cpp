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
#include "indexlib/file_system/Directory.h"

#include <assert.h>

#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, Directory);

// ATTENTION: this a ISOLATED file system, may be a bug begin...
std::shared_ptr<Directory> Directory::GetPhysicalDirectory(const std::string& path)
{
    return std::make_shared<Directory>(IDirectory::GetPhysicalDirectory(path));
}

std::shared_ptr<Directory> Directory::Get(const std::shared_ptr<IFileSystem>& fs)
{
    return std::make_shared<Directory>(IDirectory::Get(fs));
}

std::shared_ptr<Directory> Directory::ConstructByFenceContext(const std::shared_ptr<Directory>& directory,
                                                              const std::shared_ptr<FenceContext>& fenceContext,
                                                              FileSystemMetricsReporter* reporter)
{
    return std::make_shared<Directory>(
        IDirectory::ConstructByFenceContext(directory->GetIDirectory(), fenceContext, reporter));
}

Directory::Directory() {}
Directory::Directory(const std::shared_ptr<IDirectory>& directory) : _directory(directory) { assert(_directory); }
Directory::~Directory() {}

std::shared_ptr<FileWriter> Directory::CreateFileWriter(const std::string& filePath, const WriterOption& writerOption)
{
    return _directory->CreateFileWriter(filePath, writerOption).GetOrThrow(filePath);
}

std::shared_ptr<FileReader> Directory::CreateFileReader(const std::string& filePath, const ReaderOption& readerOption)
{
    return _directory->CreateFileReader(filePath, readerOption).GetOrThrow(filePath);
}

std::shared_ptr<Directory> Directory::MakeDirectory(const std::string& dirPath, const DirectoryOption& directoryOption)
{
    return std::make_shared<Directory>(_directory->MakeDirectory(dirPath, directoryOption).GetOrThrow(dirPath));
}

std::shared_ptr<Directory> Directory::GetDirectory(const std::string& dirPath, bool throwExceptionIfNotExist)
{
    auto [ec, directory] = _directory->GetDirectory(dirPath);
    if (ec == FSEC_NOENT && !throwExceptionIfNotExist) {
        return std::shared_ptr<Directory>();
    }
    THROW_IF_FS_ERROR(ec, "GetDirectory [%s] failed", DebugString(dirPath).c_str());
    return directory ? std::make_shared<Directory>(directory) : std::shared_ptr<Directory>();
}

void Directory::RemoveDirectory(const std::string& dirPath, const RemoveOption& removeOption)
{
    return _directory->RemoveDirectory(dirPath, removeOption).GetOrThrow(dirPath);
}

void Directory::RemoveFile(const std::string& filePath, const RemoveOption& removeOption)
{
    return _directory->RemoveFile(filePath, removeOption).GetOrThrow(filePath);
}

void Directory::Rename(const std::string& srcPath, const std::shared_ptr<Directory>& destDirectory,
                       const std::string& destPath)
{
    return _directory->Rename(srcPath, destDirectory->GetIDirectory(), destPath).GetOrThrow(srcPath);
}

size_t Directory::EstimateFileMemoryUse(const std::string& filePath, FSOpenType type)
{
    return _directory->EstimateFileMemoryUse(filePath, type).GetOrThrow(filePath);
}

size_t Directory::EstimateFileMemoryUseChange(const std::string& filePath, const std::string& oldTemperature,
                                              const std::string& newTemperature)
{
    return _directory->EstimateFileMemoryUseChange(filePath, oldTemperature, newTemperature).GetOrThrow(filePath);
}

FSFileType Directory::DeduceFileType(const std::string& filePath, FSOpenType type)
{
    return _directory->DeduceFileType(filePath, type).GetOrThrow(filePath);
}

void Directory::Close() { return _directory->Close().GetOrThrow(); }

const std::string& Directory::GetRootDir() const { return _directory->GetRootDir(); }

bool Directory::IsExist(const std::string& path) { return _directory->IsExist(path).GetOrThrow(path); }

bool Directory::IsDir(const std::string& path) { return _directory->IsDir(path).GetOrThrow(path); }

void Directory::ListDir(const std::string& path, FileList& fileList, bool recursive) const
{
    return _directory->ListDir(path, ListOption::Recursive(recursive), fileList).GetOrThrow(path);
}
void Directory::Load(const std::string& fileName, std::string& fileContent, bool insertToCacheIfNotExist)
{
    auto readerOption = insertToCacheIfNotExist ? ReaderOption::PutIntoCache(FSOT_MEM) : ReaderOption(FSOT_MEM);
    return _directory->Load(fileName, readerOption, fileContent).GetOrThrow(fileName);
}

void Directory::Store(const std::string& fileName, const std::string& fileContent, const WriterOption& writerOption)
{
    return _directory->Store(fileName, fileContent, writerOption).GetOrThrow(fileName);
}

size_t Directory::GetFileLength(const std::string& filePath) const
{
    return _directory->GetFileLength(filePath).GetOrThrow(filePath);
}

size_t Directory::GetDirectorySize(const std::string& path)
{
    return _directory->GetDirectorySize(path).GetOrThrow(path);
}

std::string Directory::GetOutputPath() const { return _directory->GetOutputPath(); }
std::string Directory::GetPhysicalPath(const std::string& path) const { return _directory->GetPhysicalPath(path); }
std::string Directory::DebugString(const std::string& path) const noexcept { return _directory->DebugString(path); }

std::shared_future<bool> Directory::Sync(bool waitFinish)
{
    auto [ec, f] = _directory->Sync(waitFinish);
    THROW_IF_FS_ERROR(ec, "Sync [%s] failed, waitFinish[%d]", DebugString("").c_str(), waitFinish);
    return f;
}

const std::shared_ptr<IFileSystem>& Directory::GetFileSystem() const { return _directory->GetFileSystem(); }

std::shared_ptr<FenceContext> Directory::GetFenceContext() const { return _directory->GetFenceContext(); }
const std::string& Directory::GetLogicalPath() const { return _directory->GetLogicalPath(); }

void Directory::MountPackage() { return _directory->MountPackage().GetOrThrow(); }
void Directory::FlushPackage() { return _directory->FlushPackage().GetOrThrow(); }

ErrorCode Directory::Validate(const std::string& path) const { return _directory->Validate(path).Code(); }

std::shared_ptr<CompressFileInfo> Directory::GetCompressFileInfo(const std::string& filePath)
{
    return _directory->GetCompressFileInfo(filePath).GetOrThrow(filePath);
}

std::shared_ptr<ResourceFile> Directory::CreateResourceFile(const std::string& path)
{
    return _directory->CreateResourceFile(path).GetOrThrow(path);
}
std::shared_ptr<ResourceFile> Directory::GetResourceFile(const std::string& path)
{
    return _directory->GetResourceFile(path).GetOrThrow(path);
}

const std::string& Directory::GetRootLinkPath() const { return _directory->GetRootLinkPath(); }

std::shared_ptr<Directory> Directory::CreateLinkDirectory() const
{
    auto directory = _directory->CreateLinkDirectory();
    return directory ? std::make_shared<Directory>(directory) : std::shared_ptr<Directory>();
}

std::shared_ptr<ArchiveFolder> Directory::CreateArchiveFolder(bool legacyMode, const std::string& suffix)
{
    return _directory->LEGACY_CreateArchiveFolder(legacyMode, suffix);
}

bool Directory::LoadMayNonExist(const std::string& fileName, std::string& fileContent, bool cacheFileNode)
{
    auto readerOption = cacheFileNode ? ReaderOption::PutIntoCache(FSOT_MEM) : ReaderOption(FSOT_MEM);
    auto ec = _directory->Load(fileName, readerOption, fileContent).Code();
    if (ec == FSEC_NOENT) {
        return false;
    }
    THROW_IF_FS_ERROR(ec, "Load [%s] failed.", DebugString(fileName).c_str());
    return true;
};
void Directory::ListPhysicalFile(const std::string& path, std::vector<FileInfo>& fileInfos, bool recursive)
{
    return _directory->ListPhysicalFile(path, ListOption::Recursive(recursive), fileInfos).GetOrThrow(path);
}
bool Directory::IsExistInCache(const std::string& path) { return _directory->IsExistInCache(path); }

FSStorageType Directory::GetStorageType(const std::string& path) const
{
    return _directory->GetStorageType(path).GetOrThrow(path);
}

bool Directory::SetLifecycle(const std::string& lifecycle) { return _directory->SetLifecycle(lifecycle); }

std::shared_ptr<IDirectory> Directory::GetIDirectory() const { return _directory; }
}} // namespace indexlib::file_system
