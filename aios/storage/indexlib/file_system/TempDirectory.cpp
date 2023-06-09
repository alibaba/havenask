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
#include "indexlib/file_system/TempDirectory.h"

#include "indexlib/file_system/FileSystemMetricsReporter.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib::file_system {
AUTIL_LOG_SETUP(indexlib.file_system, TempDirectory);

TempDirectory::TempDirectory(std::shared_ptr<IDirectory>& directory, CloseFunc&& closeFunc, const std::string& dirName,
                             FileSystemMetricsReporter* reporter) noexcept
    : _isClosed(false)
    , _directory(directory)
    , _closeFunc(std::move(closeFunc))
    , _dirName(dirName)
    , _reporter(reporter)
{
    if (_reporter) {
        _reporter->ReportTempDirCreate(directory->DebugString());
    }
}

TempDirectory::~TempDirectory()
{
    if (!_isClosed) {
        AUTIL_LOG(ERROR, "temp directory [%s] not CLOSED in destructor. Exception flag [%d]", DebugString("").c_str(),
                  util::IoExceptionController::HasFileIOException());
        assert(util::IoExceptionController::HasFileIOException());
    }
}

FSResult<void> TempDirectory::Close() noexcept
{
    _isClosed = true;
    return _closeFunc(_dirName);
}

FSResult<std::shared_ptr<FileWriter>> TempDirectory::CreateFileWriter(const std::string& filePath,
                                                                      const WriterOption& writerOption) noexcept
{
    if (_isClosed) {
        AUTIL_LOG(ERROR, "directory[%s] has been closed, create file[%s] failed", DebugString("").c_str(),
                  filePath.c_str());
        return {FSEC_ERROR, std::shared_ptr<FileWriter>()};
    }
    RETURN2_IF_FS_ERROR(_directory->RemoveFile(filePath, RemoveOption::MayNonExist()).Code(),
                        std::shared_ptr<FileWriter>(), "Remove [%s] failed", filePath.c_str());
    return _directory->CreateFileWriter(filePath, writerOption);
}

FSResult<std::shared_ptr<FileReader>> TempDirectory::CreateFileReader(const std::string& filePath,
                                                                      const ReaderOption& readerOption) noexcept
{
    return _directory->CreateFileReader(filePath, readerOption);
}

FSResult<std::shared_ptr<IDirectory>> TempDirectory::MakeDirectory(const std::string& dirPath,
                                                                   const DirectoryOption& directoryOption) noexcept
{
    //    _directory->RemoveDirectory(dirPath, true);
    if (_isClosed) {
        AUTIL_LOG(ERROR, "directory[%s] has been closed, mkdir [%s] failed", DebugString("").c_str(), dirPath.c_str());
        return {FSEC_ERROR, std::shared_ptr<IDirectory>()};
    }
    return _directory->MakeDirectory(dirPath, directoryOption);
}

FSResult<std::shared_ptr<IDirectory>> TempDirectory::GetDirectory(const std::string& dirPath) noexcept
{
    return _directory->GetDirectory(dirPath);
}

FSResult<bool> TempDirectory::IsExist(const std::string& path) const noexcept { return _directory->IsExist(path); }

FSResult<bool> TempDirectory::IsDir(const std::string& path) const noexcept { return _directory->IsDir(path); }

FSResult<void> TempDirectory::ListDir(const std::string& path, const ListOption& listOption,
                                      std::vector<std::string>& fileList) const noexcept
{
    return _directory->ListDir(path, listOption, fileList);
}

FSResult<void> TempDirectory::Load(const std::string& fileName, const ReaderOption& readerOption,
                                   std::string& fileContent) noexcept
{
    return _directory->Load(fileName, readerOption, fileContent);
}

FSResult<size_t> TempDirectory::GetFileLength(const std::string& filePath) const noexcept
{
    return _directory->GetFileLength(filePath);
}

std::string TempDirectory::GetOutputPath() const noexcept { return _directory->GetOutputPath(); }

std::string TempDirectory::GetPhysicalPath(const std::string& path) const noexcept
{
    return _directory->GetPhysicalPath(path);
}

std::string TempDirectory::DebugString(const std::string& path) const noexcept { return _directory->DebugString(path); }

FSResult<std::shared_future<bool>> TempDirectory::Sync(bool waitFinish) noexcept
{
    return _directory->Sync(waitFinish);
}

FSResult<size_t> TempDirectory::EstimateFileMemoryUse(const std::string& filePath, FSOpenType type) noexcept
{
    return _directory->EstimateFileMemoryUse(filePath, type);
}

FSResult<FSFileType> TempDirectory::DeduceFileType(const std::string& filePath, FSOpenType type) noexcept
{
    return _directory->DeduceFileType(filePath, type);
}

FSResult<size_t> TempDirectory::EstimateFileMemoryUseChange(const std::string& filePath,
                                                            const std::string& oldTemperature,
                                                            const std::string& newTemperature) noexcept
{
    return _directory->EstimateFileMemoryUseChange(filePath, oldTemperature, newTemperature);
}

FSResult<std::shared_ptr<FileReader>> TempDirectory::DoCreateFileReader(const std::string& filePath,
                                                                        const ReaderOption& readerOption) noexcept
{
    assert(false);
    return {FSEC_NOTSUP, std::shared_ptr<FileReader>()};
}

FSResult<std::shared_ptr<FileWriter>> TempDirectory::DoCreateFileWriter(const std::string& filePath,
                                                                        const WriterOption& writerOption) noexcept
{
    assert(false);
    return {FSEC_NOTSUP, std::shared_ptr<FileWriter>()};
}

const std::string& TempDirectory::GetRootDir() const noexcept { return _directory->GetRootDir(); }

const std::shared_ptr<IFileSystem>& TempDirectory::GetFileSystem() const noexcept
{
    return _directory->GetFileSystem();
}

FSResult<void> TempDirectory::InnerRemoveDirectory(const std::string& dirPath,
                                                   const RemoveOption& removeOption) noexcept
{
    if (_isClosed) {
        AUTIL_LOG(ERROR, "directory[%s] has been closed, rm dir[%s] failed", DebugString("").c_str(), dirPath.c_str());
        return FSEC_ERROR;
    }
    return _directory->RemoveDirectory(dirPath, removeOption);
}

FSResult<void> TempDirectory::InnerRemoveFile(const std::string& filePath, const RemoveOption& removeOption) noexcept
{
    if (_isClosed) {
        AUTIL_LOG(ERROR, "directory[%s] has been closed, rm file[%s] failed", DebugString("").c_str(),
                  filePath.c_str());
        return FSEC_ERROR;
    }
    return _directory->RemoveFile(filePath, removeOption);
}

FSResult<void> TempDirectory::InnerRename(const std::string& srcPath, const std::shared_ptr<IDirectory>& destDirectory,
                                          const std::string& destPath, FenceContext* fenceContext) noexcept
{
    if (_isClosed) {
        AUTIL_LOG(ERROR, "directory[%s] has been closed, rename src[%s] to dest[%s:%s], failed",
                  DebugString("").c_str(), srcPath.c_str(), destDirectory->DebugString("").c_str(), destPath.c_str());
        return FSEC_ERROR;
    }
    return _directory->Rename(srcPath, destDirectory, destPath);
}

std::shared_ptr<FenceContext> TempDirectory::GetFenceContext() const noexcept
{
    return std::shared_ptr<FenceContext>(FenceContext::NoFence());
}

} // namespace indexlib::file_system
