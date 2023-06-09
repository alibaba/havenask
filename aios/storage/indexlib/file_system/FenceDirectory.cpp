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
#include "indexlib/file_system/FenceDirectory.h"

#include "indexlib/file_system/FileSystemMetricsReporter.h"
#include "indexlib/file_system/TempDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/TempFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib::file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FenceDirectory);

const std::string FenceDirectory::PRE_DIRECTORY = string("pre") + TEMP_FILE_SUFFIX;

FenceDirectory::FenceDirectory(const std::shared_ptr<IDirectory>& rootDirectory,
                               const std::shared_ptr<FenceContext>& fenceContext, FileSystemMetricsReporter* reporter)
    : _directory(std::dynamic_pointer_cast<DirectoryImpl>(rootDirectory))
    , _fenceContext(fenceContext)
    , _reporter(reporter)
{
    assert(_fenceContext);
    assert(_directory);
}

FenceDirectory::~FenceDirectory() {}

FSResult<void> FenceDirectory::Close() noexcept
{
    RETURN_IF_FS_ERROR(ClearPreDirectory().Code(), "fence directory [%s] remove pre dir [%s] failed",
                       DebugString("").c_str(), _preDirectory->DebugString("").c_str());
    return FSEC_OK;
}

FSResult<void> FenceDirectory::ClearPreDirectory() noexcept
{
    if (_preDirectory == nullptr) {
        return FSEC_OK;
    }
    RETURN_IF_FS_ERROR(CheckPreDirectory().Code(), "check pre directory [%s] failed",
                       _preDirectory->DebugString("").c_str());
    RemoveOption removeOption;
    removeOption.mayNonExist = true;
    removeOption.fenceContext = _fenceContext.get();
    RETURN_IF_FS_ERROR(_directory->InnerRemoveDirectory(PRE_DIRECTORY, removeOption).Code(),
                       "InnerRemoveDirectory [%s] faield", PRE_DIRECTORY.c_str());
    return FSEC_OK;
}

FSResult<void> FenceDirectory::CheckPreDirectory() noexcept
{
    FileList fileList;
    RETURN_IF_FS_ERROR(_preDirectory->ListDir("", ListOption::Recursive(true), fileList).Code(), "ListDir failed");
    for (auto file : fileList) {
        auto [ec, isDir] = _preDirectory->IsDir(file);
        RETURN_IF_FS_ERROR(ec, "IsDir [%s] failed", file.c_str());
        if (!isDir && file.find(_fenceContext->epochId) != string::npos) {
            AUTIL_LOG(ERROR, "file [%s] is in pre directory [%s] not move before fence dir [%s] clean", file.c_str(),
                      _preDirectory->DebugString("").c_str(), _directory->DebugString("").c_str());
            return FSEC_ERROR;
        }
    }
    return FSEC_OK;
}

FSResult<void> FenceDirectory::PreparePreDirectory() noexcept
{
    auto [ec, directory] = _directory->MakeDirectory(PRE_DIRECTORY, DirectoryOption());
    RETURN_IF_FS_ERROR(ec, "MakeDirectory [%s] failed", PRE_DIRECTORY.c_str());
    _preDirectory = std::dynamic_pointer_cast<DirectoryImpl>(directory);
    assert(_preDirectory);
    // only report for write fence dir
    if (_reporter) {
        _reporter->ReportFenceDirCreate(_directory->DebugString(""));
    }
    return FSEC_OK;
}

FSResult<std::shared_ptr<FileWriter>> FenceDirectory::CreateFileWriter(const std::string& fileName,
                                                                       const WriterOption& writerOption) noexcept
{
    if (writerOption.openType == FSOT_SLICE) {
        return _directory->CreateFileWriter(fileName, writerOption);
    }

    if (_preDirectory == nullptr) {
        RETURN2_IF_FS_ERROR(PreparePreDirectory().Code(), std::shared_ptr<FileWriter>(), "PreparePreDirectory failed");
    }

    auto [ec, file] = _preDirectory->CreateFileWriter(fileName + "." + _fenceContext->epochId, writerOption);
    RETURN2_IF_FS_ERROR(ec, file, "CreateFileWriter failed");
    auto closeFunc = [this](const std::string& fileName) mutable noexcept { return this->CloseTempPath(fileName); };
    return {FSEC_OK, std::make_shared<TempFileWriter>(file, move(closeFunc))};
}

FSResult<std::shared_ptr<FileReader>> FenceDirectory::CreateFileReader(const std::string& filePath,
                                                                       const ReaderOption& readerOption) noexcept
{
    return _directory->CreateFileReader(filePath, readerOption);
}

FSResult<std::shared_ptr<IDirectory>> FenceDirectory::MakeDirectory(const std::string& dirPath,
                                                                    const DirectoryOption& directoryOption) noexcept
{
    auto [ec, directory] = _directory->GetDirectory(dirPath);
    if (ec != FSEC_NOENT) {
        RETURN2_IF_FS_ERROR(ec, directory, "GetDirectory [%s] failed", dirPath.c_str());
    }
    if (directory) {
        assert(_fenceContext);
        AUTIL_LOG(WARN, "directory [%s] is exist, create fence dir", dirPath.c_str());

        return {FSEC_OK, std::make_shared<FenceDirectory>(directory, _fenceContext, _reporter)};
    }

    if (_preDirectory == nullptr) {
        RETURN2_IF_FS_ERROR(PreparePreDirectory().Code(), std::shared_ptr<IDirectory>(), "PreparePreDirectory failed");
    }

    // tmp/xxx.epoch
    string dir = dirPath + "." + _fenceContext->epochId;
    auto [ec2, directory2] = _preDirectory->MakeDirectory(dir, directoryOption);
    RETURN2_IF_FS_ERROR(ec2, directory2, "MakeDirectory [%s] failed", dir.c_str());
    if (!directory2) {
        assert(false);
        return {FSEC_ERROR, std::shared_ptr<IDirectory>()};
    }
    auto closeFunc = [this](const std::string& dirName) mutable noexcept -> FSResult<void> {
        return this->CloseTempPath(dirName);
    };
    return {FSEC_OK, std::make_shared<TempDirectory>(directory2, std::move(closeFunc), dir, _reporter)};
}

FSResult<bool> FenceDirectory::IsExist(const std::string& path) const noexcept { return _directory->IsExist(path); }

FSResult<bool> FenceDirectory::IsDir(const std::string& path) const noexcept { return _directory->IsDir(path); }

FSResult<void> FenceDirectory::ListDir(const std::string& path, const ListOption& listOptions,
                                       std::vector<std::string>& fileList) const noexcept
{
    return _directory->ListDir(path, listOptions, fileList);
}

FSResult<void> FenceDirectory::Load(const std::string& fileName, const ReaderOption& readerOption,
                                    std::string& fileContent) noexcept
{
    return _directory->Load(fileName, readerOption, fileContent);
}

FSResult<size_t> FenceDirectory::GetFileLength(const std::string& filePath) const noexcept
{
    return _directory->GetFileLength(filePath);
}

std::string FenceDirectory::GetOutputPath() const noexcept { return _directory->GetOutputPath(); }

std::string FenceDirectory::GetPhysicalPath(const std::string& path) const noexcept
{
    return _directory->GetPhysicalPath(path);
}

std::string FenceDirectory::DebugString(const std::string& path) const noexcept
{
    return _directory->DebugString(path);
}

FSResult<std::shared_future<bool>> FenceDirectory::Sync(bool waitFinish) noexcept
{
    return _directory->Sync(waitFinish);
}

// for read
FSResult<std::shared_ptr<IDirectory>> FenceDirectory::GetDirectory(const std::string& dirPath) noexcept
{
    auto [ec, directory] = _directory->GetDirectory(dirPath);
    RETURN2_IF_FS_ERROR(ec, directory, "GetDirectory [%s] failed", dirPath.c_str());
    if (directory) {
        return {FSEC_OK, std::make_shared<FenceDirectory>(directory, _fenceContext, _reporter)};
    }
    return {FSEC_OK, std::shared_ptr<IDirectory>()};
}

FSResult<size_t> FenceDirectory::EstimateFileMemoryUse(const std::string& filePath, FSOpenType type) noexcept
{
    return _directory->EstimateFileMemoryUse(filePath, type);
}

FSResult<FSFileType> FenceDirectory::DeduceFileType(const std::string& filePath, FSOpenType type) noexcept
{
    return _directory->DeduceFileType(filePath, type);
}

FSResult<size_t> FenceDirectory::EstimateFileMemoryUseChange(const std::string& filePath,
                                                             const std::string& oldTemperature,
                                                             const std::string& newTemperature) noexcept
{
    return _directory->EstimateFileMemoryUseChange(filePath, oldTemperature, newTemperature);
}

FSResult<void> FenceDirectory::InnerRemoveDirectory(const std::string& dirPath,
                                                    const RemoveOption& removeOption) noexcept
{
    RemoveOption newRemoveOption(removeOption);
    newRemoveOption.fenceContext = _fenceContext.get();
    return _directory->InnerRemoveDirectory(dirPath, newRemoveOption);
}

FSResult<void> FenceDirectory::InnerRemoveFile(const std::string& filePath, const RemoveOption& removeOption) noexcept
{
    RemoveOption newRemoveOption(removeOption);
    newRemoveOption.fenceContext = _fenceContext.get();
    return _directory->InnerRemoveFile(filePath, newRemoveOption);
}

FSResult<void> FenceDirectory::InnerRename(const std::string& srcPath, const std::shared_ptr<IDirectory>& destDirectory,
                                           const std::string& destPath, FenceContext*) noexcept
{
    AUTIL_LOG(ERROR, "fence dirtory not support rename action, srcPath [%s], destPath [%s]", srcPath.c_str(),
              destPath.c_str());
    return FSEC_NOTSUP;
}

FSResult<void> FenceDirectory::CloseTempPath(const std::string& name) noexcept
{
    // root/pre/xxx.epochId -> root/xxx
    string destName = name;
    autil::StringUtil::replaceLast(destName, "." + _fenceContext->epochId, "");
    return _preDirectory->InnerRename(name, _directory, destName, _fenceContext.get());
}

FSResult<std::shared_ptr<FileReader>> FenceDirectory::DoCreateFileReader(const std::string& filePath,
                                                                         const ReaderOption& readerOption) noexcept
{
    assert(false);
    return {FSEC_NOTSUP, std::shared_ptr<FileReader>()};
}
FSResult<std::shared_ptr<FileWriter>> FenceDirectory::DoCreateFileWriter(const std::string& filePath,
                                                                         const WriterOption& writerOption) noexcept
{
    assert(false);
    return {FSEC_NOTSUP, std::shared_ptr<FileWriter>()};
}

const std::shared_ptr<IFileSystem>& FenceDirectory::GetFileSystem() const noexcept
{
    return _directory->GetFileSystem();
}

std::shared_ptr<FenceContext> FenceDirectory::GetFenceContext() const noexcept { return _fenceContext; }

const std::string& FenceDirectory::GetRootDir() const noexcept { return _directory->GetRootDir(); }

} // namespace indexlib::file_system
