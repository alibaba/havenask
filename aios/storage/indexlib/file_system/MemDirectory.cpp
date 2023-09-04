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
#include "indexlib/file_system/MemDirectory.h"

#include <assert.h>
#include <memory>

#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/LinkDirectory.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, MemDirectory);

MemDirectory::MemDirectory(const std::string& path, const std::shared_ptr<IFileSystem>& fileSystem) noexcept
    : _rootDir(util::PathUtil::NormalizePath(path))
    , _fileSystem(fileSystem)
{
}

MemDirectory::~MemDirectory() noexcept {}

FSResult<std::shared_ptr<IDirectory>> MemDirectory::MakeDirectory(const std::string& dirPath,
                                                                  const DirectoryOption& directoryOption) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, dirPath);
    DirectoryOption option(directoryOption);
    assert(directoryOption.recursive);
    if (directoryOption.isMem) {
        option.packageHint = false;
    }
    RETURN2_IF_FS_ERROR(_fileSystem->MakeDirectory(absPath, option), std::shared_ptr<IDirectory>(), "mkdir [%s] failed",
                        absPath.c_str());
    return {FSEC_OK, std::make_shared<MemDirectory>(absPath, _fileSystem)};
}

FSResult<std::shared_ptr<IDirectory>> MemDirectory::GetDirectory(const std::string& dirPath) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, dirPath);
    auto [ec, isDir] = _fileSystem->IsDir(absPath);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<IDirectory>(), "IsDir [%s] failed", absPath.c_str());
    if (isDir) {
        return {FSEC_OK, std::make_shared<MemDirectory>(absPath, _fileSystem)};
    }
    AUTIL_LOG(DEBUG, "directory [%s] not exist", absPath.c_str());
    return {FSEC_NOENT, std::shared_ptr<IDirectory>()};
}

FSResult<FSFileType> MemDirectory::DeduceFileType(const std::string& filePath, FSOpenType openType) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, filePath);
    FSOpenType newOpenType = openType == FSOT_SLICE ? FSOT_SLICE : FSOT_MEM;
    return _fileSystem->DeduceFileType(absPath, newOpenType);
}

FSResult<size_t> MemDirectory::EstimateFileMemoryUse(const std::string& filePath, FSOpenType openType) noexcept
{
    auto [ec, exist] = IsExist(filePath);
    RETURN2_IF_FS_ERROR(ec, 0, "IsExist [%s] failed", filePath.c_str());
    if (!exist) {
        return {FSEC_OK, 0};
    }
    std::string absPath = util::PathUtil::JoinPath(_rootDir, filePath);
    FSOpenType newOpenType = openType == FSOT_SLICE ? FSOT_SLICE : FSOT_MEM;
    return _fileSystem->EstimateFileLockMemoryUse(absPath, newOpenType);
}

FSResult<size_t> MemDirectory::EstimateFileMemoryUseChange(const std::string& filePath,
                                                           const std::string& oldTemperature,
                                                           const std::string& newTemperature) noexcept
{
    AUTIL_LOG(ERROR, "MemDirectory not supports EstimateFileMemoryUseChange");
    assert(false);
    return {FSEC_NOTSUP, 0};
}

const std::shared_ptr<IFileSystem>& MemDirectory::GetFileSystem() const noexcept { return _fileSystem; }

const std::string& MemDirectory::GetRootDir() const noexcept { return _rootDir; }

std::shared_ptr<FenceContext> MemDirectory::GetFenceContext() const noexcept
{
    return std::shared_ptr<FenceContext>(FenceContext::NoFence());
}

FSResult<void> MemDirectory::Close() noexcept { return FSEC_OK; }

FSResult<std::shared_ptr<FileReader>> MemDirectory::DoCreateFileReader(const std::string& filePath,
                                                                       const ReaderOption& readerOption) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, filePath);
    ReaderOption newReaderOption(readerOption);
    if (readerOption.openType != FSOT_SLICE) {
        newReaderOption.openType = FSOT_MEM;
    }
    return _fileSystem->CreateFileReader(absPath, newReaderOption);
}

FSResult<std::shared_ptr<FileWriter>> MemDirectory::DoCreateFileWriter(const std::string& filePath,
                                                                       const WriterOption& writerOption) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, filePath);
    return _fileSystem->CreateFileWriter(absPath, writerOption);
}

FSResult<void> MemDirectory::InnerRemoveDirectory(const std::string& dirPath, const RemoveOption& removeOption) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, dirPath);

    auto linkDirectory = CreateLinkDirectory();
    if (!linkDirectory) {
        return _fileSystem->RemoveDirectory(absPath, removeOption);
    }
    auto [ec, exist] = linkDirectory->IsExist(dirPath);
    RETURN_IF_FS_ERROR(ec, "IsExist [%s] failed", dirPath.c_str());
    if (exist) {
        AUTIL_LOG(INFO, "Remove directory [%s] in link path [%s]", dirPath.c_str(),
                  linkDirectory->DebugString("").c_str());
        auto ec = linkDirectory->RemoveDirectory(dirPath, removeOption).Code();
        RETURN_IF_FS_ERROR(ec, "RemoveDirectory [%s] failed", dirPath.c_str());
        RemoveOption newRemoveOption(removeOption);
        newRemoveOption.mayNonExist = true;
        return _fileSystem->RemoveDirectory(absPath, newRemoveOption);
    }

    return _fileSystem->RemoveDirectory(absPath, removeOption);
}

FSResult<void> MemDirectory::InnerRemoveFile(const std::string& filePath, const RemoveOption& removeOption) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, filePath);

    auto linkDirectory = CreateLinkDirectory();
    if (!linkDirectory) {
        return _fileSystem->RemoveFile(absPath, removeOption);
    }
    auto [ec, exist] = linkDirectory->IsExist(filePath);
    RETURN_IF_FS_ERROR(ec, "IsExist [%s] failed", filePath.c_str());
    if (exist) {
        AUTIL_LOG(INFO, "Remove file [%s] in link path [%s]", filePath.c_str(), linkDirectory->DebugString("").c_str());
        auto ec = linkDirectory->RemoveFile(filePath, RemoveOption()).Code();
        RETURN_IF_FS_ERROR(ec, "RemoveDirectory [%s] failed", filePath.c_str());
        RemoveOption newRemoveOption(removeOption);
        newRemoveOption.mayNonExist = true;
        return _fileSystem->RemoveFile(absPath, newRemoveOption);
    }

    return _fileSystem->RemoveFile(absPath, removeOption);
}

FSResult<void> MemDirectory::InnerRename(const std::string& srcPath, const std::shared_ptr<IDirectory>& destDirectory,
                                         const std::string& destPath, FenceContext*) noexcept
{
    AUTIL_LOG(ERROR, "Only support LocalDirectory, rename from [%s] to [%s]", DebugString(srcPath).c_str(),
              destDirectory->DebugString(destPath).c_str());
    assert(false);
    return FSEC_NOTSUP;
}

}} // namespace indexlib::file_system
