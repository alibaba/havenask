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
#include "indexlib/file_system/LocalDirectory.h"

#include <assert.h>
#include <memory>

#include "autil/CommonMacros.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/MemDirectory.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, LocalDirectory);

LocalDirectory::LocalDirectory(const std::string& path, const std::shared_ptr<IFileSystem>& fileSystem) noexcept
    : _rootDir(util::PathUtil::NormalizePath(path))
    , _fileSystem(fileSystem)
{
}

LocalDirectory::~LocalDirectory() noexcept {}

FSResult<std::shared_ptr<IDirectory>> LocalDirectory::MakeDirectory(const std::string& dirPath,
                                                                    const DirectoryOption& directoryOption) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, dirPath);
    assert(directoryOption.recursive);
    RETURN2_IF_FS_ERROR(_fileSystem->MakeDirectory(absPath, directoryOption), std::shared_ptr<IDirectory>(),
                        "mkdir [%s] failed", absPath.c_str());
    if (directoryOption.isMem) {
        return {FSEC_OK, std::make_shared<MemDirectory>(absPath, _fileSystem)};
    }
    return {FSEC_OK, std::make_shared<LocalDirectory>(absPath, _fileSystem)};
}

FSResult<std::shared_ptr<IDirectory>> LocalDirectory::GetDirectory(const std::string& dirPath) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, dirPath);
    auto [ec, isDir] = _fileSystem->IsDir(absPath);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<IDirectory>(), "IsDir [%s] failed", absPath.c_str());
    if (isDir) {
        return {FSEC_OK, std::make_shared<LocalDirectory>(absPath, _fileSystem)};
    }
    AUTIL_LOG(DEBUG, "directory [%s] not exist", absPath.c_str());
    return {FSEC_NOENT, std::shared_ptr<IDirectory>()};
}

FSResult<FSFileType> LocalDirectory::DeduceFileType(const std::string& filePath, FSOpenType type) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, filePath);
    return _fileSystem->DeduceFileType(absPath, type);
}

FSResult<size_t> LocalDirectory::EstimateFileMemoryUse(const std::string& filePath, FSOpenType type) noexcept
{
    auto [ec, exist] = IsExist(filePath);
    RETURN2_IF_FS_ERROR(ec, 0, "IsExist [%s] failed", filePath.c_str());
    if (!exist) {
        return {FSEC_OK, 0};
    }
    std::string absPath = util::PathUtil::JoinPath(_rootDir, filePath);
    return _fileSystem->EstimateFileLockMemoryUse(absPath, type);
}

FSResult<size_t> LocalDirectory::EstimateFileMemoryUseChange(const std::string& filePath,
                                                             const std::string& oldTemperature,
                                                             const std::string& newTemperature) noexcept
{
    auto [ec, exist] = IsExist(filePath);
    RETURN2_IF_FS_ERROR(ec, 0, "IsExist [%s] failed", filePath.c_str());
    if (!exist) {
        return {FSEC_OK, 0};
    }
    std::string absPath = util::PathUtil::JoinPath(_rootDir, filePath);
    return _fileSystem->EstimateFileMemoryUseChange(absPath, oldTemperature, newTemperature);
}

const std::shared_ptr<IFileSystem>& LocalDirectory::GetFileSystem() const noexcept { return _fileSystem; }

const std::string& LocalDirectory::GetRootDir() const noexcept { return _rootDir; }

std::shared_ptr<FenceContext> LocalDirectory::GetFenceContext() const noexcept
{
    return std::shared_ptr<FenceContext>(FenceContext::NoFence());
}

FSResult<void> LocalDirectory::Close() noexcept { return FSEC_OK; }

FSResult<std::shared_ptr<FileReader>> LocalDirectory::DoCreateFileReader(const std::string& filePath,
                                                                         const ReaderOption& readerOption) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, filePath);
    return _fileSystem->CreateFileReader(absPath, readerOption);
}

FSResult<std::shared_ptr<FileWriter>> LocalDirectory::DoCreateFileWriter(const std::string& filePath,
                                                                         const WriterOption& writerOption) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, filePath);
    return _fileSystem->CreateFileWriter(absPath, writerOption);
}

FSResult<void> LocalDirectory::InnerRemoveDirectory(const std::string& dirPath,
                                                    const RemoveOption& removeOption) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, dirPath);
    return _fileSystem->RemoveDirectory(absPath, removeOption);
}

FSResult<void> LocalDirectory::InnerRemoveFile(const std::string& filePath, const RemoveOption& removeOption) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(_rootDir, filePath);
    return _fileSystem->RemoveFile(absPath, removeOption);
}

FSResult<void> LocalDirectory::InnerRename(const std::string& srcPath, const std::shared_ptr<IDirectory>& destDirectory,
                                           const std::string& destPath, FenceContext* fenceContext) noexcept
{
    std::string absSrcPath = util::PathUtil::JoinPath(_rootDir, srcPath);
    std::string absDestPath =
        util::PathUtil::JoinPath(destDirectory->GetLogicalPath(), (destPath.empty() ? srcPath : destPath));

    if (unlikely(_fileSystem != destDirectory->GetFileSystem())) {
        AUTIL_LOG(ERROR, "only support rename in the same fs, not support from [%s] [%s] to [%s] [%s]",
                  _fileSystem->DebugString().c_str(), DebugString("").c_str(),
                  destDirectory->GetFileSystem()->DebugString().c_str(), destDirectory->DebugString().c_str());
        return FSEC_NOTSUP;
    }

    if (!std::dynamic_pointer_cast<LocalDirectory>(destDirectory)) {
        assert(false);
        AUTIL_LOG(ERROR, "Only support LocalDirectory, rename from [%s] to [%s]", absSrcPath.c_str(),
                  absDestPath.c_str());
        return FSEC_NOTSUP;
    }
    return _fileSystem->Rename(absSrcPath, absDestPath, fenceContext);
}

}} // namespace indexlib::file_system
