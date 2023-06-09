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
#include "indexlib/file_system/PhysicalDirectory.h"

#include <assert.h>
#include <memory>

#include "alog/Logger.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/BufferedFileNode.h"
#include "indexlib/file_system/file/BufferedFileOutputStream.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/NormalFileReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, PhysicalDirectory);

PhysicalDirectory::PhysicalDirectory(const std::string& path) noexcept : _rootDir(util::PathUtil::NormalizePath(path))
{
}

std::string PhysicalDirectory::AbsPath(const std::string& relativePath) const
{
    return util::PathUtil::NormalizePath(util::PathUtil::JoinPath(_rootDir, relativePath));
}

FSResult<std::shared_ptr<IDirectory>> PhysicalDirectory::MakeDirectory(const std::string& dirPath,
                                                                       const DirectoryOption& directoryOption) noexcept
{
    std::string absPath = AbsPath(dirPath);
    bool packageHint = directoryOption.isMem ? false : directoryOption.packageHint;
    if (unlikely(packageHint)) {
        AUTIL_LOG(ERROR, "not support make packageHint=[%d] dir [%s]", packageHint, absPath.c_str());
        return {FSEC_NOTSUP, std::shared_ptr<IDirectory>()};
    }
    RETURN2_IF_FS_ERROR(FslibWrapper::MkDir(absPath, true, true).Code(), std::shared_ptr<IDirectory>(),
                        "Make directory [%s] failed", absPath.c_str());
    return {FSEC_OK, std::make_shared<PhysicalDirectory>(absPath)};
}

FSResult<std::shared_ptr<IDirectory>> PhysicalDirectory::GetDirectory(const std::string& dirPath) noexcept
{
    std::string absPath = AbsPath(dirPath);
    auto [ec, isDir] = FslibWrapper::IsDir(absPath);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<IDirectory>(), "IsDir [%s] failed", absPath.c_str());
    if (isDir) {
        return {FSEC_OK, std::make_shared<PhysicalDirectory>(absPath)};
    }
    return {FSEC_NOENT, std::shared_ptr<IDirectory>()};
}

FSResult<FSFileType> PhysicalDirectory::DeduceFileType(const std::string& filePath, FSOpenType type) noexcept
{
    AUTIL_LOG(ERROR, "not support DeduceFileType");
    assert(false);
    return {FSEC_NOTSUP, FSFT_UNKNOWN};
}

FSResult<size_t> PhysicalDirectory::EstimateFileMemoryUse(const std::string& filePath, FSOpenType type) noexcept
{
    AUTIL_LOG(ERROR, "not support EstimateFileMemoryUse");
    assert(false);
    return {FSEC_NOTSUP, 0};
}

FSResult<size_t> PhysicalDirectory::EstimateFileMemoryUseChange(const std::string& filePath,
                                                                const std::string& oldTemperature,
                                                                const std::string& newTemperature) noexcept
{
    AUTIL_LOG(ERROR, "not support EstimateFileMemoryUseChange");
    assert(false);
    return {FSEC_NOTSUP, 0};
}

FSResult<void> PhysicalDirectory::Close() noexcept { return FSEC_OK; }

const std::string& PhysicalDirectory::GetRootDir() const noexcept { return _rootDir; }

FSResult<bool> PhysicalDirectory::IsExist(const std::string& path) const noexcept
{
    std::string absPath = AbsPath(path);
    return FslibWrapper::IsExist(absPath);
}

FSResult<bool> PhysicalDirectory::IsDir(const std::string& path) const noexcept
{
    std::string absPath = AbsPath(path);
    return FslibWrapper::IsDir(absPath);
}

FSResult<void> PhysicalDirectory::ListDir(const std::string& path, const ListOption& listOption,
                                          std::vector<std::string>& fileList) const noexcept
{
    std::string absPath = AbsPath(path);
    if (listOption.recursive) {
        if (fslib::fs::FileSystem::getFsType(_rootDir) != FSLIB_FS_LOCAL_FILESYSTEM_NAME) {
            assert(false); // very heavy operation
        }
        return FslibWrapper::ListDirRecursive(absPath, fileList);
    }
    return FslibWrapper::ListDir(absPath, fileList).Code();
}

FSResult<void> PhysicalDirectory::Load(const std::string& fileName, const ReaderOption& readerOption,
                                       std::string& fileContent) noexcept
{
    std::string absPath = AbsPath(fileName);
    return FslibWrapper::AtomicLoad(absPath, fileContent).Code();
}

FSResult<void> PhysicalDirectory::Store(const std::string& fileName, const std::string& fileContent,
                                        const WriterOption& writerOption) noexcept
{
    std::string absPath = AbsPath(fileName);
    return FslibWrapper::AtomicStore(absPath, fileContent, false).Code();
}

FSResult<size_t> PhysicalDirectory::GetFileLength(const std::string& filePath) const noexcept
{
    std::string absPath = AbsPath(filePath);
    return FslibWrapper::GetFileLength(AbsPath(filePath));
}

std::string PhysicalDirectory::GetOutputPath() const noexcept { return _rootDir; }

std::string PhysicalDirectory::GetPhysicalPath(const std::string& path) const noexcept { return AbsPath(path); }

FSResult<std::shared_future<bool>> PhysicalDirectory::Sync(bool waitFinish) noexcept
{
    std::promise<bool> p;
    auto f = p.get_future();
    p.set_value(true);
    return {FSEC_OK, f.share()};
}

const std::shared_ptr<IFileSystem>& PhysicalDirectory::GetFileSystem() const noexcept
{
    static std::shared_ptr<IFileSystem> nullFs;
    return nullFs;
}

std::shared_ptr<FenceContext> PhysicalDirectory::GetFenceContext() const noexcept
{
    return std::shared_ptr<FenceContext>(FenceContext::NoFence());
}

std::string PhysicalDirectory::DebugString(const std::string& path) const noexcept { return AbsPath(path); }

FSResult<std::shared_ptr<FileReader>> PhysicalDirectory::DoCreateFileReader(const std::string& relativePath,
                                                                            const ReaderOption& readerOption) noexcept
{
    std::string absPath = AbsPath(relativePath);
    auto fileNode = std::make_shared<BufferedFileNode>();
    RETURN2_IF_FS_ERROR(fileNode->Open(absPath, absPath, FSOT_BUFFERED, -1), std::shared_ptr<FileReader>(),
                        "reader open [%s] failed", absPath.c_str());
    return {FSEC_OK, std::make_shared<NormalFileReader>(fileNode)};
}

FSResult<std::shared_ptr<FileWriter>> PhysicalDirectory::DoCreateFileWriter(const std::string& relativePath,
                                                                            const WriterOption& writerOption) noexcept
{
    auto writer = std::make_shared<BufferedFileWriter>(writerOption);
    std::string absPath = AbsPath(relativePath);
    RETURN2_IF_FS_ERROR(writer->Open(absPath, absPath), std::shared_ptr<FileWriter>(), "writer open [%s] failed",
                        absPath.c_str());
    return {FSEC_OK, writer};
}

FSResult<void> PhysicalDirectory::InnerRemoveDirectory(const std::string& dirPath,
                                                       const RemoveOption& removeOption) noexcept
{
    std::string absPath = AbsPath(dirPath);
    DeleteOption deleteOption;
    deleteOption.fenceContext = removeOption.fenceContext;
    deleteOption.mayNonExist = true; // why not use removeOption.mayNonExist?
    return FslibWrapper::DeleteDir(absPath, deleteOption).Code();
}

FSResult<void> PhysicalDirectory::InnerRemoveFile(const std::string& filePath,
                                                  const RemoveOption& removeOption) noexcept
{
    std::string absPath = AbsPath(filePath);
    DeleteOption deleteOption;
    deleteOption.fenceContext = removeOption.fenceContext;
    deleteOption.mayNonExist = true; // why not use removeOption.mayNonExist?
    return FslibWrapper::DeleteFile(absPath, deleteOption).Code();
}

FSResult<void> PhysicalDirectory::InnerRename(const std::string& srcPath,
                                              const std::shared_ptr<IDirectory>& destDirectory,
                                              const std::string& destPath, FenceContext* fenceContext) noexcept
{
    std::string absSrcPath = AbsPath(srcPath);
    std::string absDestPath =
        util::PathUtil::JoinPath(destDirectory->GetLogicalPath(), (destPath.empty() ? srcPath : destPath));

    if (!std::dynamic_pointer_cast<PhysicalDirectory>(destDirectory)) {
        assert(false);
        AUTIL_LOG(ERROR, "Only support physicalDirectory, rename from [%s] to [%s]", absSrcPath.c_str(),
                  absDestPath.c_str());
        return FSEC_NOTSUP;
    }
    return fenceContext ? FslibWrapper::RenameWithFenceContext(absSrcPath, absDestPath, fenceContext)
                        : FslibWrapper::Rename(absSrcPath, absDestPath);
}

}} // namespace indexlib::file_system
