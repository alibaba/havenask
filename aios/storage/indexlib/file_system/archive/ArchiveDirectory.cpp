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
#include "indexlib/file_system/archive/ArchiveDirectory.h"

#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib::file_system {
AUTIL_LOG_SETUP(indexlib.file_system, ArchiveDirectory);

ArchiveDirectory::ArchiveDirectory(const std::shared_ptr<ArchiveFolder>& archiveFolder, const std::string& relativePath)
    : _archiveFolder(archiveFolder)
{
    _relativePath = util::PathUtil::NormalizePath(relativePath);
    _logicalPath = util::PathUtil::JoinPath(_archiveFolder->GetRootDirectory()->GetLogicalPath(), _relativePath);
    assert(archiveFolder);
}

ArchiveDirectory::~ArchiveDirectory() {}

FSResult<std::shared_ptr<IDirectory>> ArchiveDirectory::MakeDirectory(const std::string& dirPath,
                                                                      const DirectoryOption& directoryOption) noexcept
{
    if (directoryOption.packageHint || directoryOption.isMem || !directoryOption.recursive) {
        return {FSEC_NOTSUP, nullptr};
    }
    std::string addSlashDirPath = ArchiveDirectory::PathToArchiveKey(dirPath) + "/";
    for (size_t i = 0; i < addSlashDirPath.size(); ++i) {
        if (addSlashDirPath[i] == '/') {
            std::string subPath = addSlashDirPath.substr(0, i + 1);
            auto [fsec1, isExist] = _archiveFolder->IsExist(subPath);
            RETURN2_IF_FS_ERROR(fsec1, nullptr, "check item [%s] is exist in archive folder [%s] failed",
                                subPath.c_str(), DebugString("").c_str());
            if (isExist) {
                continue;
            }
            auto [fsec2, fileWriter] = _archiveFolder->CreateFileWriter(subPath);
            RETURN2_IF_FS_ERROR(fsec2, nullptr, "create item [%s] writer in archive folder [%s] failed",
                                subPath.c_str(), DebugString("").c_str());
            auto fsec3 = fileWriter->Close();
            RETURN2_IF_FS_ERROR(fsec3, nullptr, "close item [%s] writer in archive folder [%s] failed", subPath.c_str(),
                                DebugString("").c_str());
        }
    }
    return {FSEC_OK, std::make_shared<ArchiveDirectory>(_archiveFolder, addSlashDirPath)};
}
FSResult<std::shared_ptr<IDirectory>> ArchiveDirectory::GetDirectory(const std::string& dirPath) noexcept
{
    std::string normalPath = ArchiveDirectory::PathToArchiveKey(dirPath);
    auto [fsec, isExist] = _archiveFolder->IsExist(normalPath + "/");
    RETURN2_IF_FS_ERROR(fsec, nullptr, "check item [%s/] exist in archive folder [%s] failed ", normalPath.c_str(),
                        DebugString("").c_str());
    if (!isExist) {
        AUTIL_LOG(INFO, "Get directory [%s] from [%s] failed, not exist", dirPath.c_str(), DebugString("").c_str());
        return {FSEC_NOENT, nullptr};
    }
    return {FSEC_OK, std::make_shared<ArchiveDirectory>(_archiveFolder, normalPath)};
}

FSResult<std::shared_ptr<FileWriter>> ArchiveDirectory::CreateFileWriter(const std::string& filePath,
                                                                         const WriterOption& writerOption) noexcept
{
    return _archiveFolder->CreateFileWriter(ArchiveDirectory::PathToArchiveKey(filePath));
}
FSResult<std::shared_ptr<FileReader>> ArchiveDirectory::CreateFileReader(const std::string& filePath,
                                                                         const ReaderOption& readerOption) noexcept
{
    return _archiveFolder->CreateFileReader(ArchiveDirectory::PathToArchiveKey(filePath));
}
FSResult<void> ArchiveDirectory::Load(const std::string& fileName, const ReaderOption& readerOption,
                                      std::string& fileContent) noexcept
{
    auto [fsec1, fileReader] = CreateFileReader(fileName, readerOption);
    RETURN_IF_FS_ERROR(fsec1, "create file reader [%s] failed", fileName.c_str());
    size_t fileLength = fileReader->GetLength();
    fileContent.resize(fileLength);
    auto [fsec2, readSize] = fileReader->Read(fileContent.data(), fileLength, /*offset*/ 0);
    RETURN_IF_FS_ERROR(fsec2, "file reader [%s] read failed", fileName.c_str());
    if (readSize != fileLength) {
        AUTIL_LOG(ERROR, "file reader [%s] read failed, file length [%lu] != read length [%lu]", fileName.c_str(),
                  fileLength, readSize);
        return FSEC_UNKNOWN;
    }
    RETURN_IF_FS_ERROR(fileReader->Close(), "close file reader failed");
    return FSEC_OK;
}
FSResult<void> ArchiveDirectory::Store(const std::string& fileName, const std::string& fileContent,
                                       const WriterOption& writerOption) noexcept
{
    return FSEC_NOTSUP;
}
FSResult<void> ArchiveDirectory::Close() noexcept { return _archiveFolder->Close(); }
FSResult<bool> ArchiveDirectory::IsExist(const std::string& path) const noexcept
{
    std::string normalizePath = ArchiveDirectory::PathToArchiveKey(path);
    auto [ec, isExist] = _archiveFolder->IsExist(normalizePath);
    RETURN2_IF_FS_ERROR(ec, false, "check [%s] exist failed in [%s]", path.c_str(), DebugString("").c_str());
    if (isExist) {
        return {FSEC_OK, isExist};
    }
    return _archiveFolder->IsExist(normalizePath + "/");
}
FSResult<bool> ArchiveDirectory::IsDir(const std::string& path) const noexcept
{
    std::string normalizePath = ArchiveDirectory::PathToArchiveKey(path);
    return _archiveFolder->IsExist(normalizePath + "/");
}

FSResult<void> ArchiveDirectory::ListDir(const std::string& path, const ListOption& listOption,
                                         std::vector<std::string>& fileList) const noexcept
{
    std::vector<std::string> allFileList;
    RETURN_IF_FS_ERROR(_archiveFolder->List(allFileList), "list archive folder failed");

    std::string normalizePath = ArchiveDirectory::PathToArchiveKey(path);
    std::string addSlashDirPath = normalizePath.empty() ? "" : (normalizePath + "/");
    auto it = std::upper_bound(allFileList.begin(), allFileList.end(), addSlashDirPath);
    std::string rootPath = _relativePath.empty() ? "" : _relativePath + "/";
    for (; it != allFileList.end() && it->compare(0, addSlashDirPath.size(), addSlashDirPath) == 0; ++it) {
        std::string subNormalizePath = util::PathUtil::NormalizePath(*it);
        if (!autil::StringUtil::startsWith(subNormalizePath, rootPath)) {
            AUTIL_LOG(ERROR, "list dir get unexpect sub entry [%s] in [%s]", subNormalizePath.c_str(),
                      DebugString("").c_str());
            return FSEC_UNKNOWN;
        }
        std::string relativePath = subNormalizePath.substr(rootPath.size());
        if (listOption.recursive || relativePath.find("/") == std::string::npos) {
            fileList.push_back(relativePath);
        }
    }
    return FSEC_OK;
}

const std::string& ArchiveDirectory::GetLogicalPath() const noexcept { return _logicalPath; }
std::string ArchiveDirectory::GetPhysicalPath(const std::string& path) const noexcept
{
    return _archiveFolder->GetRootDirectory()->GetPhysicalPath(util::PathUtil::JoinPath(_relativePath, path));
}
const std::string& ArchiveDirectory::GetRootDir() const noexcept { return _logicalPath; }
std::string ArchiveDirectory::GetOutputPath() const noexcept { return GetPhysicalPath(""); }
std::string ArchiveDirectory::DebugString(const std::string& path) const noexcept
{
    std::stringstream ss;
    ss << "ArchiveDir [" << GetLogicalPath() << "] => [" << GetPhysicalPath("") << "]";
    return ss.str();
}
std::string ArchiveDirectory::PathToArchiveKey(const std::string& path) const noexcept
{
    return util::PathUtil::JoinPath(_relativePath, util::PathUtil::NormalizePath(path));
}
FSResult<std::shared_ptr<IDirectory>> ArchiveDirectory::CreateArchiveDirectory(const std::string& suffix) noexcept
{
    assert(false);
    return {FSEC_NOTSUP, nullptr};
}
// --------------- not support methods ----------------
FSResult<std::shared_ptr<ResourceFile>> ArchiveDirectory::CreateResourceFile(const std::string& path) noexcept
{
    return {FSEC_NOTSUP, nullptr};
}
FSResult<std::shared_ptr<ResourceFile>> ArchiveDirectory::GetResourceFile(const std::string& path) noexcept
{
    return {FSEC_NOTSUP, nullptr};
}
FSResult<void> ArchiveDirectory::RemoveDirectory(const std::string& dirPath, const RemoveOption& removeOption) noexcept
{
    return FSEC_NOTSUP;
}
FSResult<void> ArchiveDirectory::RemoveFile(const std::string& filePath, const RemoveOption& removeOption) noexcept
{
    return FSEC_NOTSUP;
}
FSResult<void> ArchiveDirectory::Rename(const std::string& srcPath, const std::shared_ptr<IDirectory>& destDirectory,
                                        const std::string& destPath) noexcept
{
    return FSEC_NOTSUP;
}
FSResult<void> ArchiveDirectory::ListPhysicalFile(const std::string& path, const ListOption& listOption,
                                                  std::vector<FileInfo>& fileInfos) noexcept
{
    return FSEC_NOTSUP;
}
FSResult<size_t> ArchiveDirectory::GetFileLength(const std::string& filePath) const noexcept
{
    return {FSEC_NOTSUP, 0};
}
FSResult<size_t> ArchiveDirectory::GetDirectorySize(const std::string& path) noexcept { return {FSEC_NOTSUP, 0}; }

FSResult<std::shared_future<bool>> ArchiveDirectory::Sync(bool waitFinish) noexcept
{
    std::future<bool> retFuture;
    return {FSEC_NOTSUP, retFuture.share()};
}
FSResult<void> ArchiveDirectory::MountPackage() noexcept { return FSEC_NOTSUP; }
FSResult<void> ArchiveDirectory::FlushPackage() noexcept { return FSEC_NOTSUP; }
FSResult<void> ArchiveDirectory::Validate(const std::string& path) const noexcept { return FSEC_NOTSUP; }
bool ArchiveDirectory::IsExistInCache(const std::string& path) noexcept { return false; }
const std::shared_ptr<IFileSystem>& ArchiveDirectory::GetFileSystem() const noexcept
{
    static const std::shared_ptr<IFileSystem> nullFieSystem;
    return nullFieSystem;
}
FSResult<FSStorageType> ArchiveDirectory::GetStorageType(const std::string& path) const noexcept
{
    return {FSEC_NOTSUP, FSST_DISK};
}
std::shared_ptr<FenceContext> ArchiveDirectory::GetFenceContext() const noexcept { return nullptr; }
FSResult<std::shared_ptr<CompressFileInfo>> ArchiveDirectory::GetCompressFileInfo(const std::string& filePath) noexcept
{
    return {FSEC_NOTSUP, nullptr};
}
std::shared_ptr<IDirectory> ArchiveDirectory::CreateLinkDirectory() const noexcept { return nullptr; }
std::shared_ptr<ArchiveFolder> ArchiveDirectory::LEGACY_CreateArchiveFolder(bool legacyMode,
                                                                            const std::string& suffix) noexcept
{
    return nullptr;
}
bool ArchiveDirectory::SetLifecycle(const std::string& lifecycle) noexcept { return false; }
const std::string& ArchiveDirectory::GetRootLinkPath() const noexcept
{
    static const std::string rootLinkPath = "";
    return rootLinkPath;
}
FSResult<size_t> ArchiveDirectory::EstimateFileMemoryUse(const std::string& filePath, FSOpenType type) noexcept
{
    return {FSEC_NOTSUP, 0};
}
FSResult<size_t> ArchiveDirectory::EstimateFileMemoryUseChange(const std::string& filePath,
                                                               const std::string& oldTemperature,
                                                               const std::string& newTemperature) noexcept
{
    return {FSEC_NOTSUP, 0};
}
FSResult<FSFileType> ArchiveDirectory::DeduceFileType(const std::string& filePath, FSOpenType type) noexcept
{
    return {FSEC_NOTSUP, FSFT_UNKNOWN};
}

} // namespace indexlib::file_system
