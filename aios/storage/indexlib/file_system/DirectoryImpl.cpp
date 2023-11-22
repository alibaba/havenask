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
#include "indexlib/file_system/DirectoryImpl.h"

#include <assert.h>
#include <exception>
#include <set>
#include <string.h>

#include "autil/CommonMacros.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FenceDirectory.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/FileSystemMetricsReporter.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/LinkDirectory.h"
#include "indexlib/file_system/LocalDirectory.h"
#include "indexlib/file_system/PhysicalDirectory.h"
#include "indexlib/file_system/archive/ArchiveDirectory.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/file/CompressFileReaderCreator.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/ResourceFile.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/RegularExpression.h"

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, DirectoryImpl);

std::shared_ptr<IDirectory> IDirectory::GetPhysicalDirectory(const std::string& path) noexcept
{
    return std::make_shared<PhysicalDirectory>(path);
}

std::shared_ptr<IDirectory> IDirectory::Get(const std::shared_ptr<IFileSystem>& fs) noexcept
{
    assert(fs);
    return std::make_shared<LocalDirectory>("", fs);
}

std::shared_ptr<IDirectory> IDirectory::ConstructByFenceContext(const std::shared_ptr<IDirectory>& directory,
                                                                const std::shared_ptr<FenceContext>& fenceContext,
                                                                FileSystemMetricsReporter* reporter) noexcept
{
    return fenceContext ? std::make_shared<FenceDirectory>(directory, fenceContext, reporter) : directory;
}

std::shared_ptr<Directory> IDirectory::ToLegacyDirectory(std::shared_ptr<IDirectory> directory) noexcept
{
    return std::make_shared<Directory>(directory);
}

DirectoryImpl::DirectoryImpl() noexcept {}

static bool MatchCompressExcludePattern(const std::string& absFilePath, const std::string& patternStr) noexcept
{
    if (patternStr.empty()) {
        return false;
    }

    auto pattern = LoadConfig::BuiltInPatternToRegexPattern(patternStr);
    return util::RegularExpression::Match(pattern, absFilePath);
}

FSResult<std::shared_ptr<FileWriter>> DirectoryImpl::CreateFileWriter(const std::string& filePath,
                                                                      const WriterOption& writerOption) noexcept
{
    size_t pos = filePath.rfind(COMPRESS_FILE_INFO_SUFFIX);
    if (pos != std::string::npos && pos == filePath.length() - strlen(COMPRESS_FILE_INFO_SUFFIX)) {
        AUTIL_LOG(ERROR, "index file [%s] should not use suffix [%s], which is retained for compress file.",
                  filePath.c_str(), COMPRESS_FILE_INFO_SUFFIX);
        return {FSEC_NOTSUP, std::shared_ptr<FileWriter>()};
    }

    pos = filePath.rfind(COMPRESS_FILE_META_SUFFIX);
    if (pos != std::string::npos && pos == filePath.length() - strlen(COMPRESS_FILE_META_SUFFIX)) {
        AUTIL_LOG(ERROR, "index file [%s] should not use suffix [%s], which is retained for compress file.",
                  filePath.c_str(), COMPRESS_FILE_META_SUFFIX);
        return {FSEC_NOTSUP, std::shared_ptr<FileWriter>()};
    }

    std::string absPath = util::PathUtil::JoinPath(GetRootDir(), filePath);
    if (writerOption.compressorName.empty() ||
        MatchCompressExcludePattern(absPath, writerOption.compressExcludePattern)) {
        return DoCreateFileWriter(filePath, writerOption);
    }
    return DoCreateCompressFileWriter(filePath, writerOption);
}

FSResult<std::shared_ptr<FileReader>> DirectoryImpl::CreateFileReader(const std::string& filePath,
                                                                      const ReaderOption& readerOption) noexcept
{
    std::string compressInfoFilePath = filePath + COMPRESS_FILE_INFO_SUFFIX;
    if (readerOption.supportCompress) {
        std::shared_ptr<CompressFileInfo> compressInfo(new CompressFileInfo);
        auto [ec, success] = LoadCompressFileInfo(filePath, compressInfo);
        RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileReader>(), "LoadCompressFileInfo [%s] failed", filePath.c_str());
        if (success) {
            auto [ec2, dataReader] = DoCreateFileReader(filePath, readerOption);
            RETURN2_IF_FS_ERROR(ec2, dataReader, "DoCreateFileReader [%s] failed", filePath.c_str());

            std::shared_ptr<FileReader> metaFileReader;
            bool useMetaFile = indexlib::util::GetTypeValueFromKeyValueMap(
                compressInfo->additionalInfo, COMPRESS_ENABLE_META_FILE, DEFAULT_COMPRESS_ENABLE_META_FILE);
            if (useMetaFile) {
                std::string metaFilePath = filePath + COMPRESS_FILE_META_SUFFIX;
                auto [tmpEc, metaReader] = DoCreateFileReader(metaFilePath, readerOption);
                RETURN2_IF_FS_ERROR(tmpEc, metaReader, "DoCreateFileReader [%s] failed", metaFilePath.c_str());
                metaFileReader = metaReader;
            }
            auto [ec3, fileReader] = CompressFileReaderCreator::Create(dataReader, metaFileReader, compressInfo, this);
            return {ec3, fileReader};
        }
    }
    return DoCreateFileReader(filePath, readerOption);
}

FSResult<std::shared_ptr<ResourceFile>> DirectoryImpl::CreateResourceFile(const std::string& path) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(GetRootDir(), path);
    auto [ec, resourceFile] = GetFileSystem()->CreateResourceFile(absPath);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<ResourceFile>(), "CreateResourceFile [%s] failed", absPath.c_str());
    return {FSEC_OK, resourceFile};
}

FSResult<std::shared_ptr<ResourceFile>> DirectoryImpl::GetResourceFile(const std::string& path) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(GetRootDir(), path);
    auto [ec, resourceFile] = GetFileSystem()->GetResourceFile(absPath);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<ResourceFile>(), "GetResourceFile [%s] failed", absPath.c_str());
    return {FSEC_OK, resourceFile};
}

FSResult<void> DirectoryImpl::RemoveDirectory(const std::string& dirPath, const RemoveOption& removeOption) noexcept
{
    return InnerRemoveDirectory(dirPath, removeOption);
}

FSResult<void> DirectoryImpl::RemoveFile(const std::string& filePath, const RemoveOption& removeOption) noexcept
{
    return InnerRemoveFile(filePath, removeOption);
}

FSResult<void> DirectoryImpl::Rename(const std::string& srcPath, const std::shared_ptr<IDirectory>& destDirectory,
                                     const std::string& destPath) noexcept
{
    return InnerRename(srcPath, destDirectory, destPath, FenceContext::NoFence());
}

FSResult<bool> DirectoryImpl::IsExist(const std::string& path) const noexcept
{
    std::string absPath = util::PathUtil::JoinPath(GetRootDir(), path);
    return GetFileSystem()->IsExist(absPath);
}

FSResult<bool> DirectoryImpl::IsDir(const std::string& path) const noexcept
{
    std::string absPath = util::PathUtil::JoinPath(GetRootDir(), path);
    return GetFileSystem()->IsDir(absPath);
}

FSResult<void> DirectoryImpl::ListDir(const std::string& path, const ListOption& listOption,
                                      std::vector<std::string>& fileList) const noexcept
{
    std::string absPath = util::PathUtil::JoinPath(GetRootDir(), path);
    return GetFileSystem()->ListDir(absPath, listOption, fileList);
}

FSResult<void> DirectoryImpl::ListPhysicalFile(const std::string& path, const ListOption& listOption,
                                               std::vector<FileInfo>& fileInfos) noexcept
{
    std::string absPath = util::PathUtil::JoinPath(GetRootDir(), path);
    return GetFileSystem()->ListPhysicalFile(absPath, listOption, fileInfos);
}

FSResult<void> DirectoryImpl::Load(const std::string& fileName, const ReaderOption& readerOption,
                                   std::string& fileContent) noexcept
{
    ReaderOption newReaderOption(readerOption);
    newReaderOption.openType = FSOT_MEM;
    auto [ec, fileReader] = CreateFileReader(fileName, newReaderOption);
    if (ec != FSEC_OK) {
        if (ec == FSEC_NOENT && readerOption.mayNonExist) {
            AUTIL_LOG(DEBUG, "CreateFileReader [%s] failed", fileName.c_str());
        } else {
            AUTIL_LOG(ERROR, "CreateFileReader [%s] failed", fileName.c_str());
        }
        return ec;
    }
    size_t fileLength = fileReader->GetLength();
    char* data = (char*)fileReader->GetBaseAddress();
    assert(fileLength == 0 || data);
    fileContent = std::string(data, fileLength);
    RETURN_IF_FS_ERROR(fileReader->Close(), "close file reader failed");
    return FSEC_OK;
}

FSResult<void> DirectoryImpl::Store(const std::string& fileName, const std::string& fileContent,
                                    const WriterOption& writerOption) noexcept
{
    WriterOption newWriterOption = writerOption;
    if (writerOption.fileLength < 0) {
        newWriterOption.fileLength = fileContent.size();
    }
    auto [ec, fileWriter] = CreateFileWriter(fileName, newWriterOption);
    RETURN_IF_FS_ERROR(ec, "CreateFileWriter [%s] failed, len[%ld]", fileName.c_str(), newWriterOption.fileLength);
    RETURN_IF_FS_ERROR(fileWriter->Write(fileContent.c_str(), fileContent.size()).Code(),
                       "write file [%s] failed, len[%ld]", fileName.c_str(), newWriterOption.fileLength);
    RETURN_IF_FS_ERROR(fileWriter->Close(), "close file [%s] failed", fileName.c_str());
    return FSEC_OK;
}

FSResult<size_t> DirectoryImpl::GetFileLength(const std::string& filePath) const noexcept
{
    std::string absPath = util::PathUtil::JoinPath(GetRootDir(), filePath);
    return GetFileSystem()->GetFileLength(absPath);
}

// GetDirectorySize is not an accurate physical size of the directory. The implementation depends on entry table thus
// files not included in entry table(e.g. segment_file_list, segment_metrics etc) will not be counted in the result.
FSResult<size_t> DirectoryImpl::GetDirectorySize(const std::string& path) noexcept
{
    auto [ec, isDir] = IsDir(path);
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "Cannot GetDirectorySize, invalid path[%s].", path.c_str());
        return {ec, 0};
    }
    if (!isDir) {
        AUTIL_LOG(ERROR, "Cannot GetDirectorySize on non-dir path[%s].", path.c_str());
        return {FSEC_NOTDIR, 0};
    }

    std::vector<FileInfo> fileInfos;
    ec = ListPhysicalFile(path, ListOption::Recursive(true), fileInfos);
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "Failed to ListPhysicalFile, path[%s], ec[%d]", path.c_str(), ec);
        return {ec, 0};
    }
    size_t dirSize = 0;
    std::set<std::string> processedPaths;
    for (const auto& fileInfo : fileInfos) {
        const std::string filePath = util::PathUtil::JoinPath(path, fileInfo.filePath);
        if (processedPaths.find(filePath) != processedPaths.end()) {
            continue;
        }
        processedPaths.insert(filePath);
        auto [ec, isDir] = IsDir(filePath);
        if (ec != FSEC_OK) {
            AUTIL_LOG(WARN, "Invalid file encountered inside GetDirectorySize(), filePath[%s].", filePath.c_str());
            continue;
        }
        if (isDir) {
            continue;
        }
        dirSize += fileInfo.fileLength;
    }
    return {FSEC_OK, dirSize};
}

std::string DirectoryImpl::GetOutputPath() const noexcept
{
    assert(GetFileSystem() != nullptr);
    return util::PathUtil::JoinPath(GetFileSystem()->GetOutputRootPath(), GetRootDir());
}

const std::string& DirectoryImpl::GetLogicalPath() const noexcept { return GetRootDir(); }

std::string DirectoryImpl::GetPhysicalPath(const std::string& path) const noexcept
{
    auto [ec, retPath] = GetFileSystem()->GetPhysicalPath(util::PathUtil::JoinPath(GetRootDir(), path));
    if (ec == FSEC_OK) {
        return retPath;
    }
    assert(ec == FSEC_NOENT || ec == FSEC_ERROR);
    AUTIL_LOG(ERROR, "Get physical path from logical path[%s] failed, output root[%s], ec[%d]", path.c_str(),
              GetFileSystem()->DebugString().c_str(), ec);
    return "";
}

const std::string& DirectoryImpl::GetRootLinkPath() const noexcept { return GetFileSystem()->GetRootLinkPath(); }

FSResult<std::shared_future<bool>> DirectoryImpl::Sync(bool waitFinish) noexcept
{
    if (unlikely(GetFileSystem() == nullptr)) {
        AUTIL_LOG(ERROR, "null filesystem not support sync, [%s]", DebugString("").c_str());
        std::future<bool> retFuture;
        return {FSEC_NOTSUP, retFuture.share()};
    }
    return GetFileSystem()->Sync(waitFinish);
}

FSResult<void> DirectoryImpl::MountPackage() noexcept
{
    if (unlikely(GetFileSystem() == nullptr)) {
        AUTIL_LOG(ERROR, "null filesystem not support mount package, [%s]", DebugString("").c_str());
        return FSEC_NOTSUP;
    }
    return GetFileSystem()->MountSegment(GetRootDir());
}

FSResult<void> DirectoryImpl::FlushPackage() noexcept
{
    if (unlikely(GetFileSystem() == nullptr)) {
        AUTIL_LOG(ERROR, "null filesystem not support flush package, [%s]", DebugString("").c_str());
        return FSEC_NOTSUP;
    }
    return GetFileSystem()->FlushPackage(GetRootDir());
}

FSResult<void> DirectoryImpl::Validate(const std::string& path) const noexcept
{
    if (unlikely(GetFileSystem() == nullptr)) {
        AUTIL_LOG(ERROR, "null filesystem not support validate, [%s]", DebugString(path).c_str());
        return FSEC_NOTSUP;
    }
    std::string absPath = util::PathUtil::JoinPath(GetRootDir(), path);
    return GetFileSystem()->Validate(absPath);
}

bool DirectoryImpl::IsExistInCache(const std::string& path) noexcept
{
    assert(GetFileSystem());
    if (unlikely(GetFileSystem() == nullptr)) {
        return false;
    }
    std::string absPath = util::PathUtil::JoinPath(GetRootDir(), path);
    return GetFileSystem()->IsExistInCache(absPath);
}

FSResult<FSStorageType> DirectoryImpl::GetStorageType(const std::string& path) const noexcept
{
    if (unlikely(GetFileSystem() == nullptr)) {
        AUTIL_LOG(ERROR, "null filesystem not support get storage type, [%s]", DebugString(path).c_str());
        return {FSEC_NOTSUP, FSST_UNKNOWN};
    }
    auto [ec, storageType] = GetFileSystem()->GetStorageType(util::PathUtil::JoinPath(GetRootDir(), path));
    RETURN2_IF_FS_ERROR(ec, FSST_UNKNOWN, "get path [%s] storage type failed", path.c_str());
    if (storageType == FSST_MEM || storageType == FSST_PACKAGE_MEM) {
        return {FSEC_OK, FSST_MEM};
    } else if (storageType == FSST_DISK || storageType == FSST_PACKAGE_DISK) {
        return {FSEC_OK, FSST_DISK};
    }
    return {FSEC_OK, FSST_UNKNOWN};
}

FSResult<std::shared_ptr<CompressFileInfo>> DirectoryImpl::GetCompressFileInfo(const std::string& filePath) noexcept
{
    std::string compressInfoPath = filePath + COMPRESS_FILE_INFO_SUFFIX;
    std::string compressInfoStr;
    ReaderOption option(FSOT_MEM);
    option.mayNonExist = true;
    auto ec = Load(compressInfoPath, option, compressInfoStr).Code();
    if (ec == FSEC_NOENT) {
        return {FSEC_OK, std::shared_ptr<CompressFileInfo>()};
    }
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<CompressFileInfo>(), "load [%s] failed",
                        DebugString(compressInfoPath).c_str());
    std::shared_ptr<CompressFileInfo> info(new CompressFileInfo);
    auto ec2 = JsonUtil::FromString(compressInfoStr, info.get()).Code();
    RETURN2_IF_FS_ERROR(ec2, std::shared_ptr<CompressFileInfo>(), "from json string failed");
    return {FSEC_OK, info};
}

std::shared_ptr<IDirectory> DirectoryImpl::CreateLinkDirectory() const noexcept
{
    if (unlikely(GetFileSystem() == nullptr)) {
        AUTIL_LOG(ERROR, "null filesystem not support create link direcsory, [%s]", DebugString("").c_str());
        return nullptr;
    }
    if (!GetFileSystem()->GetFileSystemOptions().useRootLink) {
        return std::shared_ptr<LinkDirectory>();
    }
    const std::string& rootPath = "";
    std::string relativePath;
    if (!util::PathUtil::GetRelativePath(rootPath, GetRootDir(), relativePath)) {
        AUTIL_LOG(ERROR, "dir path [%s] not in root path[%s]", GetRootDir().c_str(), rootPath.c_str());
        return std::shared_ptr<IDirectory>();
    }

    std::string linkPath = util::PathUtil::JoinPath(GetFileSystem()->GetRootLinkPath(), relativePath);
    auto [ec, exist] = GetFileSystem()->IsExist(linkPath);
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "IsExist [%s] failed", linkPath.c_str());
        return std::shared_ptr<IDirectory>();
    }
    if (!exist) {
        return std::shared_ptr<IDirectory>();
    }
    return std::make_shared<LinkDirectory>(linkPath, GetFileSystem());
}

std::shared_ptr<ArchiveFolder> DirectoryImpl::LEGACY_CreateArchiveFolder(bool legacyMode,
                                                                         const std::string& suffix) noexcept
{
    // TODO: refer to original code rewrite
    std::shared_ptr<ArchiveFolder> folder(new ArchiveFolder(legacyMode));
    auto [ec, directory] = GetDirectory("");
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "GetDirectory [%s] failed", DebugString("").c_str());
        return std::shared_ptr<ArchiveFolder>();
    }
    ec = folder->Open(directory, suffix).Code();
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "dir path [%s] not in root path[%s], ec[%d]", GetRootDir().c_str(),
                  GetFileSystem() ? GetFileSystem()->DebugString().c_str() : "", ec);
        return std::shared_ptr<ArchiveFolder>();
    }
    return folder;
}

FSResult<std::shared_ptr<IDirectory>> DirectoryImpl::CreateArchiveDirectory(const std::string& suffix) noexcept
{
    std::shared_ptr<ArchiveFolder> folder(new ArchiveFolder(/*legacyMode*/ false));
    auto [ec, directory] = GetDirectory("");
    RETURN2_IF_FS_ERROR(ec, nullptr, "GetDirectory [%s] failed", DebugString("").c_str());
    ec = folder->Open(directory, suffix).Code();
    RETURN2_IF_FS_ERROR(ec, nullptr, "Open archive folder [%s] failed", DebugString("").c_str());
    return {FSEC_OK, std::make_shared<ArchiveDirectory>(folder, /* relativePath*/ "")};
}

bool DirectoryImpl::SetLifecycle(const std::string& lifecycle) noexcept
{
    if (unlikely(GetFileSystem() == nullptr)) {
        AUTIL_LOG(ERROR, "null filesystem not support set lifecycle, [%s]", DebugString("").c_str());
        return false;
    }
    return GetFileSystem()->SetDirLifecycle(GetRootDir(), lifecycle);
}

std::string DirectoryImpl::DebugString(const std::string& path) const noexcept
{
    return (GetFileSystem() ? GetFileSystem()->GetOutputRootPath() : "") + ":" +
           util::PathUtil::JoinPath(GetRootDir(), path);
}

FSResult<bool> DirectoryImpl::LoadCompressFileInfo(const std::string& filePath,
                                                   std::shared_ptr<CompressFileInfo>& compressInfo) noexcept
{
    std::string compressInfoPath = filePath + COMPRESS_FILE_INFO_SUFFIX;
    std::string compressInfoStr;
    ReaderOption option(FSOT_MEM);
    option.mayNonExist = true;
    auto ec = Load(compressInfoPath, option, compressInfoStr).Code();
    if (ec == FSEC_NOENT) {
        return {FSEC_OK, false};
    }
    RETURN2_IF_FS_ERROR(ec, false, "Load [%s] failed", compressInfoPath.c_str());
    compressInfo->FromString(compressInfoStr);
    return {FSEC_OK, true};
}

FSResult<std::shared_ptr<FileWriter>>
DirectoryImpl::DoCreateCompressFileWriter(const std::string& filePath, const WriterOption& writerOption) noexcept
{
    std::string compressInfoFilePath = filePath + COMPRESS_FILE_INFO_SUFFIX;
    auto [ec, compressInfoFileWriter] = DoCreateFileWriter(compressInfoFilePath, writerOption);
    RETURN2_IF_FS_ERROR(ec, compressInfoFileWriter, "DoCreateFileWriter [%s] failed", compressInfoFilePath.c_str());
    assert(compressInfoFileWriter);

    std::shared_ptr<FileWriter> metaFileWriter;
    bool useMetaFile = indexlib::util::GetTypeValueFromKeyValueMap(
        writerOption.compressorParams, COMPRESS_ENABLE_META_FILE, DEFAULT_COMPRESS_ENABLE_META_FILE);
    if (useMetaFile) {
        std::string compressMetaFilePath = filePath + COMPRESS_FILE_META_SUFFIX;
        auto [ec2, compressMetaFileWriter] = DoCreateFileWriter(compressMetaFilePath, writerOption);
        RETURN2_IF_FS_ERROR(ec2, compressMetaFileWriter, "DoCreateFileWriter [%s] failed",
                            compressMetaFilePath.c_str());
        assert(compressMetaFileWriter);
        metaFileWriter = compressMetaFileWriter;
    }

    auto [ec3, fileWriter] = DoCreateFileWriter(filePath, writerOption);
    RETURN2_IF_FS_ERROR(ec3, fileWriter, "DoCreateFileWriter [%s] failed", filePath.c_str());
    assert(fileWriter);

    std::shared_ptr<CompressFileWriter> compressWriter(
        new CompressFileWriter(GetFileSystem()->GetFileSystemMetricsReporter()));
    RETURN2_IF_FS_ERROR(compressWriter->Init(fileWriter, compressInfoFileWriter, metaFileWriter,
                                             writerOption.compressorName, writerOption.compressBufferSize,
                                             writerOption.compressorParams),
                        compressWriter, "init CompressFileWriter failed");
    return {FSEC_OK, compressWriter};
}

}} // namespace indexlib::file_system
