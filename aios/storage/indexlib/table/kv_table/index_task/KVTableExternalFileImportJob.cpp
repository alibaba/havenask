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
#include "indexlib/table/kv_table/index_task/KVTableExternalFileImportJob.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/SegmentDescriptions.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTableExternalFileImportJob);

using PathUtil = indexlib::util::PathUtil;
using FslibWrapper = indexlib::file_system::FslibWrapper;
using IDirectory = indexlib::file_system::IDirectory;
using FileSystemCreator = indexlib::file_system::FileSystemCreator;

KVTableExternalFileImportJob::KVTableExternalFileImportJob(const std::string& workRoot,
                                                           const framework::ImportExternalFileOptions& importOptions,
                                                           const size_t expectShardCount)
    : _workRoot(workRoot)
    , _importOptions(importOptions)
    , _expectShardCount(expectShardCount)
{
}

Status KVTableExternalFileImportJob::Prepare(const std::vector<std::string>& externalFilesPaths)
{
    Status status;
    if (_importOptions.mode != framework::ImportExternalFileOptions::VERSION_MODE) {
        status = Status::InvalidArgs("invalid mode for import options, kv table only support version mode");
    }
    if (_importOptions.importBehind || _importOptions.failIfNotBottomMostLevel) {
        status = Status::InvalidArgs("import behind not supported for kv table");
    }
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    _internalFileInfos.clear();
    std::set<std::string> externalFilePathSet;
    auto& externalVersions = externalFilesPaths;
    for (size_t i = 0; i < externalVersions.size(); i++) {
        const auto& versionPath = externalVersions[i];
        std::vector<InternalFileInfo> fileInfos;
        status = Convert(versionPath, &fileInfos);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "version convert to file info failed, version path %s, status %s", versionPath.c_str(),
                      status.ToString().c_str());
            return status;
        }
        for (auto& fileInfo : fileInfos) {
            if (_importOptions.ignoreDuplicateFiles) {
                std::string normalizedExternalPath = PathUtil::NormalizeDir(fileInfo.externalFilePath);
                auto iter = externalFilePathSet.find(normalizedExternalPath);
                if (iter != externalFilePathSet.end()) {
                    continue;
                }
                externalFilePathSet.emplace(normalizedExternalPath);
            }
            // sequence num is used to put bulkload external file into right position
            // high 32 bits notify version sequence
            // low 32 bits notify segment sequence
            fileInfo.sequenceNumber |= (i << 32);
            _internalFileInfos.emplace_back(fileInfo);
        }
    }
    if (_internalFileInfos.empty()) {
        status = Status::InvalidArgs("internal file list is empty");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    for (const auto& internalFileInfo : _internalFileInfos) {
        if (internalFileInfo.externalFilePath.empty()) {
            return Status::InvalidArgs("invalid internal file info, empty external file path");
        }
        if (internalFileInfo.targetFileName.empty() && !internalFileInfo.isDir) {
            return Status::InvalidArgs("invalid internal file info, file path is not dir and file name is empty");
        }
    }

    // Copy/Move external files
    for (size_t i = 0; i < _internalFileInfos.size(); i++) {
        auto& f = _internalFileInfos[i];
        if (f.isDir) {
            continue;
        }
        f.copyFile = false;
        std::string internalFilePath = PathUtil::JoinPath(
            _workRoot, std::to_string(i) + "#" + f.targetRelativePathInSegment + "#" + f.targetFileName);
        if (PathUtil::NormalizeDir(f.externalFilePath) == PathUtil::NormalizeDir(internalFilePath)) {
            status = Status::InternalError("src path %s and dst path %s is the same.", f.externalFilePath.c_str(),
                                           internalFilePath.c_str());
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
        status =
            FslibWrapper::DeleteFile(internalFilePath, indexlib::file_system::DeleteOption::MayNonExist()).Status();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "try delete file %s failed", internalFilePath.c_str());
            return status;
        }
        if (_importOptions.moveFiles) {
            status = FslibWrapper::Link(f.externalFilePath, internalFilePath).Status();
            if (status.IsUnimplement() && _importOptions.failedMoveFallBackToCopy) {
                // Original file is on a different FS, use copy instead of hard linking.
                f.copyFile = true;
                AUTIL_LOG(WARN, "link file not supported, fall back to copy, filePath %s", f.externalFilePath.c_str());
            }
        } else {
            f.copyFile = true;
        }
        if (f.copyFile) {
            status = FslibWrapper::Copy(f.externalFilePath, internalFilePath).Status();
        }
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "link or copy external files failed, status is %s", status.ToString().c_str());
            return status;
        }
        f.internalFilePath = internalFilePath;
    }
    return status;
}

Status KVTableExternalFileImportJob::Convert(const std::string& versionPath,
                                             std::vector<InternalFileInfo>* internalFileInfos)
{
    Status status;
    std::string versionFileName = PathUtil::GetFileName(versionPath);
    if (!framework::VersionLoader::IsValidVersionFileName(versionFileName)) {
        return Status::InvalidArgs("invalid external file, file name %s", versionFileName.c_str());
    }
    versionid_t versionId = framework::VersionLoader::GetVersionId(versionFileName);
    std::string root = PathUtil::GetDirectoryPath(versionPath);
    std::shared_ptr<indexlib::file_system::IFileSystem> fs;
    std::tie(status, fs) =
        FileSystemCreator::Create(
            /*name=*/"fs", root, indexlib::file_system::FileSystemOptions(), /*metricsProvider=*/nullptr,
            /*isOverride=*/false, indexlib::file_system::FenceContext::NoFence())
            .StatusWith();
    if (fs == nullptr) {
        status = Status::InternalError("fs is nullptr");
    }
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create file system failed, root path [%s], status:%s", root.c_str(),
                  status.ToString().c_str());
        return status;
    }
    // check if version exists in public root
    status = fs->MountVersion(root, versionId,
                              /*rawLogicalPath=*/"", indexlib::file_system::FSMT_READ_ONLY, /*lifeCycleTable=*/nullptr)
                 .Status();
    if (status.IsNotFound()) {
        // check if version exists in fence root
        framework::Version version;
        status = framework::VersionLoader::GetVersion(indexlib::file_system::Directory::GetPhysicalDirectory(root),
                                                      versionId, &version);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "load version [%d] from [%s] failed: %s", versionId, root.c_str(),
                      status.ToString().c_str());
            return status;
        }
        auto fenceRoot = PathUtil::JoinPath(root, version.GetFenceName());
        status =
            fs->MountVersion(fenceRoot, versionId,
                             /*rawLogicalPath=*/"", indexlib::file_system::FSMT_READ_ONLY, /*lifeCycleTable=*/nullptr)
                .Status();
    }
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "mount version failed, root path [%s], version id [%d], status: %s", root.c_str(), versionId,
                  status.ToString().c_str());
        return status;
    }

    auto rootDir = IDirectory::Get(fs);
    framework::Version version;
    status = framework::VersionLoader::GetVersion(IDirectory::ToLegacyDirectory(rootDir), versionId, &version);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "load version failed, root path [%s], version id [%d], status: %s", root.c_str(), versionId,
                  status.ToString().c_str());
        return status;
    }
    auto segmentDesc = version.GetSegmentDescriptions();
    if (segmentDesc == nullptr) {
        status = Status::InvalidArgs("version segment description is nullptr, version path[%s]", versionPath.c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    auto levelInfo = segmentDesc->GetLevelInfo();
    if (levelInfo == nullptr) {
        status = Status::InvalidArgs("level info is nullptr, version path[%s]", versionPath.c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    if (levelInfo->GetShardCount() != _expectShardCount) {
        status = Status::InvalidArgs("shard count [%lu] not match with expect shard count[%lu], version path[%s]",
                                     levelInfo->GetShardCount(), _expectShardCount, versionPath.c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    uint64_t sequenceNumber = 0;
    for (const auto& segment : version) {
        std::string segDirName = version.GetSegmentDirName(segment.segmentId);
        std::vector<std::string> fileList;
        indexlib::file_system::ListOption listOption;
        listOption.recursive = true;
        status = rootDir->ListDir(segDirName, listOption, fileList).Status();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "list dir failed, root dir [%s], relative path [%s], status: %s",
                      rootDir->GetOutputPath().c_str(), segDirName.c_str(), status.ToString().c_str());
            return status;
        }
        for (const auto& file : fileList) {
            bool isDir = false;
            std::string relativePathInRoot = PathUtil::JoinPath(segDirName, file);
            std::tie(status, isDir) = rootDir->IsDir(relativePathInRoot).StatusWith();
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "check is dir failed, path [%s], status: %s", relativePathInRoot.c_str(),
                          status.ToString().c_str());
                return status;
            }
            InternalFileInfo info;
            info.externalFilePath = rootDir->GetPhysicalPath(relativePathInRoot);
            if (isDir) {
                info.targetRelativePathInSegment = file;
            } else {
                info.targetRelativePathInSegment = PathUtil::GetDirectoryPath(file);
            }
            info.targetFileName = PathUtil::GetFileName(relativePathInRoot);
            info.sequenceNumber = sequenceNumber;
            info.isDir = isDir;
            (*internalFileInfos).push_back(info);
        }
        sequenceNumber++;
    }

    return status;
}

} // namespace indexlibv2::table
