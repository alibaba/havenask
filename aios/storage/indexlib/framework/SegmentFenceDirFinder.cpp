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
#include "indexlib/framework/SegmentFenceDirFinder.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/framework/Fence.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.table, SegmentFenceDirFinder);

SegmentFenceDirFinder::SegmentFenceDirFinder() {}

SegmentFenceDirFinder::~SegmentFenceDirFinder() {}

Status SegmentFenceDirFinder::Init(const std::string& rootDir, const framework::Version& version)
{
    if (_fileSystem) {
        return Status::OK();
    }
    _version = version;
    ::indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = ::indexlib::file_system::FSST_DISK;
    auto [fsStatus, fsPtr] = ::indexlib::file_system::FileSystemCreator::Create(
                                 "", rootDir, fsOptions, nullptr,
                                 /*isOverride*/ false, ::indexlib::file_system::FenceContext::NoFence())
                                 .StatusWith();
    RETURN_IF_STATUS_ERROR(fsStatus, "init file system failed");
    std::string fenceRoot = PathUtil::JoinPath(rootDir, version.GetFenceName());
    auto status = ::indexlib::file_system::toStatus(
        fsPtr->MountVersion(fenceRoot, version.GetVersionId(),
                            /*logicalPath*/ "", ::indexlib::file_system::FSMT_READ_ONLY, nullptr));
    if (status.IsOK()) {
        _fileSystem = fsPtr;
    }
    RETURN_IF_STATUS_ERROR(status, "mount version [%d] at fence [%s] failed", version.GetVersionId(),
                           fenceRoot.c_str());
    return Status::OK();
}

std::pair<Status, std::string> SegmentFenceDirFinder::GetFenceDir(segmentid_t segId)
{
    if (!_version.HasSegment(segId)) {
        AUTIL_LOG(ERROR, "segment [%d] not found", segId);
        return {Status::NotFound(), ""};
    }
    auto segDir = _version.GetSegmentDirName(segId);
    auto rootDirectory = ::indexlib::file_system::Directory::Get(_fileSystem)->GetIDirectory();
    auto [status, isExist] = rootDirectory->IsExist(segDir).StatusWith();
    RETURN2_IF_STATUS_ERROR(status, "", "check seg dir exist failed");
    if (!isExist) {
        auto status = Status::Corruption("segment [%d] not found", segId);
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return {status, ""};
    }
    auto phyPath = rootDirectory->GetPhysicalPath(segDir);
    return {Status::OK(), framework::Fence::GetFenceNameFromSegDir(phyPath)};
}

} // namespace indexlibv2::framework
