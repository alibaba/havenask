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
#include "indexlib/framework/cleaner/InMemoryIndexCleaner.h"

#include "fslib/fs/FileSystem.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/VersionLoader.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, InMemoryIndexCleaner);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

Status InMemoryIndexCleaner::Clean()
{
    assert(_tabletDir != nullptr);
    std::unique_lock<std::mutex> lock;
    if (_cleanerMutex) {
        lock = std::unique_lock<std::mutex>(*_cleanerMutex, std::try_to_lock);
        if (!lock.owns_lock()) {
            TABLET_LOG(WARN, "acquire cleaner lock failed, try again later");
            return Status::OK();
        }
    }
    Status st = DoClean(_tabletDir);
    if (!st.IsOK()) {
        TABLET_LOG(ERROR, "clean logical index failed");
        return st;
    }
    if (_cleanPhysicalFiles) {
        std::string outputPath = _tabletDir->GetOutputPath();
        auto fsType = fslib::fs::FileSystem::getFsType(outputPath);
        if (fsType != FSLIB_FS_LOCAL_FILESYSTEM_NAME) {
            TABLET_LOG(WARN, "can not clean remote path [%s]", outputPath.c_str());
            return Status::InternalError("can not clean remote path");
        }
        st = DoClean(indexlib::file_system::Directory::GetPhysicalDirectory(outputPath));
        if (!st.IsOK()) {
            TABLET_LOG(ERROR, "clean physical index failed");
            return st;
        }
    }
    return Status::OK();
}

Status InMemoryIndexCleaner::DoClean(const std::shared_ptr<indexlib::file_system::Directory>& dir)
{
    auto [status, isExist] = dir->GetIDirectory()->IsExist(/*path*/ "").StatusWith();
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "isexist dir failed, error [%s]", status.ToString().c_str());
        return status;
    }
    if (!isExist) {
        TABLET_LOG(INFO, "tablet dir [%s] not created yet", dir->GetOutputPath().c_str());
        return Status::OK();
    }
    Status st = CleanUnusedSegments(dir);
    if (!st.IsOK()) {
        TABLET_LOG(ERROR, "clean unused segments failed, error [%s]", st.ToString().c_str());
        return st;
    }
    st = CleanUnusedVersions(dir);
    if (!st.IsOK()) {
        TABLET_LOG(ERROR, "clean unused versions failed, error [%s]", st.ToString().c_str());
        return st;
    }
    return Status::OK();
}

Status InMemoryIndexCleaner::CleanUnusedSegments(const std::shared_ptr<indexlib::file_system::Directory>& dir)
{
    fslib::FileList segments;
    Status status = CollectUnusedSegments(dir, &segments);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "collect un-used segments failed");
        return status;
    }

    auto [st, f] = dir->GetIDirectory()->Sync(/*wait finish*/ true).StatusWith();
    if (!st.IsOK()) {
        TABLET_LOG(ERROR, "dir sync failed, logical dir [%s] error [%s]", dir->GetLogicalPath().c_str(),
                   st.ToString().c_str());
        return st;
    }
    f.wait();

    for (size_t i = 0; i < segments.size(); ++i) {
        indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        // removeOption.relaxedConsistency = true;
        st = dir->GetIDirectory()->RemoveDirectory(segments[i], removeOption /*may not exist*/).Status();
        if (!st.IsOK()) {
            TABLET_LOG(ERROR, "remove dir failed, logical dir [%s] segment [%s] error [%s]",
                       dir->GetLogicalPath().c_str(), segments[i].c_str(), st.ToString().c_str());
            return st;
        }
        TABLET_LOG(INFO, "segment [%s][%s] cleaned", dir->GetOutputPath().c_str(), segments[i].c_str());
    }
    if (!segments.empty()) {
        TABLET_LOG(INFO, "end clean unused segment size [%lu]", segments.size());
    }
    return Status::OK();
}

Status InMemoryIndexCleaner::CollectUnusedSegments(const std::shared_ptr<indexlib::file_system::Directory>& dir,
                                                   fslib::FileList* segments)
{
    Version latestVersion;
    Status st = VersionLoader::GetVersion(dir, INVALID_VERSIONID, &latestVersion);
    if (!st.IsOK()) {
        TABLET_LOG(ERROR, "get version failed, logical dir [%s] error [%s]", dir->GetLogicalPath().c_str(),
                   st.ToString().c_str());
        return st;
    }
    fslib::FileList tmpSegments;
    st = VersionLoader::ListSegment(dir, &tmpSegments);
    if (!st.IsOK()) {
        TABLET_LOG(ERROR, "list segments failed, logical dir [%s] error [%s]", dir->GetLogicalPath().c_str(),
                   st.ToString().c_str());
        return st;
    }

    for (size_t i = 0; i < tmpSegments.size(); ++i) {
        segmentid_t segId = PathUtil::GetSegmentIdByDirName(tmpSegments[i]);
        if (segId > latestVersion.GetLastSegmentId()) {
            continue;
        }
        if (latestVersion.HasSegment(segId)) {
            continue;
        }
        if (_tabletReaderContainer->IsUsingSegment(segId)) {
            continue;
        }
        segments->emplace_back(tmpSegments[i]);
    }
    return Status::OK();
}

Status InMemoryIndexCleaner::CleanUnusedVersions(const std::shared_ptr<indexlib::file_system::Directory>& dir)
{
    auto directoryPath = dir->GetOutputPath();
    fslib::FileList fileList;
    std::vector<Version> usingVersions;
    {
        Status status = VersionLoader::ListVersion(dir, &fileList);
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "list version failed, dir [%s]", directoryPath.c_str());
            return status;
        }
        if (fileList.size() <= 1) {
            return Status::OK();
        }
        _tabletReaderContainer->GetIncVersions(usingVersions);
    }

    auto [st, f] = dir->GetIDirectory()->Sync(/*wait finish*/ true).StatusWith();
    if (!st.IsOK()) {
        TABLET_LOG(ERROR, "dir sync failed, logical dir [%s] error [%s]", dir->GetLogicalPath().c_str(),
                   st.ToString().c_str());
        return st;
    }
    // TODO(xiuchen) change to non-block
    f.wait();
    auto latestPrivateVersion = _tabletReaderContainer->GetLatestPrivateVersion();
    for (size_t i = 0; i < fileList.size() - 1; ++i) {
        versionid_t versionId = INVALID_VERSIONID;
        if (!PathUtil::GetVersionId(fileList[i], &versionId)) {
            TABLET_LOG(WARN, "invalid version [%s]", fileList[i].c_str());
            continue;
        }
        if (std::find_if(usingVersions.begin(), usingVersions.end(), [versionId](const Version& version) {
                return versionId == version.GetVersionId();
            }) != usingVersions.end()) {
            continue;
        }
        if ((versionId & Version::PRIVATE_VERSION_ID_MASK) && versionId > latestPrivateVersion) {
            continue;
        }
        auto entryTableFile = indexlib::file_system::EntryTable::GetEntryTableFileName(versionId);

        indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        removeOption.relaxedConsistency = true;
        st = dir->GetIDirectory()->RemoveFile(fileList[i], removeOption /*may not exist*/).Status();
        if (st.IsOK()) {
            st = dir->GetIDirectory()->RemoveFile(entryTableFile, removeOption /*may not exist*/).Status();
        }
        if (!st.IsOK()) {
            TABLET_LOG(ERROR, "remove file failed, logical dir [%s] version file [%s] entryTable file [%s] error [%s]",
                       dir->GetLogicalPath().c_str(), fileList[i].c_str(), entryTableFile.c_str(),
                       st.ToString().c_str());
            return st;
        }

        TABLET_LOG(INFO, "version [%s][%s] cleaned", dir->GetOutputPath().c_str(), fileList[i].c_str());
    }
    if (!fileList.empty()) {
        TABLET_LOG(INFO, "end cleaned unused version, find version [%lu], using version [%lu]", fileList.size(),
                   usingVersions.size());
    }
    return Status::OK();
}

#undef TABLET_LOG

} // namespace indexlibv2::framework
