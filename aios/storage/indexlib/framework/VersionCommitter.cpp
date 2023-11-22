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
#include "indexlib/framework/VersionCommitter.h"

#include <algorithm>
#include <assert.h>
#include <ostream>

#include "indexlib/base/Constant.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, VersionCommitter);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] " format, version.GetTabletName().c_str(), ##args)

std::string VersionCommitter::GetVersionFileName(versionid_t versionId)
{
    std::stringstream ss;
    ss << VERSION_FILE_NAME_PREFIX << "." << versionId;
    return ss.str();
}

Status VersionCommitter::Commit(const Version& version, const Fence& fence,
                                const std::vector<std::string>& filteredDirs)
{
    auto versionId = version.GetVersionId();
    TABLET_LOG(INFO, "begin commit version, version id [%d]", versionId);
    if (versionId == INVALID_VERSIONID) {
        TABLET_LOG(ERROR, "commit version failed, version id is invalid");
        return Status::InvalidArgs("not support store invalid version!");
    }
    auto fenceDirectory = indexlib::file_system::Directory::Get(fence.GetFileSystem());
    // 1. commit version to build dir
    // 2. commit version to partition dir
    //    a) if exist, return EC_EXIST
    const std::string versionFileName = GetVersionFileName(version.GetVersionId());
    Status status = Commit(version, fenceDirectory, filteredDirs);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "commit version[%s] in fence failed: %s", versionFileName.c_str(), status.ToString().c_str());
        return status;
    }
    return Status::OK();
}

Status VersionCommitter::CommitAndPublish(const Version& version, const Fence& fence,
                                          const std::vector<std::string>& filteredDirs)
{
    auto status = Commit(version, fence, filteredDirs);
    RETURN_IF_STATUS_ERROR(status, "");
    const std::string versionContent = version.ToString();
    auto fsErrCode = PublishVersion(version, fence.GetGlobalRoot(), fence.GetFenceName());
    if (fsErrCode == indexlib::file_system::FSEC_OK) {
        TABLET_LOG(INFO, "success commit version, version id [%d]", version.GetVersionId());
        return Status::OK();
    } else if (fsErrCode != indexlib::file_system::FSEC_EXIST) {
        const std::string versionFileName = GetVersionFileName(version.GetVersionId());
        TABLET_LOG(ERROR, "commit version[%s] to master failed, error code[%d]", versionFileName.c_str(), fsErrCode);
        return Status::IOError("commit version to master failed");
    } else {
        // only ec_exist need to rollback
        // status = RollbackBranchVersion(branchFS->GetBranchDirectory(), versionFileName);
        // if (!status.IsOK()) {
        //     TABLET_LOG(ERROR, "rollback master version[%s] failed, error[%s]", versionFileName.c_str(),
        //               status.ToString.data());
        //     return status;
        // }
        TABLET_LOG(WARN, "publish version[%d] failed, already exist", version.GetVersionId());
        return Status::Exist("publish version failed");
    }
    // TODO(hanyao): add beeper support
    return Status::Unknown("should not reach here");
}

void VersionCommitter::FillWishList(const Version& version, std::vector<std::string>* wishFileList,
                                    std::vector<std::string>* wishDirList)
{
    // TODO(hanyao): fill by table
    wishFileList->emplace_back(INDEX_FORMAT_VERSION_FILE_NAME);
    // TODO(hanyao): schema name by version
    // wishFileList->emplace_back(version.GetSchemaFileName());
    wishFileList->emplace_back(SCHEMA_FILE_NAME);
    wishFileList->emplace_back(INDEX_PARTITION_META_FILE_NAME);
    wishFileList->emplace_back(version.GetVersionFileName());
    wishFileList->emplace_back(std::string(indexlib::DEPLOY_META_FILE_NAME_PREFIX) + "." +
                               std::to_string(version.GetVersionId()));
    wishFileList->emplace_back(PARALLEL_BUILD_INFO_FILE);
    wishDirList->emplace_back(TRUNCATE_META_DIR_NAME);
    wishDirList->emplace_back(ADAPTIVE_DICT_DIR_NAME);

    // Add segment dir
    for (auto [segmentId, schemaId] : version) {
        wishDirList->emplace_back(version.GetSegmentDirName(segmentId));
    }

    // Add all schema list
    auto schemaList = version.GetSchemaVersionRoadMap();
    for (auto schemaId : schemaList) {
        if (schemaId != DEFAULT_SCHEMAID) {
            wishFileList->emplace_back(config::TabletSchema::GetSchemaFileName(schemaId));
        }
    }
}

Status VersionCommitter::Commit(const Version& version,
                                const std::shared_ptr<indexlib::file_system::Directory>& fenceDirectory,
                                const std::vector<std::string>& filteredDirs)
{
    const std::string versionFileName = GetVersionFileName(version.GetVersionId());
    const std::string versionFilePath =
        indexlib::util::PathUtil::JoinPath(fenceDirectory->GetOutputPath(), versionFileName);
    TABLET_LOG(INFO, "store version to [%s]", versionFilePath.c_str());
    indexlib::file_system::IFileSystemPtr fs = fenceDirectory->GetFileSystem();
    assert(fs);
    assert(fenceDirectory->GetLogicalPath().empty());
    std::vector<std::string> fileList;
    std::vector<std::string> dirList;
    FillWishList(version, &fileList, &dirList);
    auto status = indexlib::file_system::toStatus(
        fs->CommitSelectedFilesAndDir(version.GetVersionId(), fileList, dirList, filteredDirs, versionFileName,
                                      version.ToString(), indexlib::file_system::FenceContext::NoFence()));
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "commit version [%s] failed: %s", versionFilePath.c_str(), status.ToString().c_str());
    }
    return status;
}

indexlib::file_system::ErrorCode VersionCommitter::PublishVersion(const Version& version, const std::string& globalRoot,
                                                                  const std::string& tempIdentifier)
{
    const std::string versionFileName =
        indexlib::util::PathUtil::JoinPath(globalRoot, GetVersionFileName(version.GetVersionId()));
    const std::string tmpVersionFileName = versionFileName + "." + tempIdentifier + ".tmp";
    const std::string versionContent = version.ToString();
    auto fsErrCode = indexlib::file_system::FslibWrapper::AtomicStore(tmpVersionFileName, versionContent).Code();
    if (fsErrCode != indexlib::file_system::FSEC_OK) {
        TABLET_LOG(ERROR, "store version[%s] failed, error code[%d]", tmpVersionFileName.c_str(), fsErrCode);
        return fsErrCode;
    }
    fsErrCode = indexlib::file_system::FslibWrapper::Rename(tmpVersionFileName, versionFileName).Code();
    if (fsErrCode != indexlib::file_system::FSEC_OK) {
        TABLET_LOG(ERROR, "rename tmp version[%s] failed, error code[%d]", tmpVersionFileName.c_str(), fsErrCode);
        return fsErrCode;
    }
    return fsErrCode;
}

} // namespace indexlibv2::framework
