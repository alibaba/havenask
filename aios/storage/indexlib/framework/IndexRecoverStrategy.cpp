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
#include "indexlib/framework/IndexRecoverStrategy.h"

#include <assert.h>
#include <stddef.h>
#include <string>
#include <vector>

#include "fslib/common/common_type.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/framework/VersionLoader.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, IndexRecoverStrategy);

std::pair<Status, Version>
IndexRecoverStrategy::RecoverLatestVersion(const std::shared_ptr<indexlib::file_system::Directory>& physicalDirectory,
                                           bool cleanBrokenSegments)
{
    assert(physicalDirectory);
    auto [status, isExist] = physicalDirectory->GetIDirectory()->IsExist(/*path*/ "").StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "is exsit failed: %s", status.ToString().c_str());
        return {status, Version()};
    }
    if (!isExist) {
        AUTIL_LOG(INFO, "physicalDirectory not exist, recover do nothing");
        return {Status::OK(), Version()};
    }
    AUTIL_LOG(INFO, "begin recover latest version in path[%s]", physicalDirectory->GetLogicalPath().c_str());
    fslib::FileList versionList;
    status = VersionLoader::ListVersion(physicalDirectory, &versionList);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "list version failed: %s", status.ToString().c_str());
        return {status, Version()};
    }
    std::string latestVersionFileName = versionList.empty() ? "" : *versionList.rbegin();
    Version version;
    if (!latestVersionFileName.empty()) {
        status = VersionLoader::LoadVersion(physicalDirectory, latestVersionFileName, &version);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "load version [%s] failed: %s", latestVersionFileName.c_str(), status.ToString().c_str());
            return {status, Version()};
        }
    }
    if (cleanBrokenSegments) {
        status = CleanUselessSegments(physicalDirectory, version);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "clean useless segments in version [%s] failed: %s", latestVersionFileName.c_str(),
                      status.ToString().c_str());
            return {status, Version()};
        }
    }

    AUTIL_LOG(INFO, "latest version in path[%s] is version[%d], last segment id[%d]",
              physicalDirectory->GetLogicalPath().c_str(), version.GetVersionId(), version.GetLastSegmentId());
    return {Status::OK(), std::move(version)};
}

Status
IndexRecoverStrategy::CleanUselessSegments(const std::shared_ptr<indexlib::file_system::Directory>& physicalDirectory,
                                           const Version& version)
{
    auto lastSegmentId = version.GetLastSegmentId();
    fslib::FileList fileList;
    auto status = physicalDirectory->GetIDirectory()
                      ->ListDir(
                          /*path*/ "", indexlib::file_system::ListOption::Recursive(false), fileList)
                      .Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "list dir [%s] failed: %s", physicalDirectory->GetLogicalPath().c_str(),
                  status.ToString().c_str());
        return status;
    }
    for (size_t i = 0; i < fileList.size(); i++) {
        const auto& pathName = fileList[i];
        if (!PathUtil::IsValidSegmentDirName(pathName)) {
            continue;
        }
        segmentid_t segId = PathUtil::GetSegmentIdByDirName(pathName);
        if (segId > lastSegmentId) {
            AUTIL_LOG(INFO, "removing lost segment[%s]", pathName.c_str());
            indexlib::file_system::RemoveOption removeOption;
            status = physicalDirectory->GetIDirectory()->RemoveDirectory(pathName, removeOption).Status();
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "remove dir [%s] failed: %s", pathName.c_str(), status.ToString().c_str());
                return status;
            }
        }
    }
    return Status::OK();
}
} // namespace indexlibv2::framework
