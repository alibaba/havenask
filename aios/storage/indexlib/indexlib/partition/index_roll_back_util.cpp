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
#include "indexlib/partition/index_roll_back_util.h"

#include <beeper/beeper.h>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/index_summary.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/patch/partition_patch_meta.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, IndexRollBackUtil);

IndexRollBackUtil::IndexRollBackUtil() {}

IndexRollBackUtil::~IndexRollBackUtil() {}

versionid_t IndexRollBackUtil::GetLatestOnDiskVersion(const std::string& indexRoot)
{
    Version version;
    try {
        VersionLoader::GetVersionS(indexRoot, version, INVALID_VERSION);
    } catch (const ExceptionBase& e) {
        IE_LOG(ERROR, "get latest version from [%s] failed", indexRoot.c_str());
        return INVALID_VERSION;
    }
    return version.GetVersionId();
}

bool IndexRollBackUtil::CreateRollBackVersion(const std::string& rootPath, versionid_t sourceVersionId,
                                              versionid_t targetVersionId, const std::string& epochId)
{
    auto rollBackFS = FileSystemCreator::Create("RollBack", rootPath, FileSystemOptions::Offline()).GetOrThrow();
    THROW_IF_FS_ERROR(rollBackFS->MountVersion(rootPath, sourceVersionId, "", FSMT_READ_ONLY, nullptr),
                      "mount version failed");
    auto rootDir = Directory::ConstructByFenceContext(
        Directory::Get(rollBackFS), FenceContextPtr(FslibWrapper::CreateFenceContext(rootPath, epochId)),
        rollBackFS->GetFileSystemMetricsReporter());
    // roll back not use fence
    try {
        Version sourceVersion;
        fslib::FileList segmentLists;
        VersionLoader::GetVersion(rootDir, sourceVersion, sourceVersionId);
        VersionLoader::ListSegments(rootDir, segmentLists);
        segmentid_t lastSegmentId = 0;
        if (segmentLists.empty()) {
            IE_LOG(WARN,
                   "no segments found in [%s], "
                   "set lastSegmentId=0 in targetVersion[%d]",
                   rootDir->DebugString().c_str(), targetVersionId);
        } else {
            size_t segmentCount = segmentLists.size();
            lastSegmentId = Version::GetSegmentIdByDirName(segmentLists[segmentCount - 1]);
            if (lastSegmentId == INVALID_SEGMENTID) {
                IE_LOG(ERROR, "invalid segment name[%s] found in [%s]", segmentLists[segmentCount - 1].c_str(),
                       rootDir->DebugString().c_str());
                IE_LOG(ERROR, "create roll back from version[%d] to version[%d] failed", sourceVersionId,
                       targetVersionId);
                BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                                  "create roll back from version[%d] to version[%d] fail!",
                                                  sourceVersionId, targetVersionId);
                return false;
            }
        }

        if (sourceVersion.GetSchemaVersionId() != DEFAULT_SCHEMAID) {
            // // mount patch file
            // const std::string patchMetaFileName
            //     = PartitionPatchMeta::GetPatchMetaFileName(sourceVersion.GetVersionId());
            // rollBackFS->MountFile(rootPath, patchMetaFileName, patchMetaFileName, file_system::FSMT_READ_ONLY, -1,
            // false);

            PartitionPatchMeta patchMeta;
            patchMeta.Load(rootDir, sourceVersion.GetVersionId());
            patchMeta.Store(rootDir, targetVersionId);
        }

        Version latestVersion;
        VersionLoader::GetVersionS(rootPath, latestVersion, INVALID_VERSION);

        Version targetVersion(sourceVersion);
        targetVersion.SetVersionId(targetVersionId);
        targetVersion.SetLastSegmentId(lastSegmentId);
        DeployIndexWrapper::DumpDeployMeta(rootDir, targetVersion);
        // write target version with overwrite flag
        targetVersion.Store(rootDir, true);

        IndexSummary ret = IndexSummary::Load(rootDir, targetVersion.GetVersionId(), latestVersion.GetVersionId());
        ret.Commit(rootDir);
    } catch (const ExceptionBase& e) {
        IE_LOG(ERROR, "create roll back from version[%d] to version[%d] failed", sourceVersionId, targetVersionId);

        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                          "create roll back from version[%d] to version[%d] fail!", sourceVersionId,
                                          targetVersionId);
        return false;
    }

    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                      "create roll back from version[%d] to version[%d] success!", sourceVersionId,
                                      targetVersionId);
    return true;
}

bool IndexRollBackUtil::CheckVersion(const std::string& indexRoot, versionid_t versionId)
{
    Version version;
    try {
        VersionLoader::GetVersionS(indexRoot, version, versionId);
    } catch (const ExceptionBase& e) {
        IE_LOG(ERROR, "get version[%d] from [%s] failed", versionId, indexRoot.c_str());
        return false;
    }
    return true;
}
}} // namespace indexlib::partition
