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
#include "indexlib/partition/reopen_decider.h"

#include "indexlib/document/index_locator.h"
#include "indexlib/index/normal/attribute/accessor/pack_attr_update_doc_size_calculator.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/deploy_index_validator.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/operation_queue/operation_iterator.h"
#include "indexlib/partition/realtime_partition_data_reclaimer.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, ReopenDecider);

ReopenDecider::ReopenDecider(const OnlineConfig& onlineConfig, bool forceReopen)
    : mOnlineConfig(onlineConfig)
    , mReopenIncVersion(INVALID_VERSION)
    , mForceReopen(forceReopen)
    , mReopenType(INVALID_REOPEN)
{
}

ReopenDecider::~ReopenDecider() {}

void ReopenDecider::Init(const PartitionDataPtr& partitionData, const std::string& indexSourceRoot,
                         const AttributeMetrics* attributeMetrics, int64_t maxRecoverTs, versionid_t reopenVersionId,
                         const OnlinePartitionReaderPtr& onlineReader)
{
    Version onDiskVersion;
    if (!GetNewOnDiskVersion(partitionData, indexSourceRoot, reopenVersionId, onDiskVersion)) {
        mReopenIncVersion = partitionData->GetOnDiskVersion();
        if (NeedReclaimReaderMem(attributeMetrics)) {
            mReopenType = RECLAIM_READER_REOPEN;
        } else if (NeedSwitchFlushRtSegments(onlineReader, partitionData)) {
            mReopenType = SWITCH_RT_SEGMENT_REOPEN;
        } else {
            mReopenType = NO_NEED_REOPEN;
        }
        return;
    }

    Version lastLoadedVersion = partitionData->GetOnDiskVersion();
    if (onDiskVersion.GetSchemaVersionId() != lastLoadedVersion.GetSchemaVersionId()) {
        IE_LOG(WARN,
               "schema_version not match, "
               "[%u] in target version [%d], [%u] in current version [%d]",
               onDiskVersion.GetSchemaVersionId(), onDiskVersion.GetVersionId(), lastLoadedVersion.GetSchemaVersionId(),
               lastLoadedVersion.GetVersionId());
        mReopenType = INCONSISTENT_SCHEMA_REOPEN;
        return;
    }

    if (onDiskVersion.GetOngoingModifyOperations() != lastLoadedVersion.GetOngoingModifyOperations()) {
        IE_LOG(WARN,
               "ongoing modify operations not match, "
               "[%s] in target version [%d], [%s] in current version [%d]",
               StringUtil::toString(onDiskVersion.GetOngoingModifyOperations(), ",").c_str(),
               onDiskVersion.GetVersionId(),
               StringUtil::toString(lastLoadedVersion.GetOngoingModifyOperations(), ",").c_str(),
               lastLoadedVersion.GetVersionId());
        mReopenType = INCONSISTENT_SCHEMA_REOPEN;
        return;
    }

    if (onDiskVersion.GetTimestamp() < lastLoadedVersion.GetTimestamp()) {
        IE_LOG(WARN,
               "new version with smaller timestamp, "
               "[%ld] in target version [%d], [%ld] in current version [%d]. "
               "new version may be rollback index!",
               onDiskVersion.GetTimestamp(), onDiskVersion.GetVersionId(), lastLoadedVersion.GetTimestamp(),
               lastLoadedVersion.GetVersionId());
        mReopenType = INDEX_ROLL_BACK_REOPEN;
        return;
    }

    if (!mOnlineConfig.AllowReopenOnRecoverRt()) {
        string locatorStr = partitionData->GetLastLocator();
        document::IndexLocator indexLocator;
        if (!locatorStr.empty() && indexLocator.fromString(locatorStr) && indexLocator.getOffset() <= maxRecoverTs) {
            mReopenType = UNABLE_REOPEN;
            IE_LOG(WARN, "rt not finish recover, cannot reopen inc, indexLocator is [%ld], recover ts [%ld]",
                   indexLocator.getOffset(), maxRecoverTs);
            return;
        }
    }

    mReopenType = NORMAL_REOPEN;
    if (mForceReopen) {
        mReopenType = FORCE_REOPEN;
    }
    mReopenIncVersion = onDiskVersion;
}

void ReopenDecider::ValidateDeploySegments(const PartitionDataPtr& partitionData, const Version& version) const
{
    if (mOnlineConfig.IsValidateIndexEnabled()) {
        Version lastLoadedVersion = partitionData->GetOnDiskVersion();
        DirectoryPtr rootDir = partitionData->GetRootDirectory();
        DeployIndexValidator::ValidateDeploySegments(rootDir, version - lastLoadedVersion);
    }
}

bool ReopenDecider::NeedReclaimReaderMem(const AttributeMetrics* attributeMetrics) const
{
    assert(attributeMetrics);

    int64_t curReaderReclaimableBytes = attributeMetrics->GetCurReaderReclaimableBytesValue();
    int64_t maxCurReaderReclaimableMem = mOnlineConfig.maxCurReaderReclaimableMem << 20;
    if (curReaderReclaimableBytes >= maxCurReaderReclaimableMem) {
        IE_LOG(INFO,
               "trigger threshold [%ld],"
               "reopen to reclaim unused reader memory [%ld]",
               maxCurReaderReclaimableMem, curReaderReclaimableBytes);

        return true;
    }
    return false;
}

bool ReopenDecider::NeedSwitchFlushRtSegments(const OnlinePartitionReaderPtr& onlineReader,
                                              const PartitionDataPtr& partitionData)
{
    if (!onlineReader || !partitionData) {
        return false;
    }

    segmentid_t lastValidLinkRtSegId = partitionData->GetLastValidRtSegmentInLinkDirectory();
    if (lastValidLinkRtSegId == INVALID_SEGMENTID) {
        return false;
    }

    return lastValidLinkRtSegId > onlineReader->GetPartitionVersion()->GetLastLinkRtSegmentId();
}

bool ReopenDecider::GetNewOnDiskVersion(const PartitionDataPtr& partitionData, const std::string& indexSourceRoot,
                                        versionid_t reopenVersionId, index_base::Version& onDiskVersion) const
{
    assert(partitionData);
    if (reopenVersionId != -1 && reopenVersionId <= partitionData->GetOnDiskVersion().GetVersionId()) {
        return false;
    }
    VersionLoader::GetVersionS(indexSourceRoot, onDiskVersion, reopenVersionId);
    return onDiskVersion.GetVersionId() > partitionData->GetOnDiskVersion().GetVersionId();
}
}} // namespace indexlib::partition
