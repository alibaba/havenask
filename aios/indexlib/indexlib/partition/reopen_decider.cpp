#include "indexlib/partition/reopen_decider.h"
#include "indexlib/partition/realtime_partition_data_reclaimer.h"
#include "indexlib/partition/operation_queue/operation_iterator.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index/normal/attribute/accessor/pack_attr_update_doc_size_calculator.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/partition/deploy_index_validator.h"
#include "indexlib/partition/online_partition_reader.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, ReopenDecider);

ReopenDecider::ReopenDecider(const OnlineConfig& onlineConfig,
                             bool forceReopen)
    : mOnlineConfig(onlineConfig)
    , mReopenIncVersion(INVALID_VERSION)
    , mForceReopen(forceReopen)
    , mReopenType(INVALID_REOPEN)
{
}

ReopenDecider::~ReopenDecider() 
{
}

void ReopenDecider::Init(
        const PartitionDataPtr& partitionData,
        const AttributeMetrics* attributeMetrics,
        versionid_t reopenVersionId,
        const OnlinePartitionReaderPtr& onlineReader)
{
    Version onDiskVersion;
    if (!GetNewOnDiskVersion(partitionData, reopenVersionId, onDiskVersion))
    {
        mReopenIncVersion = partitionData->GetOnDiskVersion();
        if (NeedReclaimReaderMem(attributeMetrics))
        {
            mReopenType = RECLAIM_READER_REOPEN;
        }
        else if (NeedSwitchFlushRtSegments(onlineReader, partitionData))
        {
            mReopenType = SWITCH_RT_SEGMENT_REOPEN;
        }
        else
        {
            mReopenType = NO_NEED_REOPEN;
        }
        return;
    }

    Version lastLoadedVersion = partitionData->GetOnDiskVersion();
    if (onDiskVersion.GetSchemaVersionId() != lastLoadedVersion.GetSchemaVersionId())
    {
        IE_LOG(WARN, "schema_version not match, "
               "[%u] in target version [%d], [%u] in current version [%d]",
               onDiskVersion.GetSchemaVersionId(), onDiskVersion.GetVersionId(),
               lastLoadedVersion.GetSchemaVersionId(),
               lastLoadedVersion.GetVersionId());
        mReopenType = INCONSISTENT_SCHEMA_REOPEN;
        return;
    }

    if (onDiskVersion.GetOngoingModifyOperations() != lastLoadedVersion.GetOngoingModifyOperations())
    {
        IE_LOG(WARN, "ongoing modify operations not match, "
               "[%s] in target version [%d], [%s] in current version [%d]",
               StringUtil::toString(onDiskVersion.GetOngoingModifyOperations(), ",").c_str(),
               onDiskVersion.GetVersionId(),
               StringUtil::toString(lastLoadedVersion.GetOngoingModifyOperations(), ",").c_str(),
               lastLoadedVersion.GetVersionId());
        mReopenType = INCONSISTENT_SCHEMA_REOPEN;
        return;
    }

    if (onDiskVersion.GetTimestamp() < lastLoadedVersion.GetTimestamp())
    {
        IE_LOG(WARN, "new version with smaller timestamp, "
               "[%ld] in target version [%d], [%ld] in current version [%d]. "
               "new version may be rollback index!",
               onDiskVersion.GetTimestamp(), onDiskVersion.GetVersionId(),
               lastLoadedVersion.GetTimestamp(),
               lastLoadedVersion.GetVersionId());
        mReopenType = INDEX_ROLL_BACK_REOPEN;
        return;
    }

    ValidateDeploySegments(partitionData->GetRootDirectory(),
                           onDiskVersion - lastLoadedVersion);

    mReopenType = NORMAL_REOPEN;
    if (mForceReopen)
    {
        mReopenType = FORCE_REOPEN;
    }
    mReopenIncVersion = onDiskVersion;
}

void ReopenDecider::ValidateDeploySegments(
    const DirectoryPtr& rootDir, const Version& version) const
{
    if (mOnlineConfig.IsValidateIndexEnabled())
    {
        DeployIndexValidator::ValidateDeploySegments(rootDir, version);
    }
}

bool ReopenDecider::NeedReclaimReaderMem(
        const AttributeMetrics* attributeMetrics) const
{
    assert(attributeMetrics);
    
    int64_t curReaderReclaimableBytes = 
        attributeMetrics->GetCurReaderReclaimableBytesValue();
    int64_t maxCurReaderReclaimableMem = mOnlineConfig.maxCurReaderReclaimableMem << 20;
    if (curReaderReclaimableBytes >= maxCurReaderReclaimableMem)
    {
        IE_LOG(INFO, "trigger threshold [%ld]," 
               "reopen to reclaim unused reader memory [%ld]",
               maxCurReaderReclaimableMem, curReaderReclaimableBytes);

        return true;
    }
    return false;
}

bool ReopenDecider::NeedSwitchFlushRtSegments(
        const OnlinePartitionReaderPtr& onlineReader,
        const PartitionDataPtr& partitionData)
{
    if (!onlineReader || !partitionData)
    {
        return false;
    }

    segmentid_t lastValidLinkRtSegId =
        partitionData->GetLastValidRtSegmentInLinkDirectory();
    if (lastValidLinkRtSegId == INVALID_SEGMENTID)
    {
        return false;
    }

    const PartitionVersion& partVersion = onlineReader->GetPartitionVersion();
    return lastValidLinkRtSegId > partVersion.GetLastLinkRtSegmentId();
}

bool ReopenDecider::GetNewOnDiskVersion(const PartitionDataPtr& partitionData,
                                        versionid_t reopenVersionId,
                                        index_base::Version& onDiskVersion) const
{
    assert(partitionData);
    if (reopenVersionId != -1 && reopenVersionId <= partitionData->GetOnDiskVersion().GetVersionId())
    {
        return false;
    }
    DirectoryPtr rootDirectory = partitionData->GetRootDirectory();
    VersionLoader::GetVersion(rootDirectory, onDiskVersion, reopenVersionId);
    return onDiskVersion.GetVersionId() > partitionData->GetOnDiskVersion().GetVersionId();
}

IE_NAMESPACE_END(partition);

