#include "indexlib/partition/open_executor/reclaim_rt_index_executor.h"
#include "indexlib/misc/metric_provider.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/realtime_partition_data_reclaimer.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index/online_join_policy.h"

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, ReclaimRtIndexExecutor);

bool ReclaimRtIndexExecutor::Execute(ExecutorResource& resource)
{
    misc::ScopeLatencyReporter scopeTime(
        resource.mOnlinePartMetrics.GetReclaimRtIndexLatencyMetric().get());

    IE_LOG(INFO, "reclaim realtime index begin");
    if (!resource.mForceReopen)
    {
        mOriginalPartitionData = resource.mPartitionDataHolder.Get();
    }

    // TODO inherit InMemorySegment
    resource.mPartitionDataHolder.Reset(PartitionDataCreator::CreateBuildingPartitionData(
            resource.mFileSystem, resource.mRtSchema, resource.mOptions,
            resource.mPartitionMemController, resource.mDumpSegmentContainer, 
            resource.mLoadedIncVersion, resource.mOnlinePartMetrics.GetMetricProvider(),
            "", InMemorySegmentPtr(), util::CounterMapPtr(), resource.mPluginManager));

    OnlineJoinPolicy joinPolicy(resource.mIncVersion,
                                resource.mRtSchema->GetTableType());
    int64_t reclaimTimestamp = joinPolicy.GetReclaimRtTimestamp();
    RealtimePartitionDataReclaimerPtr reclaimer(
        new RealtimePartitionDataReclaimer(resource.mRtSchema, resource.mOptions));
    reclaimer->Reclaim(reclaimTimestamp, resource.mPartitionDataHolder.Get());
    IE_LOG(INFO, "reclaim realtime index end");
    return true;
}

void ReclaimRtIndexExecutor::Drop(ExecutorResource& resource)
{
    if (resource.mForceReopen)
    {
        return;
    }
    resource.mPartitionDataHolder.Reset(mOriginalPartitionData);
    OnlineSegmentDirectoryPtr segDir = DYNAMIC_POINTER_CAST(OnlineSegmentDirectory,
            mOriginalPartitionData->GetSegmentDirectory());
    SegmentDirectoryPtr rtSegmentDir = segDir->GetRtSegmentDirectory();
    rtSegmentDir->RollbackToCurrentVersion();
    mOriginalPartitionData.reset();
}

IE_NAMESPACE_END(partition);

