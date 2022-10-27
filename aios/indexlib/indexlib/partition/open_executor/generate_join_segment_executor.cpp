#include "indexlib/partition/open_executor/generate_join_segment_executor.h"
#include "indexlib/misc/metric_provider.h"
#include "indexlib/partition/join_segment_writer.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/index_base/segment/online_segment_directory.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, GenerateJoinSegmentExecutor);

bool GenerateJoinSegmentExecutor::Execute(ExecutorResource& resource)
{
    misc::ScopeLatencyReporter scopeTime(
        resource.mOnlinePartMetrics.GetGenerateJoinSegmentLatencyMetric().get());
    IE_LOG(INFO, "generate join segment begin");
    if (!resource.mForceReopen)
    {
        mOriginalPartitionData = resource.mPartitionDataHolder.Get();
    }
    InMemorySegmentPtr orgInMemSegment;
    if (resource.mNeedInheritInMemorySegment)
    {
        orgInMemSegment = resource.mPartitionDataHolder.Get()->GetInMemorySegment();
    }
    resource.mPartitionDataHolder.Reset(PartitionDataCreator::CreateBuildingPartitionData(
            resource.mFileSystem, resource.mRtSchema, resource.mOptions,
            resource.mPartitionMemController, resource.mDumpSegmentContainer, 
            resource.mLoadedIncVersion, resource.mOnlinePartMetrics.GetMetricProvider(),
            "", orgInMemSegment, resource.mCounterMap, resource.mPluginManager));
    bool ret = mJoinSegWriter->Join() && mJoinSegWriter->Dump(resource.mPartitionDataHolder.Get());
    mJoinSegWriter.reset();
    IE_LOG(INFO, "generate join segment end");
    return ret;
}
void GenerateJoinSegmentExecutor::Drop(ExecutorResource& resource)
{
    if (resource.mForceReopen)
    {
        return;
    }
    resource.mPartitionDataHolder.Reset(mOriginalPartitionData);
    OnlineSegmentDirectoryPtr segDir = DYNAMIC_POINTER_CAST(OnlineSegmentDirectory,
            mOriginalPartitionData->GetSegmentDirectory());
    SegmentDirectoryPtr joinSegmentDir = segDir->GetJoinSegmentDirectory();
    joinSegmentDir->RollbackToCurrentVersion();
    mOriginalPartitionData.reset();
    mJoinSegWriter.reset();
}

IE_NAMESPACE_END(partition);

