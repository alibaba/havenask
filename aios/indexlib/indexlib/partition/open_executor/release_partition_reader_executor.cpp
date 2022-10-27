#include <autil/Lock.h>
#include "indexlib/partition/open_executor/release_partition_reader_executor.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/common/executor_scheduler.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/partition/partition_data_creator.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, ReleasePartitionReaderExecutor);

IndexPartitionReaderPtr ReleasePartitionReaderExecutor::CreateRealtimeReader(
        ExecutorResource& resource)
{
    //TODO: make code readable
    //partition data not use on disk segment
    Version version = resource.mPartitionDataHolder.Get()->GetOnDiskVersion();
    version.Clear();
    //building partition data can auto scan rt partition and join partition
    BuildingPartitionDataPtr realtimePartitionData = 
        PartitionDataCreator::CreateBuildingPartitionData(
                resource.mFileSystem, resource.mSchema, resource.mOptions,
                resource.mPartitionMemController, resource.mDumpSegmentContainer,
                version, nullptr, "", InMemorySegmentPtr(),
                util::CounterMapPtr(), resource.mPluginManager);

    segmentid_t lastValidLinkRtSegId =
        realtimePartitionData->GetLastValidRtSegmentInLinkDirectory();
    OnlinePartitionReaderPtr reader(new partition::OnlinePartitionReader(
                    resource.mOptions, resource.mSchema, resource.mSearchCache,
                    &resource.mOnlinePartMetrics, lastValidLinkRtSegId));
    reader->Open(realtimePartitionData);
    return reader;
}

void ReleasePartitionReaderExecutor::CleanResource(ExecutorResource& resource)
{
    IE_LOG(DEBUG, "resource clean begin");
    ScopedLock lock(*resource.mCleanerLock);
    resource.mResourceCleaner->Execute();
    IE_LOG(DEBUG, "resource clean end");
}

bool ReleasePartitionReaderExecutor::Execute(ExecutorResource& resource) 
{
    IE_LOG(INFO, "release partition reader executor begin");
    mLastLoadedIncVersion = resource.mLoadedIncVersion;
    resource.mReader.reset();
    if (resource.mWriter)
    {
        resource.mWriter->Reset();
    }
    IndexPartitionReaderPtr reader = CreateRealtimeReader(resource);
    InMemorySegmentPtr orgInMemSegment;
    if (resource.mNeedInheritInMemorySegment)
    {
        orgInMemSegment = resource.mPartitionDataHolder.Get()->GetInMemorySegment();
    }
    resource.mPartitionDataHolder.Reset();
    CleanResource(resource);
    resource.mPartitionDataHolder.Reset(PartitionDataCreator::CreateBuildingPartitionData(
            resource.mFileSystem, resource.mRtSchema, resource.mOptions,
            resource.mPartitionMemController, resource.mDumpSegmentContainer, 
            resource.mLoadedIncVersion, resource.mOnlinePartMetrics.GetMetricProvider(),
            "", orgInMemSegment, util::CounterMapPtr(), resource.mPluginManager));
    reader.reset();
    assert(resource.mReaderContainer->Size() == 0);
    resource.mLoadedIncVersion = INVALID_VERSION;
    IE_LOG(INFO, "release partition reader executor end");
    return true;
}

IE_NAMESPACE_END(partition);

