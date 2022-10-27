#include "indexlib/partition/open_executor/dump_container_flush_executor.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"
#include "indexlib/misc/metric_provider.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/open_executor/reopen_partition_reader_executor.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/online_partition_writer.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, DumpContainerFlushExecutor);

DumpContainerFlushExecutor::DumpContainerFlushExecutor(
    autil::RecursiveThreadMutex* dataLock, const string& partitionName)
    : OpenExecutor(partitionName)
    , mDataLock(dataLock)
{
}

DumpContainerFlushExecutor::~DumpContainerFlushExecutor() 
{
}

bool DumpContainerFlushExecutor::Execute(ExecutorResource& resource)
{
    IE_LOG(INFO, "flush dump container begin");
    DumpSegmentContainerPtr dumpSegmentContainer = resource.mDumpSegmentContainer;
    size_t dumpSize = dumpSegmentContainer->GetUnDumpedSegmentSize();
    bool hasDumpedSegment = false;
    for (size_t i = 0; i < dumpSize; i++)
    {
        NormalSegmentDumpItemPtr item = dumpSegmentContainer->GetOneSegmentItemToDump();
        if (!item)
        {
            break;
        }
        if (!item->DumpWithMemLimit())
        {
            if (hasDumpedSegment)
            {
                resource.mPartitionDataHolder.Get()->CommitVersion();
                ReopenNewSegment(resource);
            }
            return false;
        }
        hasDumpedSegment = true;
    }
    if (hasDumpedSegment)
    {
        resource.mPartitionDataHolder.Get()->CommitVersion();
        ReopenNewSegment(resource);
    }
    IE_LOG(INFO, "flush dump container end");
    return true;
}

void DumpContainerFlushExecutor::ReopenNewSegment(ExecutorResource& resource)
{
    ScopedLock lock(*mDataLock);
    PartitionDataPtr partData = resource.mPartitionDataHolder.Get();
    if (resource.mWriter)
    {
        partData->CreateNewSegment();        
    }
    ReopenPartitionReaderExecutor::InitReader(
            resource, partData, PatchLoaderPtr(), mPartitionName);
    PartitionModifierPtr modifier = 
        PartitionModifierCreator::CreateInplaceModifier(
                resource.mSchema, resource.mReader);
    if (resource.mWriter)
    {
        resource.mWriter->ReOpenNewSegment(modifier);        
    }
    resource.mReader->EnableAccessCountors();
}

IE_NAMESPACE_END(partition);

