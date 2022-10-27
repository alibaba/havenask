#include "indexlib/partition/open_executor/dump_segment_executor.h"
#include "indexlib/partition/open_executor/reopen_partition_reader_executor.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, DumpSegmentExecutor);

DumpSegmentExecutor::DumpSegmentExecutor(const string& partitionName,
    bool reInitReaderAndWriter)
    : OpenExecutor(partitionName)
    , mReInitReaderAndWriter(reInitReaderAndWriter)
    , mHasSkipInitReaderAndWriter(false)
{
}

DumpSegmentExecutor::~DumpSegmentExecutor() 
{
}

bool DumpSegmentExecutor::Execute(ExecutorResource& resource)
{
    IE_LOG(INFO, "dump segment executor begin");
    mHasSkipInitReaderAndWriter = false;
    if (!resource.mWriter || resource.mWriter->IsClosed())
    {
        return true;
    }

    if (!resource.mWriter->DumpSegmentWithMemLimit())
    {
        return false;
    }

    PartitionDataPtr partData = resource.mPartitionDataHolder.Get();
    partData->CreateNewSegment();
    
    if (mReInitReaderAndWriter)
    {
        ReopenPartitionReaderExecutor::InitReader(
            resource, partData, PatchLoaderPtr(), mPartitionName);
        PartitionModifierPtr modifier = 
            PartitionModifierCreator::CreateInplaceModifier(
                resource.mSchema, resource.mReader);
        resource.mWriter->ReOpenNewSegment(modifier);        
        resource.mReader->EnableAccessCountors();
    }
    else
    {
        mHasSkipInitReaderAndWriter = true;
        IE_LOG(INFO, "skip init reader and writer");
    }
    IE_LOG(INFO, "dump segment executor end");
    return true;
}

void DumpSegmentExecutor::Drop(ExecutorResource& resource)
{
    if (mHasSkipInitReaderAndWriter)
    {
        if (!resource.mWriter || resource.mWriter->IsClosed())
        {
            return;
        }
        IE_LOG(INFO, "drop reinit reader and writer");
        PartitionDataPtr partData = resource.mPartitionDataHolder.Get();
        partData->CreateNewSegment();
        ReopenPartitionReaderExecutor::InitReader(
            resource, partData, PatchLoaderPtr(), mPartitionName);
        PartitionModifierPtr modifier = 
            PartitionModifierCreator::CreateInplaceModifier(
                resource.mSchema, resource.mReader);
        resource.mWriter->ReOpenNewSegment(modifier);        
        resource.mReader->EnableAccessCountors();
    }
}

IE_NAMESPACE_END(partition);

