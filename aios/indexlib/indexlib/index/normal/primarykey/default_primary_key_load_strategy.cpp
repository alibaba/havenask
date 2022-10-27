#include "indexlib/index/normal/primarykey/default_primary_key_load_strategy.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DefaultPrimaryKeyLoadStrategy);

DefaultPrimaryKeyLoadStrategy::~DefaultPrimaryKeyLoadStrategy() 
{
}

void DefaultPrimaryKeyLoadStrategy::CreatePrimaryKeyLoadPlans(
        const index_base::PartitionDataPtr& partitionData,
        PrimaryKeyLoadPlanVector& plans)
{
    DeletionMapReaderPtr deletionMapReader;
    try
    {
        deletionMapReader = partitionData->GetDeletionMapReader();
    }
    catch (UnSupportedException& e)
    {
        deletionMapReader.reset();
        IE_LOG(INFO, "Merger reclaim document will use MergePartitionData, "
               "which not support get deletionMapReader.");
    }
    
    docid_t baseDocid = 0;
    PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    SegmentIteratorPtr builtSegIter = segIter->CreateIterator(SIT_BUILT);
    while (builtSegIter->IsValid())
    {
        SegmentData segData = builtSegIter->GetSegmentData();
        size_t docCount = segData.GetSegmentInfo().docCount;
        if (docCount == 0)
        {
            builtSegIter->MoveToNext();
            continue;
        }
        PrimaryKeyLoadPlanPtr plan(new PrimaryKeyLoadPlan());
        plan->Init(baseDocid, mPkConfig);
        size_t deleteDocCount = 0;
        if (deletionMapReader)
        {
            deleteDocCount = deletionMapReader->GetDeletedDocCount(
                    segData.GetSegmentId());
        }
        
        plan->AddSegmentData(segData, deleteDocCount);
        baseDocid += docCount;
        plans.push_back(plan);
        builtSegIter->MoveToNext();
    }
}

IE_NAMESPACE_END(index);

