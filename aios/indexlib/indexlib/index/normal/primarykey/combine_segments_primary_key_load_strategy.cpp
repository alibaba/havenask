#include "indexlib/index/normal/primarykey/combine_segments_primary_key_load_strategy.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, CombineSegmentsPrimaryKeyLoadStrategy);

void CombineSegmentsPrimaryKeyLoadStrategy::CreatePrimaryKeyLoadPlans(
        const index_base::PartitionDataPtr& partitionData,
        PrimaryKeyLoadPlanVector& plans)
{
    DeletionMapReaderPtr deletionMapReader = partitionData->GetDeletionMapReader();

    PrimaryKeyLoadPlanPtr plan;
    docid_t baseDocid = 0;
    bool isRtPlan = false;

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
        bool isRtSegment = RealtimeSegmentDirectory::IsRtSegmentId(
                segData.GetSegmentId());
        if (NeedCreateNewPlan(isRtSegment, plan, isRtPlan))
        {
            PushBackPlan(plan, plans);
            plan.reset(new PrimaryKeyLoadPlan());
            plan->Init(baseDocid, mPkConfig);
            isRtPlan = isRtSegment;
        }
        plan->AddSegmentData(segData, GetDeleteCount(segData, deletionMapReader));
        baseDocid += docCount;
        builtSegIter->MoveToNext();
    }
    PushBackPlan(plan, plans);
}

size_t CombineSegmentsPrimaryKeyLoadStrategy::GetDeleteCount(
        const index_base::SegmentData& segData,
        const DeletionMapReaderPtr& deletionMapReader)
{
    if (deletionMapReader)
    {
        return deletionMapReader->GetDeletedDocCount(
                segData.GetSegmentId());
    }    
    return 0;
}

void CombineSegmentsPrimaryKeyLoadStrategy::PushBackPlan(
        const PrimaryKeyLoadPlanPtr& plan, 
        PrimaryKeyLoadPlanVector& plans)
{
    if (plan && plan->GetDocCount() > 0)
    {
        plans.push_back(plan);
    }
}

bool CombineSegmentsPrimaryKeyLoadStrategy::NeedCreateNewPlan(
        bool isRtSegment, const PrimaryKeyLoadPlanPtr& plan, bool isRtPlan)
{
    if (!plan)
    {
        return true;
    }

    if (isRtPlan != isRtSegment)
    {
        return true;
    }
    //deal with segment doc count > mMaxDocCount
    size_t currentDocCount = plan->GetDocCount();
    if (currentDocCount == 0)
    {
        return false;
    }

    if (currentDocCount >= mMaxDocCount)
    {
        return true;
    }
    return false;
}

IE_NAMESPACE_END(index);

