#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/merger/test/merge_task_maker.h"

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);

using namespace std;
class MergeTaskTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(MergeTaskTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void CreateMergeTaskData(MergeTask& task, uint32_t planItemCount,
                             uint32_t segmentCountPerPlanItem)
    {
        for (uint32_t k = 0; k < planItemCount; ++k)
        {
            MergePlan item;     
            for (uint32_t i = k * segmentCountPerPlanItem; 
                 i < (k + 1) *segmentCountPerPlanItem; ++i)
            {
                SegmentMergeInfo segMergeInfo;
                segMergeInfo.segmentId = i;
                item.AddSegment(segMergeInfo);
            }
            task.AddPlan(item);
        }
    }

    void TestCaseForAddOneItem()
    {
        MergeTask task;
        CreateMergeTaskData(task, 1, 3);

        INDEXLIB_TEST_EQUAL((size_t)1, task.GetPlanCount());

        const MergePlan& ref = task[0];
        INDEXLIB_TEST_EQUAL((size_t)3, ref.GetSegmentCount());

        MergePlan::Iterator segIter = ref.CreateIterator();
        segmentid_t segId = 0;
        while(segIter.HasNext())
        {
            INDEXLIB_TEST_EQUAL(segId, segIter.Next());
            segId++;
        }
    }

    void TestCaseForAddMultiItems()
    {
        MergeTask task;
        const uint32_t PLAN_ITEM_COUNT = 5;

        CreateMergeTaskData(task, PLAN_ITEM_COUNT, 3);

        INDEXLIB_TEST_EQUAL(task.GetPlanCount(), PLAN_ITEM_COUNT);

        for (uint32_t k = 0; k < PLAN_ITEM_COUNT; ++k)
        {
            const MergePlan& ref = task[k];
            INDEXLIB_TEST_EQUAL((size_t)3, ref.GetSegmentCount());

            MergePlan::Iterator segIter = ref.CreateIterator();
            segmentid_t segId = 3 * k;
            while(segIter.HasNext())
            {
                INDEXLIB_TEST_EQUAL(segId, segIter.Next());
                segId++;
            }
        }
    }

};

INDEXLIB_UNIT_TEST_CASE(MergeTaskTest, TestCaseForAddOneItem);
INDEXLIB_UNIT_TEST_CASE(MergeTaskTest, TestCaseForAddMultiItems);

IE_NAMESPACE_END(merger);
