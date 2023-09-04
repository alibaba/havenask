#include "indexlib/merger/test/merge_task_item_unittest.h"
using namespace std;

using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeTaskItemTest);

MergeTaskItemTest::MergeTaskItemTest() {}

MergeTaskItemTest::~MergeTaskItemTest() {}

void MergeTaskItemTest::CaseSetUp() {}

void MergeTaskItemTest::CaseTearDown() {}

void MergeTaskItemTest::TestSimpleProcess()
{
    {
        MergeTaskItem item1(1, "abc", "123", true);
        item1.mCost = 111.111;
        string jsonStr = ToJsonString(item1);
        MergeTaskItem item2;
        FromJsonString(item2, jsonStr);
        INDEXLIB_TEST_EQUAL(item1.mMergePlanIdx, item2.mMergePlanIdx);
        INDEXLIB_TEST_EQUAL(item1.mMergeType, item2.mMergeType);
        INDEXLIB_TEST_EQUAL(item1.mName, item2.mName);
        INDEXLIB_TEST_EQUAL(item1.mIsSubItem, item2.mIsSubItem);
        INDEXLIB_TEST_EQUAL(item1.mCost, item2.mCost);
        INDEXLIB_TEST_EQUAL(item1.mParallelMergeItem, item2.mParallelMergeItem);
        INDEXLIB_TEST_EQUAL(-1, item1.mParallelMergeItem.mId);
        INDEXLIB_TEST_EQUAL(0, item1.mParallelMergeItem.mResourceIds.size());
    }
    {
        ParallelMergeItem mergeItem(2, 0.4, 1, 200);
        mergeItem.AddResource(3);
        mergeItem.AddResource(4);
        MergeTaskItem item1(2, "abc", "123", true);
        item1.mCost = 111.111;
        item1.SetParallelMergeItem(mergeItem);
        string jsonStr = ToJsonString(item1);
        cout << "mergeItemStr is: [" << jsonStr << "]" << endl;
        MergeTaskItem item2;
        FromJsonString(item2, jsonStr);
        INDEXLIB_TEST_EQUAL(item1.mMergePlanIdx, item2.mMergePlanIdx);
        INDEXLIB_TEST_EQUAL(item1.mMergeType, item2.mMergeType);
        INDEXLIB_TEST_EQUAL(item1.mName, item2.mName);
        INDEXLIB_TEST_EQUAL(item1.mIsSubItem, item2.mIsSubItem);
        INDEXLIB_TEST_EQUAL(item1.mCost, item2.mCost);
        INDEXLIB_TEST_EQUAL(item1.mParallelMergeItem, item2.mParallelMergeItem);
        INDEXLIB_TEST_EQUAL(2, item1.mParallelMergeItem.mId);
        ASSERT_EQ((float)0.4, item1.mParallelMergeItem.mDataRatio);
        INDEXLIB_TEST_EQUAL(item1.mParallelMergeItem.mDataRatio, item2.mParallelMergeItem.mDataRatio);
        INDEXLIB_TEST_EQUAL(item1.mParallelMergeItem.mResourceIds.size(), item2.mParallelMergeItem.mResourceIds.size());
        for (size_t i = 0; i < item1.mParallelMergeItem.mResourceIds.size(); i++) {
            ASSERT_EQ(item1.mParallelMergeItem.mResourceIds[i], item2.mParallelMergeItem.mResourceIds[i]);
        }
    }
}

void MergeTaskItemTest::TestGetCheckPointName()
{
    MergeTaskItem item(1, "abc", "123", true, 3);
    string checkPointStr = item.GetCheckPointName();
    string identifyStr = item.ToString();

    ParallelMergeItem mergeItem(0, 1.0, 1, 1);
    item.SetParallelMergeItem(mergeItem);
    ASSERT_EQ(checkPointStr, item.GetCheckPointName());
    ASSERT_EQ(identifyStr, item.ToString());

    mergeItem.SetTotalParallelCount(2);
    mergeItem.SetId(1);
    item.SetParallelMergeItem(mergeItem);
    ASSERT_NE(checkPointStr, item.GetCheckPointName());
    ASSERT_NE(identifyStr, item.ToString());
    ASSERT_EQ(string("MergePlan_1_abc_123-2-1_3_sub.checkpoint"), item.GetCheckPointName());
    ASSERT_EQ(string("MergePlan[1] abc [123-2-1] TargetSegmentIdx[3]"), item.ToString());
}
}} // namespace indexlib::merger
