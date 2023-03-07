#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/multi_part_counter_merger.h"
#include "indexlib/util/counter/counter_base.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/counter/state_counter.h"

using namespace std;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(merger);

class MultiPartCounterMergerTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(MultiPartCounterMergerTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForMultiPartition()
    {
        string test_data1 = MakeData(100, 10);
        string test_data2 = MakeData(200, 20);

        MultiPartCounterMerger merger(false);
        merger.Merge(test_data1);

        INDEXLIB_TEST_EQUAL(100, merger.mDocCount);
        INDEXLIB_TEST_EQUAL(10, merger.mDeletedDocCount);
        INDEXLIB_TEST_EQUAL(100, merger.GetStateCounter("partitionDocCount", false)->Get());
        INDEXLIB_TEST_EQUAL(10, merger.GetStateCounter("deletedDocCount", false)->Get());

        merger.Merge(test_data2);

        INDEXLIB_TEST_EQUAL(300, merger.mDocCount);
        INDEXLIB_TEST_EQUAL(30, merger.mDeletedDocCount);
        INDEXLIB_TEST_EQUAL(300, merger.GetStateCounter("partitionDocCount", false)->Get());
        INDEXLIB_TEST_EQUAL(30, merger.GetStateCounter("deletedDocCount", false)->Get());
    }

    void TestCaseForMultiPartitionWithSub()
    {
        string test_data1 = MakeData(100, 10, 101, 11);
        string test_data2 = MakeData(200, 20, 201, 21);

        MultiPartCounterMerger merger(true);
        merger.Merge(test_data1);

        INDEXLIB_TEST_EQUAL(100, merger.mDocCount);
        INDEXLIB_TEST_EQUAL(10, merger.mDeletedDocCount);
        INDEXLIB_TEST_EQUAL(100, merger.GetStateCounter("partitionDocCount", false)->Get());
        INDEXLIB_TEST_EQUAL(10, merger.GetStateCounter("deletedDocCount", false)->Get());

        INDEXLIB_TEST_EQUAL(101, merger.mSubDocCount);
        INDEXLIB_TEST_EQUAL(11, merger.mSubDeletedDocCount);
        INDEXLIB_TEST_EQUAL(101, merger.GetStateCounter("partitionDocCount", true)->Get());
        INDEXLIB_TEST_EQUAL(11, merger.GetStateCounter("deletedDocCount", true)->Get());

        merger.Merge(test_data2);

        INDEXLIB_TEST_EQUAL(300, merger.mDocCount);
        INDEXLIB_TEST_EQUAL(30, merger.mDeletedDocCount);
        INDEXLIB_TEST_EQUAL(300, merger.GetStateCounter("partitionDocCount", false)->Get());
        INDEXLIB_TEST_EQUAL(30, merger.GetStateCounter("deletedDocCount", false)->Get());

        INDEXLIB_TEST_EQUAL(302, merger.mSubDocCount);
        INDEXLIB_TEST_EQUAL(32, merger.mSubDeletedDocCount);
        INDEXLIB_TEST_EQUAL(302, merger.GetStateCounter("partitionDocCount", true)->Get());
        INDEXLIB_TEST_EQUAL(32, merger.GetStateCounter("deletedDocCount", true)->Get());
    }

    string MakeData(int64_t docCount, int64_t deletedDocCount) {
        CounterMap counterMap;
        counterMap.GetStateCounter("offline.partitionDocCount")->Set(docCount);
        counterMap.GetStateCounter("offline.deletedDocCount")->Set(deletedDocCount);
        return counterMap.ToJsonString();
    }

    string MakeData(int64_t docCount, int64_t deletedDocCount, int64_t subDocCount, int64_t subDeletedDocCount) {
        CounterMap counterMap;
        counterMap.GetStateCounter("offline.partitionDocCount")->Set(docCount);
        counterMap.GetStateCounter("offline.deletedDocCount")->Set(deletedDocCount);
        counterMap.GetStateCounter("offline.sub_partitionDocCount")->Set(subDocCount);
        counterMap.GetStateCounter("offline.sub_deletedDocCount")->Set(subDeletedDocCount);
        return counterMap.ToJsonString();
    }
};

INDEXLIB_UNIT_TEST_CASE(MultiPartCounterMergerTest, TestCaseForMultiPartition);
INDEXLIB_UNIT_TEST_CASE(MultiPartCounterMergerTest, TestCaseForMultiPartitionWithSub);

IE_NAMESPACE_END(merger);
