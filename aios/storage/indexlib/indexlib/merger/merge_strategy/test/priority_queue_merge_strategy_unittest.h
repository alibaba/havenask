#ifndef __INDEXLIB_PRIORITYQUEUEMERGESTRATEGYTEST_H
#define __INDEXLIB_PRIORITYQUEUEMERGESTRATEGYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_strategy/priority_queue_merge_strategy.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class PriorityQueueMergeStrategyTest : public INDEXLIB_TESTBASE
{
public:
    PriorityQueueMergeStrategyTest();
    ~PriorityQueueMergeStrategyTest();

    DECLARE_CLASS_NAME(PriorityQueueMergeStrategyTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSimpleProcess();
    void TestInputLimits();
    void TestOutputLimits();
    void TestSelectMergeSegments();
    void TestSetParameter();
    void TestMergeForOptimize();

    void TestMaxSegmentSize();
    void TestMaxValidSegmentSize();
    void TestMaxTotalMergeSize();
    void TestMaxMergedSegmentSize();
    void TestMaxTotalMergedSize();

private:
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const std::string& mergeStrategyParams, bool optmize = false);
    void MakeSegmentMergeInfos(const std::string& str, index_base::SegmentMergeInfos& segMergeInfos);
    void CheckMergeTask(const std::string& expectResults, const MergeTask& task);
    void InnerTestPriorityMergeTask(const std::string& originalSegs, const std::string& mergeParams,
                                    const std::string& expectMergeSegs, bool optimizeMerge);
    std::string TaskToString(const MergeTask& task);

    void InnerTestSetParameter(const std::string& mergeParams, bool badParam);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PriorityQueueMergeStrategyTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PriorityQueueMergeStrategyTest, TestInputLimits);
INDEXLIB_UNIT_TEST_CASE(PriorityQueueMergeStrategyTest, TestOutputLimits);
INDEXLIB_UNIT_TEST_CASE(PriorityQueueMergeStrategyTest, TestSelectMergeSegments);
INDEXLIB_UNIT_TEST_CASE(PriorityQueueMergeStrategyTest, TestSetParameter);
INDEXLIB_UNIT_TEST_CASE(PriorityQueueMergeStrategyTest, TestMergeForOptimize);
INDEXLIB_UNIT_TEST_CASE(PriorityQueueMergeStrategyTest, TestMaxSegmentSize);
INDEXLIB_UNIT_TEST_CASE(PriorityQueueMergeStrategyTest, TestMaxValidSegmentSize);
INDEXLIB_UNIT_TEST_CASE(PriorityQueueMergeStrategyTest, TestMaxTotalMergeSize);
INDEXLIB_UNIT_TEST_CASE(PriorityQueueMergeStrategyTest, TestMaxMergedSegmentSize);
INDEXLIB_UNIT_TEST_CASE(PriorityQueueMergeStrategyTest, TestMaxTotalMergedSize);
}} // namespace indexlib::merger

#endif //__INDEXLIB_PRIORITYQUEUEMERGESTRATEGYTEST_H
