#ifndef __INDEXLIB_TIMESERIESMERGESTRATEGYTEST_H
#define __INDEXLIB_TIMESERIESMERGESTRATEGYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/merge_strategy/time_series_merge_strategy.h"

IE_NAMESPACE_BEGIN(merger);

class TimeSeriesMergeStrategyTest : public INDEXLIB_TESTBASE
{
public:
    TimeSeriesMergeStrategyTest();
    ~TimeSeriesMergeStrategyTest();

    DECLARE_CLASS_NAME(TimeSeriesMergeStrategyTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestInputOutputLimit();
    void TestRangeLimit();
    void TestException();
    void TestMergeWithTag();

private:
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const std::string& mergeStrategyParams,
                              bool optmize = false);
    void MakeSegmentMergeInfos(const std::string& str, index_base::SegmentMergeInfos& segMergeInfos);
    void CheckMergeTask(const std::string& expectResults, const MergeTask& task);
    void InnerTestTimeSeriesMergeTask(const std::string& originalSegs,
                                    const std::string& mergeParams,
                                    const std::string& expectMergeSegs,
                                    bool optimizeMerge);
    std::string TaskToString(const MergeTask& task);

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TimeSeriesMergeStrategyTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(TimeSeriesMergeStrategyTest, TestInputOutputLimit);
INDEXLIB_UNIT_TEST_CASE(TimeSeriesMergeStrategyTest, TestRangeLimit);
INDEXLIB_UNIT_TEST_CASE(TimeSeriesMergeStrategyTest, TestException);
INDEXLIB_UNIT_TEST_CASE(TimeSeriesMergeStrategyTest, TestMergeWithTag);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_TIMESERIESMERGESTRATEGYTEST_H
