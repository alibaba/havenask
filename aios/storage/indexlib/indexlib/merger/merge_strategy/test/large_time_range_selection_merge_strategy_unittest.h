#pragma once

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_strategy/large_time_range_selection_merge_strategy.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class LargeTimeRangeSelectionMergeStrategyTest : public INDEXLIB_TESTBASE
{
public:
    LargeTimeRangeSelectionMergeStrategyTest();
    ~LargeTimeRangeSelectionMergeStrategyTest();

    DECLARE_CLASS_NAME(LargeTimeRangeSelectionMergeStrategyTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const std::string& mergeStrategyParams, bool optmize = false);
    void MakeSegmentMergeInfos(const std::string& str, index_base::SegmentMergeInfos& segMergeInfos);
    void CheckMergeTask(const std::string& expectResults, const MergeTask& task);
    void InnerTestLargeTimeRangeSelectionMergeTask(const std::string& originalSegs, const std::string& mergeParams,
                                                   const std::string& expectMergeSegs, bool optimizeMerge);
    std::string TaskToString(const MergeTask& task);

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LargeTimeRangeSelectionMergeStrategyTest, TestSimpleProcess);
}} // namespace indexlib::merger
