#pragma once

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class ShardBasedMergeStrategyTest : public INDEXLIB_TESTBASE
{
public:
    ShardBasedMergeStrategyTest();
    ~ShardBasedMergeStrategyTest();

    DECLARE_CLASS_NAME(ShardBasedMergeStrategyTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestMergeMultiShard();
    void TestMergeWithAbsentSegment();
    void TestMergeWithCursor();
    void TestOnlyL0HasSegments();
    void TestSpecialSpaceAmplification();
    void TestMultiPlanInOneShard();
    void TestCalculateLevelPercent();

private:
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const indexlibv2::framework::LevelInfo& levelInfo, const std::string& spaceAmplification);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestMergeMultiShard);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestMergeWithAbsentSegment);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestMergeWithCursor);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestOnlyL0HasSegments);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestCalculateLevelPercent);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestSpecialSpaceAmplification);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestMultiPlanInOneShard);
}} // namespace indexlib::merger
