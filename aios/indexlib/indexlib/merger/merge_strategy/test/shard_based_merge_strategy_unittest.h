#ifndef __INDEXLIB_SHARDBASEDMERGESTRATEGYTEST_H
#define __INDEXLIB_SHARDBASEDMERGESTRATEGYTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/merge_task.h"

IE_NAMESPACE_BEGIN(merger);

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
    void TestMergeMultiColumn();
    void TestMergeWithAbsentSegment();
    void TestMergeWithCursor();
    void TestOnlyL0HasSegments();
    void TestSpecialSpaceAmplification();
    void TestMultiPlanInOneShard();
    void TestCalculateLevelPercent();
private:
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::LevelInfo& levelInfo,
                              const std::string& spaceAmplification);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestMergeMultiColumn);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestMergeWithAbsentSegment);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestMergeWithCursor);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestOnlyL0HasSegments);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestCalculateLevelPercent);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestSpecialSpaceAmplification);
INDEXLIB_UNIT_TEST_CASE(ShardBasedMergeStrategyTest, TestMultiPlanInOneShard);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_SHARDBASEDMERGESTRATEGYTEST_H
