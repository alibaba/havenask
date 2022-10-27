#include "indexlib/merger/merge_strategy/shard_based_merge_strategy.h"
#include "indexlib/merger/merge_strategy/test/shard_based_merge_strategy_unittest.h"
#include "indexlib/merger/test/merge_helper.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, ShardBasedMergeStrategyTest);

ShardBasedMergeStrategyTest::ShardBasedMergeStrategyTest()
{
}

ShardBasedMergeStrategyTest::~ShardBasedMergeStrategyTest()
{
}

void ShardBasedMergeStrategyTest::CaseSetUp()
{
}

void ShardBasedMergeStrategyTest::CaseTearDown()
{
}

void ShardBasedMergeStrategyTest::TestSimpleProcess()
{
    string levelStr = "hash_mod,2#5:10,6:10;2:20,3:20;0:50,1:50";
    auto mergeInfo = MergeHelper::PrepareMergeInfo(levelStr);
    MergeTask task = CreateMergeTask(mergeInfo.first, mergeInfo.second, "1.5");
    ASSERT_EQ(2u, task.GetPlanCount());
    ASSERT_TRUE(MergeHelper::CheckMergePlan(task[0], {0, 2, 5, 6}, {2, 0, 1}));
    ASSERT_TRUE(MergeHelper::CheckMergePlan(task[1], {3, 5, 6}, {1, 1, 0}));
}

void ShardBasedMergeStrategyTest::TestMergeMultiColumn()
{
    string levelStr = "hash_mod,2#5:10,6:10;2:20,3:20;0:50,1:50";
    auto mergeInfo = MergeHelper::PrepareMergeInfo(levelStr);
    MergeTask task = CreateMergeTask(mergeInfo.first, mergeInfo.second, "1.1");
    ASSERT_EQ(2u, task.GetPlanCount());
    ASSERT_TRUE(MergeHelper::CheckMergePlan(task[0], {0, 2, 5, 6}, {2, 0, 1}));
    ASSERT_TRUE(MergeHelper::CheckMergePlan(task[1], {1, 3, 5, 6}, {2, 1, 1}));
}

void ShardBasedMergeStrategyTest::TestMergeWithAbsentSegment()
{
    {
        string levelStr = "hash_mod,2#5:10,6:10;-1,3:20;0:50,1:50";
        auto mergeInfo = MergeHelper::PrepareMergeInfo(levelStr);
        MergeTask task = CreateMergeTask(mergeInfo.first, mergeInfo.second, "1.1");
        ASSERT_EQ(2u, task.GetPlanCount());
        ASSERT_TRUE(MergeHelper::CheckMergePlan(task[0], {0, 5, 6}, {2, 0, 1}));
        ASSERT_TRUE(MergeHelper::CheckMergePlan(task[1], {1, 3, 5, 6}, {2, 1, 1}));
    }
    {
        string levelStr = "hash_mod,2#-1;-1,-1;-1,3:20;0:50,1:50";
        auto mergeInfo = MergeHelper::PrepareMergeInfo(levelStr);
        MergeTask task = CreateMergeTask(mergeInfo.first, mergeInfo.second, "1.1");
        ASSERT_EQ(1u, task.GetPlanCount());
        ASSERT_TRUE(MergeHelper::CheckMergePlan(task[0], {1, 3}, {3, 1, 1}));
    }
}

void ShardBasedMergeStrategyTest::TestMergeWithCursor()
{
    string levelStr = "hash_mod,2#5:10,6:10;2:20,3:20|1;0:50,1:50";
    auto mergeInfo = MergeHelper::PrepareMergeInfo(levelStr);
    MergeTask task = CreateMergeTask(mergeInfo.first, mergeInfo.second, "1.5");
    ASSERT_EQ(2u, task.GetPlanCount());
    ASSERT_TRUE(MergeHelper::CheckMergePlan(task[0], {2, 5, 6}, {1, 0, 0}));
    ASSERT_TRUE(MergeHelper::CheckMergePlan(task[1], {1, 3, 5, 6}, {2, 1, 1}));
}

void ShardBasedMergeStrategyTest::TestOnlyL0HasSegments()
{
    string levelStr = "hash_mod,2#1:10;-1,-1;-1,-1";
    auto mergeInfo = MergeHelper::PrepareMergeInfo(levelStr);
    MergeTask task = CreateMergeTask(mergeInfo.first, mergeInfo.second, "1.5");
    ASSERT_EQ(2u, task.GetPlanCount());
    ASSERT_TRUE(MergeHelper::CheckMergePlan(task[0], {1}, {2, 0, 1}));
    ASSERT_TRUE(MergeHelper::CheckMergePlan(task[1], {1}, {2, 1, 1}));
}

void ShardBasedMergeStrategyTest::TestSpecialSpaceAmplification()
{
    {
        string levelStr = "hash_mod,2#5:1,6:1;2:1,3:1|1;0:50,1:50";
        auto mergeInfo = MergeHelper::PrepareMergeInfo(levelStr);
        MergeTask task = CreateMergeTask(mergeInfo.first, mergeInfo.second, "1.0");
        ASSERT_EQ(2u, task.GetPlanCount());
        ASSERT_TRUE(MergeHelper::CheckMergePlan(task[0], {0, 2, 5, 6}, {2, 0, 1}));
        ASSERT_TRUE(MergeHelper::CheckMergePlan(task[1], {1, 3, 5, 6}, {2, 1, 1}));
    }
    {
        string levelStr = "hash_mod,2#5:100,6:100;2:100,3:100|1;0:50,1:50";
        auto mergeInfo = MergeHelper::PrepareMergeInfo(levelStr);
        MergeTask task = CreateMergeTask(mergeInfo.first, mergeInfo.second, "9999.0");
        ASSERT_EQ(2u, task.GetPlanCount());
        ASSERT_TRUE(MergeHelper::CheckMergePlan(task[0], {0, 2, 5, 6}, {2, 0, 1}));
        ASSERT_TRUE(MergeHelper::CheckMergePlan(task[1], {1, 3, 5, 6}, {2, 1, 1}));
    }
}

void ShardBasedMergeStrategyTest::TestMultiPlanInOneShard()
{
    string levelStr = "hash_mod,2#7:1,8:1;5:1,6:1|0;3:40,4:40|0;1:50,2:50";
    auto mergeInfo = MergeHelper::PrepareMergeInfo(levelStr);
    // NOTE : level precent is 50%
    MergeTask task = CreateMergeTask(mergeInfo.first, mergeInfo.second, "1.9");
    ASSERT_EQ(3u, task.GetPlanCount());
    ASSERT_TRUE(MergeHelper::CheckMergePlan(task[0], {5, 7, 8}, {1, 0, 0}));
    ASSERT_TRUE(MergeHelper::CheckMergePlan(task[1], {1, 3}, {3, 0, 1}));
    ASSERT_TRUE(MergeHelper::CheckMergePlan(task[2], {6, 7, 8}, {1, 1, 0}));
}

void ShardBasedMergeStrategyTest::TestCalculateLevelPercent()
{
    SegmentDirectoryPtr segDir;
    config::IndexPartitionSchemaPtr schema;
    ShardBasedMergeStrategy ms(segDir, schema);

    ASSERT_EQ(0u, ms.CalculateLevelPercent(0, 3));
    ASSERT_EQ(0u, ms.CalculateLevelPercent(1.0, 3));
    ASSERT_EQ(99u, ms.CalculateLevelPercent(9.0, 3));
    uint32_t percent = ms.CalculateLevelPercent(2.0, 3);
    ASSERT_LT(0, percent);
    ASSERT_LT(percent, 99u);
}

MergeTask ShardBasedMergeStrategyTest::CreateMergeTask(
        const SegmentMergeInfos& segMergeInfos,
        const LevelInfo& levelInfo,
        const string& spaceAmplification)
{
    SegmentDirectoryPtr segDir;
    config::IndexPartitionSchemaPtr schema;
    ShardBasedMergeStrategy strategy(segDir, schema);
    MergeStrategyParameter param;
    param.strategyConditions = string("space_amplification=") + spaceAmplification;
    strategy.SetParameter(param);
    return strategy.CreateMergeTask(segMergeInfos, levelInfo);
}

IE_NAMESPACE_END(merger);

