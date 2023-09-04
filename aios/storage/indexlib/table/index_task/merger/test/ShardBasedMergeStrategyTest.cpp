#include "indexlib/table/index_task/merger/ShardBasedMergeStrategy.h"

#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/table/index_task/merger/test/MergeTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace table {
namespace {
using config::MergeStrategyParameter;
using FakeSegmentInfos = std::vector<FakeSegmentInfo>;
} // namespace
class ShardBasedMergeStrategyTest : public TESTBASE
{
public:
    ShardBasedMergeStrategyTest();
    ~ShardBasedMergeStrategyTest();

public:
    void setUp() override
    {
        std::string field = "string1:string;string2:string";
        _tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2");
    }
    void tearDown() override {}

private:
    void CheckMergePlan(const std::shared_ptr<MergePlan>& mergePlan, size_t idx,
                        const std::vector<segmentid_t>& inPlanIds, const std::vector<size_t>& targetPosition);

private:
    std::shared_ptr<config::ITabletSchema> _tabletSchema;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.table, ShardBasedMergeStrategyTest);

ShardBasedMergeStrategyTest::ShardBasedMergeStrategyTest() {}

ShardBasedMergeStrategyTest::~ShardBasedMergeStrategyTest() {}

TEST_F(ShardBasedMergeStrategyTest, TestSetParameter)
{
    config::MergeStrategyParameter param;
    std::string paramStr;

    {
        // empty config, return default value
        ShardBasedMergeStrategy strategy;
        strategy.SetParameter(param);
        ASSERT_EQ(strategy._spaceAmplification, 1.5);
        ASSERT_FALSE(strategy._disableLastLevelMerge);
    }
    {
        ShardBasedMergeStrategy strategy;
        paramStr = "space_amplification=1.2";
        param.SetLegacyString(paramStr);
        strategy.SetParameter(param);
        ASSERT_EQ(strategy._spaceAmplification, 1.2);
        ASSERT_FALSE(strategy._disableLastLevelMerge);
    }
    {
        ShardBasedMergeStrategy strategy;
        paramStr = "disable_last_level_merge=true";
        param.SetLegacyString(paramStr);
        strategy.SetParameter(param);
        ASSERT_EQ(strategy._spaceAmplification, 1.5);
        ASSERT_TRUE(strategy._disableLastLevelMerge);
    }
    {
        ShardBasedMergeStrategy strategy;
        paramStr = "space_amplification=1.2;disable_last_level_merge=true";
        param.SetLegacyString(paramStr);
        strategy.SetParameter(param);
        ASSERT_EQ(strategy._spaceAmplification, 1.2);
        ASSERT_TRUE(strategy._disableLastLevelMerge);
    }
    {
        ShardBasedMergeStrategy strategy;
        paramStr = "space_amplification=1.8;disable_last_level_merge=false";
        param.SetLegacyString(paramStr);
        strategy.SetParameter(param);
        ASSERT_EQ(strategy._spaceAmplification, 1.8);
        ASSERT_FALSE(strategy._disableLastLevelMerge);
    }
}

TEST_F(ShardBasedMergeStrategyTest, TestSimpleProcess)
{
    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 1, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 2, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 3, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 5, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10},
                                 {.id = 6, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10}};
    std::string levelInfoStr = "hash_mod,2,3:5,6;2,3;0,1";
    auto context = MergeTestHelper::CreateContextWithLevelInfo(segInfos, levelInfoStr);
    context->TEST_SetTabletSchema(_tabletSchema);
    auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    std::string strategyString = "space_amplification=1.5";
    mergeConfig.TEST_SetMergeStrategyStr("shard_based");
    mergeConfig.TEST_SetMergeStrategyParameterStr(strategyString);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    ShardBasedMergeStrategy mergeStrategy;
    auto mergePlan = mergeStrategy.CreateMergePlan(context.get()).second;
    ASSERT_EQ(2u, mergePlan->Size());
    CheckMergePlan(mergePlan, 0, {0, 2, 5, 6}, {2, 0, 1});
    CheckMergePlan(mergePlan, 1, {3, 5, 6}, {1, 1, 0});
}

TEST_F(ShardBasedMergeStrategyTest, TestMergeMultiShard)
{
    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 1, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 2, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 3, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 5, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10},
                                 {.id = 6, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10}};
    std::string levelInfoStr = "hash_mod,2,3:5,6;2,3;0,1";
    auto context = MergeTestHelper::CreateContextWithLevelInfo(segInfos, levelInfoStr);
    context->TEST_SetTabletSchema(_tabletSchema);
    auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    std::string strategyString = "space_amplification=1.1";
    mergeConfig.TEST_SetMergeStrategyStr("shard_based");
    mergeConfig.TEST_SetMergeStrategyParameterStr(strategyString);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    ShardBasedMergeStrategy mergeStrategy;
    auto mergePlan = mergeStrategy.CreateMergePlan(context.get()).second;
    ASSERT_EQ(2u, mergePlan->Size());
    CheckMergePlan(mergePlan, 0, {0, 2, 5, 6}, {2, 0, 1});
    CheckMergePlan(mergePlan, 1, {1, 3, 5, 6}, {2, 1, 1});
}

TEST_F(ShardBasedMergeStrategyTest, TestMergeMultiShardWithDisableLastLevelMerge)
{
    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 1, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 2, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 3, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 5, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10},
                                 {.id = 6, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10}};
    std::string levelInfoStr = "hash_mod,2,3:5,6;2,3;0,1";
    auto context = MergeTestHelper::CreateContextWithLevelInfo(segInfos, levelInfoStr);
    context->TEST_SetTabletSchema(_tabletSchema);
    auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    std::string strategyString = "space_amplification=1.1;disable_last_level_merge=true";
    mergeConfig.TEST_SetMergeStrategyStr("shard_based");
    mergeConfig.TEST_SetMergeStrategyParameterStr(strategyString);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    ShardBasedMergeStrategy mergeStrategy;
    auto mergePlan = mergeStrategy.CreateMergePlan(context.get()).second;
    ASSERT_EQ(2u, mergePlan->Size());
    CheckMergePlan(mergePlan, 0, {2, 5, 6}, {1, 0, 0});
    CheckMergePlan(mergePlan, 1, {3, 5, 6}, {1, 1, 0});
}

TEST_F(ShardBasedMergeStrategyTest, TestMergeWithAbsentSegment)
{
    {
        FakeSegmentInfos segInfos = {
            {.id = 0, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
            {.id = 1, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
            {.id = 3, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
            {.id = 5, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10},
            {.id = 6, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10}};
        std::string levelInfoStr = "hash_mod,2,3:5,6;-1,3;0,1";
        auto context = MergeTestHelper::CreateContextWithLevelInfo(segInfos, levelInfoStr);
        context->TEST_SetTabletSchema(_tabletSchema);
        auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
        std::string strategyString = "space_amplification=1.1";
        mergeConfig.TEST_SetMergeStrategyStr("shard_based");
        mergeConfig.TEST_SetMergeStrategyParameterStr(strategyString);
        ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
        ShardBasedMergeStrategy mergeStrategy;
        auto mergePlan = mergeStrategy.CreateMergePlan(context.get()).second;
        ASSERT_EQ(2u, mergePlan->Size());
        CheckMergePlan(mergePlan, 0, {0, 5, 6}, {2, 0, 1});
        CheckMergePlan(mergePlan, 1, {1, 3, 5, 6}, {2, 1, 1});
    }
    {
        FakeSegmentInfos segInfos = {
            {.id = 0, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
            {.id = 1, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
            {.id = 3, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20}};

        std::string levelInfoStr = "hash_mod,2,4:-1;-1,-1;-1,3;0,1";
        auto context = MergeTestHelper::CreateContextWithLevelInfo(segInfos, levelInfoStr);
        context->TEST_SetTabletSchema(_tabletSchema);
        auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
        std::string strategyString = "space_amplification=1.1";
        mergeConfig.TEST_SetMergeStrategyStr("shard_based");
        mergeConfig.TEST_SetMergeStrategyParameterStr(strategyString);
        ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
        ShardBasedMergeStrategy mergeStrategy;
        auto mergePlan = mergeStrategy.CreateMergePlan(context.get()).second;
        ASSERT_EQ(1u, mergePlan->Size());
        CheckMergePlan(mergePlan, 0, {1, 3}, {3, 1, 1});
    }
}

TEST_F(ShardBasedMergeStrategyTest, TestMergeWithCursor)
{
    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 1, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 2, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 3, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 5, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10},
                                 {.id = 6, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10}};
    std::string levelInfoStr = "hash_mod,2,3:5,6;2,3|1;0,1";
    auto context = MergeTestHelper::CreateContextWithLevelInfo(segInfos, levelInfoStr);
    context->TEST_SetTabletSchema(_tabletSchema);
    auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    std::string strategyString = "space_amplification=1.5";
    mergeConfig.TEST_SetMergeStrategyStr("shard_based");
    mergeConfig.TEST_SetMergeStrategyParameterStr(strategyString);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    ShardBasedMergeStrategy mergeStrategy;
    auto mergePlan = mergeStrategy.CreateMergePlan(context.get()).second;
    ASSERT_EQ(2u, mergePlan->Size());
    CheckMergePlan(mergePlan, 0, {2, 5, 6}, {1, 0, 0});
    CheckMergePlan(mergePlan, 1, {1, 3, 5, 6}, {2, 1, 1});
}

TEST_F(ShardBasedMergeStrategyTest, TestMergeWithCursorWithDisableLastLevelMerge)
{
    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 1, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 2, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 3, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 5, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10},
                                 {.id = 6, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10}};
    std::string levelInfoStr = "hash_mod,2,3:5,6;2,3|1;0,1";
    auto context = MergeTestHelper::CreateContextWithLevelInfo(segInfos, levelInfoStr);
    context->TEST_SetTabletSchema(_tabletSchema);
    auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    std::string strategyString = "space_amplification=1.5;disable_last_level_merge=true";
    mergeConfig.TEST_SetMergeStrategyStr("shard_based");
    mergeConfig.TEST_SetMergeStrategyParameterStr(strategyString);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    ShardBasedMergeStrategy mergeStrategy;
    auto mergePlan = mergeStrategy.CreateMergePlan(context.get()).second;
    ASSERT_EQ(2u, mergePlan->Size());
    CheckMergePlan(mergePlan, 0, {2, 5, 6}, {1, 0, 0});
    CheckMergePlan(mergePlan, 1, {3, 5, 6}, {1, 1, 0});
}

TEST_F(ShardBasedMergeStrategyTest, TestOnlyL0HasSegments)
{
    FakeSegmentInfos segInfos = {{.id = 1, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10}};
    std::string levelInfoStr = "hash_mod,2,3:1;-1,-1;-1,-1";
    auto context = MergeTestHelper::CreateContextWithLevelInfo(segInfos, levelInfoStr);
    context->TEST_SetTabletSchema(_tabletSchema);
    auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    std::string strategyString = "space_amplification=1.5";
    mergeConfig.TEST_SetMergeStrategyStr("shard_based");
    mergeConfig.TEST_SetMergeStrategyParameterStr(strategyString);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    ShardBasedMergeStrategy mergeStrategy;
    auto mergePlan = mergeStrategy.CreateMergePlan(context.get()).second;
    ASSERT_EQ(2u, mergePlan->Size());
    CheckMergePlan(mergePlan, 0, {1}, {2, 0, 1});
    CheckMergePlan(mergePlan, 1, {1}, {2, 1, 1});
}

TEST_F(ShardBasedMergeStrategyTest, TestSpecialSpaceAmplification)
{
    {
        FakeSegmentInfos segInfos = {
            {.id = 0, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
            {.id = 1, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
            {.id = 2, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 1},
            {.id = 3, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 1},
            {.id = 5, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 1},
            {.id = 6, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 1}};

        std::string levelInfoStr = "hash_mod,2,3:5,6;2,3|1;0,1";
        auto context = MergeTestHelper::CreateContextWithLevelInfo(segInfos, levelInfoStr);
        context->TEST_SetTabletSchema(_tabletSchema);
        auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
        std::string strategyString = "space_amplification=1.0";
        mergeConfig.TEST_SetMergeStrategyStr("shard_based");
        mergeConfig.TEST_SetMergeStrategyParameterStr(strategyString);
        ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
        ShardBasedMergeStrategy mergeStrategy;
        auto mergePlan = mergeStrategy.CreateMergePlan(context.get()).second;
        ASSERT_EQ(2u, mergePlan->Size());
        CheckMergePlan(mergePlan, 0, {0, 2, 5, 6}, {2, 0, 1});
        CheckMergePlan(mergePlan, 1, {1, 3, 5, 6}, {2, 1, 1});
    }
    {
        FakeSegmentInfos segInfos = {
            {.id = 0, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
            {.id = 1, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
            {.id = 2, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 100},
            {.id = 3, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 100},
            {.id = 5, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 100},
            {.id = 6, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 100}};

        std::string levelInfoStr = "hash_mod,2,3:5,6;2,3|1;0,1";
        auto context = MergeTestHelper::CreateContextWithLevelInfo(segInfos, levelInfoStr);
        context->TEST_SetTabletSchema(_tabletSchema);
        auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
        std::string strategyString = "space_amplification=9999.0";
        mergeConfig.TEST_SetMergeStrategyStr("shard_based");
        mergeConfig.TEST_SetMergeStrategyParameterStr(strategyString);
        ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
        ShardBasedMergeStrategy mergeStrategy;
        auto mergePlan = mergeStrategy.CreateMergePlan(context.get()).second;
        ASSERT_EQ(2u, mergePlan->Size());
        CheckMergePlan(mergePlan, 0, {0, 2, 5, 6}, {2, 0, 1});
        CheckMergePlan(mergePlan, 1, {1, 3, 5, 6}, {2, 1, 1});
    }
}

TEST_F(ShardBasedMergeStrategyTest, TestMultiPlanInOneShard)
{
    FakeSegmentInfos segInfos = {{.id = 1, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 2, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 3, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 40},
                                 {.id = 4, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 40},
                                 {.id = 5, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 1},
                                 {.id = 6, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 1},
                                 {.id = 7, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 1},
                                 {.id = 8, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 1}};
    std::string levelInfoStr = "hash_mod,2,4:7,8;5,6|0;3,4|0;1,2";
    auto context = MergeTestHelper::CreateContextWithLevelInfo(segInfos, levelInfoStr);
    context->TEST_SetTabletSchema(_tabletSchema);
    auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    std::string strategyString = "space_amplification=1.9";
    mergeConfig.TEST_SetMergeStrategyStr("shard_based");
    mergeConfig.TEST_SetMergeStrategyParameterStr(strategyString);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    ShardBasedMergeStrategy mergeStrategy;
    auto mergePlan = mergeStrategy.CreateMergePlan(context.get()).second;
    ASSERT_EQ(3u, mergePlan->Size());
    CheckMergePlan(mergePlan, 0, {5, 7, 8}, {1, 0, 0});
    CheckMergePlan(mergePlan, 1, {1, 3}, {3, 0, 1});
    CheckMergePlan(mergePlan, 2, {6, 7, 8}, {1, 1, 0});
}

TEST_F(ShardBasedMergeStrategyTest, TestCalculateLevelPercent)
{
    ShardBasedMergeStrategy ms;

    ASSERT_EQ(0u, ShardBasedMergeStrategy::CalculateLevelPercent(0, 3));
    ASSERT_EQ(0u, ShardBasedMergeStrategy::CalculateLevelPercent(1.0, 3));
    ASSERT_EQ(99u, ShardBasedMergeStrategy::CalculateLevelPercent(9.0, 3));
    uint32_t percent = ShardBasedMergeStrategy::CalculateLevelPercent(2.0, 3);
    ASSERT_LT(0, percent);
    ASSERT_LT(percent, 99u);
}

void ShardBasedMergeStrategyTest::CheckMergePlan(const std::shared_ptr<MergePlan>& mergePlan, size_t idx,
                                                 const std::vector<segmentid_t>& inPlanIds,
                                                 const std::vector<size_t>& targetPosition)
{
    auto& plan = mergePlan->GetSegmentMergePlan(idx);
    ASSERT_EQ(inPlanIds.size(), plan.GetSrcSegmentCount());
    for (size_t i = 0; i < inPlanIds.size(); i++) {
        ASSERT_EQ(inPlanIds[i], plan.GetSrcSegmentId(i));
    }
    ASSERT_EQ(1u, plan.GetTargetSegmentCount());
    auto targetSegId = plan.GetTargetSegmentId(0);
    const auto& targetVersion = mergePlan->GetTargetVersion();
    auto levelInfo = targetVersion.GetSegmentDescriptions()->GetLevelInfo();
    uint32_t levelIdx, shardIdx;
    ASSERT_TRUE(levelInfo->FindPosition(targetSegId, levelIdx, shardIdx));
    ASSERT_EQ(targetPosition[0], levelIdx);
    ASSERT_EQ(targetPosition[1], shardIdx);
    bool isBottomLevel = (levelIdx == (levelInfo->GetLevelCount() - 1));
    ASSERT_EQ((bool)targetPosition[2], isBottomLevel);
}

}} // namespace indexlibv2::table
