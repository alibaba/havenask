#include "indexlib/table/kv_table/index_task/KeyValueOptimizeMergeStrategy.h"

#include "autil/StringUtil.h"
#include "indexlib/framework/SegmentTopologyInfo.h"
#include "indexlib/table/index_task/merger/test/MergeTestHelper.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace table {

class KeyValueOptimizeMergeStrategyTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

    std::shared_ptr<framework::IndexTaskContext> PrepareContext(const std::string& topology, const size_t levelNum,
                                                                const size_t shardCount,
                                                                const std::string& levelStr) const
    {
        std::vector<FakeSegmentInfo> segInfos;
        std::vector<std::vector<segmentid_t>> segmentIdVecs;
        autil::StringUtil::fromString(levelStr, segmentIdVecs, ",", ";");
        assert(segmentIdVecs.size() == levelNum);
        for (size_t level = 0; level < levelNum; level++) {
            for (segmentid_t segmentId : segmentIdVecs[level]) {
                if (segmentId != INVALID_SEGMENTID) {
                    segInfos.push_back({.id = segmentId, .isMerged = level != 0});
                }
            }
        }
        std::string levelMetaStr = topology + "," + autil::StringUtil::toString(shardCount) + "," +
                                   autil::StringUtil::toString(levelNum) + ":";
        return MergeTestHelper::CreateContextWithLevelInfo(segInfos,
                                                           levelMetaStr + levelStr + (levelNum == 0 ? "-1" : ""));
    }
    void CheckMergePlan(const std::shared_ptr<MergePlan>& mergePlan, size_t idx,
                        const std::vector<segmentid_t>& inPlanIds, const std::vector<size_t>& targetPosition)
    {
        auto& plan = mergePlan->GetSegmentMergePlan(idx);
        ASSERT_EQ(inPlanIds.size(), plan.GetSrcSegmentCount());
        for (size_t i = 0; i < inPlanIds.size(); i++) {
            ASSERT_EQ(inPlanIds[i], plan.GetSrcSegmentId(i));
        }

        const auto& targetVersion = mergePlan->GetTargetVersion();
        auto levelInfo = targetVersion.GetSegmentDescriptions()->GetLevelInfo();
        ASSERT_EQ(1u, plan.GetTargetSegmentCount());
        auto targetSegId = plan.GetTargetSegmentId(0);
        uint32_t levelIdx, shardIdx;
        ASSERT_TRUE(levelInfo->FindPosition(targetSegId, levelIdx, shardIdx));
        ASSERT_EQ(targetPosition[0], levelIdx);
        ASSERT_EQ(targetPosition[1], shardIdx);
        bool isBottomLevel = (levelIdx == (levelInfo->GetLevelCount() - 1));
        ASSERT_EQ((bool)targetPosition[2], isBottomLevel);
    }

private:
};

TEST_F(KeyValueOptimizeMergeStrategyTest, TestSimpleProcess)
{
    {
        auto context = PrepareContext("hash_mod", 3, 2, "536870917,536870918,536870919,536870920;5,6;3,4");
        KeyValueOptimizeMergeStrategy strategy;
        auto mergePlan = strategy.CreateMergePlan(context.get()).second;
        ASSERT_EQ(2u, mergePlan->Size());
        CheckMergePlan(mergePlan, 0, {3, 5, 536870917, 536870918, 536870919, 536870920}, {2, 0, 1});
        CheckMergePlan(mergePlan, 1, {4, 6, 536870917, 536870918, 536870919, 536870920}, {2, 1, 1});
    }
    {
        // One shard
        auto context = PrepareContext("hash_mod", 3, 1, "536870917,536870918,536870919,536870920;4;3");
        KeyValueOptimizeMergeStrategy strategy;
        auto mergePlan = strategy.CreateMergePlan(context.get()).second;
        ASSERT_EQ(1u, mergePlan->Size());
        CheckMergePlan(mergePlan, 0, {3, 4, 536870917, 536870918, 536870919, 536870920}, {2, 0, 1});
    }
}

TEST_F(KeyValueOptimizeMergeStrategyTest, TestInvalidSegmentId)
{
    auto context = PrepareContext("hash_mod", 3, 2, "536870917,536870918,536870919,536870920;-1,4;3,-1");
    KeyValueOptimizeMergeStrategy strategy;
    auto mergePlan = strategy.CreateMergePlan(context.get()).second;
    ASSERT_EQ(2u, mergePlan->Size());
    CheckMergePlan(mergePlan, 0, {3, 536870917, 536870918, 536870919, 536870920}, {2, 0, 1});
    CheckMergePlan(mergePlan, 1, {4, 536870917, 536870918, 536870919, 536870920}, {2, 1, 1});
}

TEST_F(KeyValueOptimizeMergeStrategyTest, TestOnlyL0HasSegments)
{
    auto context = PrepareContext("hash_mod", 3, 2, "536870917,536870918,536870919,536870920;-1,-1;-1,-1");
    KeyValueOptimizeMergeStrategy strategy;
    auto mergePlan = strategy.CreateMergePlan(context.get()).second;
    ASSERT_EQ(2u, mergePlan->Size());
    CheckMergePlan(mergePlan, 0, {536870917, 536870918, 536870919, 536870920}, {2, 0, 1});
    CheckMergePlan(mergePlan, 1, {536870917, 536870918, 536870919, 536870920}, {2, 1, 1});
}

TEST_F(KeyValueOptimizeMergeStrategyTest, TestCheckLevelInfo)
{
    {
        // Invalid levelInfo topology
        auto context = PrepareContext("sequence", 3, 2, "536870917,536870918,536870919,536870920;5,6;3,4");
        KeyValueOptimizeMergeStrategy strategy;
        ASSERT_FALSE(strategy.CreateMergePlan(context.get()).first.IsOK());
    }
    {
        // Invalid levelMeta topology
        auto context = PrepareContext("hash_mod", 3, 2, "536870917,536870918,536870919,536870920;-1,4;3,-1");
        auto levelInfo = context->GetTabletData()->GetOnDiskVersion().GetSegmentDescriptions()->GetLevelInfo();
        levelInfo->levelMetas[1].topology = framework::topo_sequence;
        KeyValueOptimizeMergeStrategy strategy;
        bool needMerge = false;
        ASSERT_FALSE(strategy.CheckLevelInfo(levelInfo, needMerge));
    }
    {
        // LevelCount == 0
        auto context = PrepareContext("hash_mod", 0, 2, "");
        KeyValueOptimizeMergeStrategy strategy;
        ASSERT_FALSE(strategy.CreateMergePlan(context.get()).first.IsOK());
    }
    {
        // ShardCount == 0
        auto context = PrepareContext("sequence", 3, 2, "-1;-1,-1;-1,-1");
        KeyValueOptimizeMergeStrategy strategy;
        ASSERT_FALSE(strategy.CreateMergePlan(context.get()).first.IsOK());
    }
    {
        // Do not need merge
        auto context = PrepareContext("hash_mod", 3, 2, "-1;-1,-1;3,4");
        KeyValueOptimizeMergeStrategy strategy;
        auto [status, mergePlan] = strategy.CreateMergePlan(context.get());
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(mergePlan->Size() == 0);
    }
}
}} // namespace indexlibv2::table
