#include "indexlib/table/normal_table/index_task/merger/BalanceTreeMergeStrategy.h"

#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/test/MergeTestHelper.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "unittest/unittest.h"

namespace indexlibv2::table {
class BalanceTreeMergeStrategyTest : public TESTBASE
{
public:
    BalanceTreeMergeStrategyTest() = default;
    ~BalanceTreeMergeStrategyTest() = default;

    using SegmentMergeInfo = BalanceTreeMergeStrategy::SegmentMergeInfo;
    using SegmentQueue = BalanceTreeMergeStrategy::SegmentQueue;
    using FakeSegmentInfos = std::vector<FakeSegmentInfo>;

public:
    void setUp() override
    {
        _tabletSchema = table::NormalTabletSchemaMaker::Make("nid:int64", "nid:primarykey64:nid", "", "");
    }
    void tearDown() override {};

private:
    void CheckMergePlan(const std::shared_ptr<MergePlan>& mergePlan, size_t idx,
                        const std::vector<segmentid_t>& srcSegmentIds);

private:
    std::shared_ptr<config::ITabletSchema> _tabletSchema;
};

void BalanceTreeMergeStrategyTest::CheckMergePlan(const std::shared_ptr<MergePlan>& mergePlan, size_t idx,
                                                  const std::vector<segmentid_t>& srcSegmentIds)
{
    auto& plan = mergePlan->GetSegmentMergePlan(idx);
    ASSERT_EQ(srcSegmentIds.size(), plan.GetSrcSegmentCount());
    for (size_t i = 0; i < srcSegmentIds.size(); i++) {
        ASSERT_EQ(srcSegmentIds[i], plan.GetSrcSegmentId(i));
    }
    ASSERT_EQ(1u, plan.GetTargetSegmentCount());
}

TEST_F(BalanceTreeMergeStrategyTest, TestSimpleProcess)
{
    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 100, .deleteDocCount = 60},
                                 {.id = 1, .isMerged = true, .docCount = 100, .deleteDocCount = 40},
                                 {.id = 3, .isMerged = true, .docCount = 5, .deleteDocCount = 2},
                                 {.id = 4, .isMerged = true, .docCount = 20, .deleteDocCount = 0},
                                 {.id = 5, .isMerged = true, .docCount = 10, .deleteDocCount = 7},
                                 {.id = 6, .isMerged = true, .docCount = 15, .deleteDocCount = 0}};
    auto context = MergeTestHelper::CreateContext(segInfos);
    context->TEST_SetTabletSchema(_tabletSchema);
    auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    std::string mergeParam = "conflict-segment-number=2;base-doc-count=10;max-doc-count=50;conflict-delete-percent=50";
    mergeConfig.TEST_SetMergeStrategyStr("balance_tree");
    mergeConfig.TEST_SetMergeStrategyParameterStr(mergeParam);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    BalanceTreeMergeStrategy mergeStrategy;
    auto mergePlan = mergeStrategy.CreateMergePlan(context.get()).second;
    ASSERT_EQ(2u, mergePlan->Size());
    CheckMergePlan(mergePlan, 0, {3, 4, 5, 6});
    CheckMergePlan(mergePlan, 1, {0});
}

TEST_F(BalanceTreeMergeStrategyTest, TestExtractParams)
{
    BalanceTreeMergeStrategy strategy;
    auto [st, param] = strategy.TEST_ExtractParams("");
    ASSERT_TRUE(st.IsOK());
    EXPECT_EQ(BalanceTreeMergeParams::DEFAULT_BASE_DOC_COUNT, param.baseDocCount);
    EXPECT_EQ(BalanceTreeMergeParams::DEFAULT_CONFLICT_SEGMENT_NUM, param.conflictSegmentNum);
    EXPECT_EQ(BalanceTreeMergeParams::DEFAULT_CONFLICT_DEL_PERCENT, param.conflictDelPercent);
    EXPECT_EQ(BalanceTreeMergeParams::DEFAULT_MAX_DOC_COUNT, param.maxDocCount);
    EXPECT_EQ(BalanceTreeMergeParams::DEFAULT_MAX_VALID_DOC_COUNT, param.maxValidDocCount);

    const std::string validParam = "conflict-segment-number=2;base-doc-count=102400;max-doc-count=1024000;conflict-"
                                   "delete-percent=80;max-valid-doc-count=20000000";
    std::tie(st, param) = strategy.TEST_ExtractParams(validParam);
    ASSERT_TRUE(st.IsOK());
    ASSERT_EQ(102400, param.baseDocCount);
    ASSERT_EQ(2, param.conflictSegmentNum);
    ASSERT_EQ(80, param.conflictDelPercent);
    ASSERT_EQ(1024000, param.maxDocCount);
    ASSERT_EQ(20000000, param.maxValidDocCount);

    const std::string inValidParam = "conflict-segment-number=2;base-doc-count=102400;max-doc-count=1024000;conflict-"
                                     "delete-percent=80;invalid-field:0";
    std::tie(st, param) = strategy.TEST_ExtractParams(inValidParam);
    ASSERT_FALSE(st.IsOK());
    ASSERT_EQ(102400, param.baseDocCount);
    ASSERT_EQ(2, param.conflictSegmentNum);
    ASSERT_EQ(80, param.conflictDelPercent);
    ASSERT_EQ(1024000, param.maxDocCount);
    ASSERT_EQ(BalanceTreeMergeParams::DEFAULT_MAX_VALID_DOC_COUNT, param.maxValidDocCount);
}

TEST_F(BalanceTreeMergeStrategyTest, TestCreateMergePlanAbnormal)
{
    FakeSegmentInfos emptySegmentMergeInfos;
    auto context = MergeTestHelper::CreateContext(emptySegmentMergeInfos);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    BalanceTreeMergeStrategy strategy;
    auto [st, mergePlan] = strategy.CreateMergePlan(context.get());
    ASSERT_TRUE(st.IsOK());
    ASSERT_EQ(0, mergePlan->Size());

    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 1, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 2, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 3, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 5, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10},
                                 {.id = 6, .isMerged = false, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10}};
    std::string levelInfoStr = "hash_mod,2,4:7,8;5,6|0;3,4|0;1,2";
    context = MergeTestHelper::CreateContextWithLevelInfo(segInfos, levelInfoStr);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    std::tie(st, mergePlan) = strategy.CreateMergePlan(context.get());
    ASSERT_FALSE(st.IsOK());

    levelInfoStr = "sequence,2,4:7,8;5,6|0;3,4|0;1,2";
    context = MergeTestHelper::CreateContextWithLevelInfo(segInfos, levelInfoStr);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    const std::string inValidParam = "conflict-segment-number=2;base-doc-count=102400;max-doc-count=1024000;conflict-"
                                     "delete-percent=80;invalid-field:0";
    mergeConfig.TEST_SetMergeStrategyStr("balance_tree");
    mergeConfig.TEST_SetMergeStrategyParameterStr(inValidParam);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    std::tie(st, mergePlan) = strategy.CreateMergePlan(context.get());
    ASSERT_FALSE(st.IsOK());
}

TEST_F(BalanceTreeMergeStrategyTest, TestCollectSegmentMergeInfos)
{
    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 1, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 50},
                                 {.id = 2, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 3, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 20},
                                 {.id = 5, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10},
                                 {.id = 6, .isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 10}};
    auto context = MergeTestHelper::CreateContext(segInfos);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    BalanceTreeMergeStrategy strategy;
    auto segmentMergeInfos = strategy.CollectSegmentMergeInfos(context->GetTabletData());
    ASSERT_EQ(6, segmentMergeInfos.size());
}

TEST_F(BalanceTreeMergeStrategyTest, TestTryAddSegmentMergePlan)
{
    BalanceTreeMergeStrategy strategy;
    auto mergePlan = std::make_shared<MergePlan>(/*name=*/MERGE_PLAN, /*type=*/MERGE_PLAN);
    bool isConflictDelPercent = true;
    SegmentMergePlan segmentMergePlan;
    strategy.TryAddSegmentMergePlan(mergePlan, segmentMergePlan, isConflictDelPercent);
    ASSERT_EQ(0, mergePlan->Size());

    segmentMergePlan.AddSrcSegment(/*segId*/ 1);
    strategy.TryAddSegmentMergePlan(mergePlan, segmentMergePlan, isConflictDelPercent);
    ASSERT_EQ(1, mergePlan->Size());

    isConflictDelPercent = false;
    segmentMergePlan.AddSrcSegment(/*segId*/ 2);
    strategy.TryAddSegmentMergePlan(mergePlan, segmentMergePlan, isConflictDelPercent);
    ASSERT_EQ(2, mergePlan->Size());
}

TEST_F(BalanceTreeMergeStrategyTest, TestCreateTopLayerMergePlan)
{
    std::vector<SegmentMergeInfo> segmentMergeInfos;
    SegmentMergeInfo normalInfo = {/*meaning less segId*/ -1, /*docCnt*/ 50, /*delDocCnt*/ 10};
    SegmentMergeInfo normalInfoWithDelPercent = {/*meaning less segId*/ -1, /*docCnt*/ 50, /*delDocCnt*/ 26};
    SegmentMergeInfo maxInfo = {/*meaning less segId*/ -1, /*docCnt*/ 162, /*delDocCnt*/ 26};
    SegmentMergeInfo maxInfoWithDelPercent1 = {/*segId*/ 10, /*docCnt*/ 162, /*delDocCnt*/ 90};
    SegmentMergeInfo maxInfoWithDelPercent2 = {/*segId*/ 100, /*docCnt*/ 164, /*delDocCnt*/ 90};
    SegmentMergeInfo maxInfoWithDelPercentWithValidDocCnt = {/*meaning less segId*/ -1, /*docCnt*/ 1000,
                                                             /*delDocCnt*/ 600};
    segmentMergeInfos.emplace_back(std::move(normalInfo));
    segmentMergeInfos.emplace_back(std::move(normalInfoWithDelPercent));
    segmentMergeInfos.emplace_back(std::move(maxInfo));
    segmentMergeInfos.emplace_back(std::move(maxInfoWithDelPercent1)); // will be in merge plan
    segmentMergeInfos.emplace_back(std::move(maxInfoWithDelPercent2)); // will be in merge plan
    segmentMergeInfos.emplace_back(std::move(maxInfoWithDelPercentWithValidDocCnt));

    auto mergePlan = std::make_shared<MergePlan>(/*name=*/MERGE_PLAN, /*type=*/MERGE_PLAN);

    BalanceTreeMergeParams param;
    param.baseDocCount = 10;
    param.conflictSegmentNum = 2;
    param.conflictDelPercent = 50;
    param.maxDocCount = 160;
    param.maxValidDocCount = 100;
    BalanceTreeMergeStrategy strategy;
    strategy._params = param;
    strategy.CreateTopLayerMergePlan(mergePlan, segmentMergeInfos);
    EXPECT_EQ(1, mergePlan->Size());
    auto segmentMergePlan = mergePlan->MutableSegmentMergePlan(0);
    ASSERT_EQ(1, segmentMergePlan->GetSrcSegmentCount());
    ASSERT_EQ(10, segmentMergePlan->GetSrcSegmentId(0));
    // segmentMergePlan = mergePlan->MutableSegmentMergePlan(1);
    // ASSERT_EQ(1, segmentMergePlan->GetSrcSegmentCount());
    // ASSERT_EQ(100, segmentMergePlan->GetSrcSegmentId(1));
}

TEST_F(BalanceTreeMergeStrategyTest, TestGetLayer)
{
    BalanceTreeMergeParams param;
    param.baseDocCount = 10;
    param.conflictSegmentNum = 2;
    param.conflictDelPercent = 70;
    param.maxDocCount = 160;
    param.maxValidDocCount = 160;
    BalanceTreeMergeStrategy strategy;
    strategy._params = param;

    ASSERT_EQ(-1, strategy.GetLayer(param.maxDocCount + 1));
    // layer | scope
    // 0     | [0 ~ baseDocCount (10)]
    // 1     | [11 ~ 20]
    // 2     | [21 ~ 40]
    // 3     | [41 ~ 80]
    // 4     | [81 ~ 160]
    // -1    | [161 ~ ]
    ASSERT_EQ(-1, strategy.GetLayer(161));
    ASSERT_EQ(-1, strategy.GetLayer(200));
    ASSERT_EQ(0, strategy.GetLayer(0));
    ASSERT_EQ(0, strategy.GetLayer(10));
    ASSERT_EQ(1, strategy.GetLayer(11));
    ASSERT_EQ(1, strategy.GetLayer(20));
    ASSERT_EQ(2, strategy.GetLayer(21));
    ASSERT_EQ(2, strategy.GetLayer(40));
    ASSERT_EQ(3, strategy.GetLayer(41));
    ASSERT_EQ(3, strategy.GetLayer(80));
    ASSERT_EQ(4, strategy.GetLayer(81));
    ASSERT_EQ(4, strategy.GetLayer(160));
}

TEST_F(BalanceTreeMergeStrategyTest, TestTryInsertBalanceTree)
{
    BalanceTreeMergeParams param;
    param.baseDocCount = 10;
    param.conflictSegmentNum = 2;
    param.conflictDelPercent = 70;
    param.maxDocCount = 160;
    param.maxValidDocCount = 160;
    BalanceTreeMergeStrategy strategy;
    strategy._params = param;

    std::vector<SegmentQueue> balanceTree;
    SegmentMergeInfo info;

    info.docCount = 161;
    ASSERT_EQ(-1, strategy.TryInsertBalanceTree(info, balanceTree));
    ASSERT_EQ(0, balanceTree.size());
    std::vector<SegmentMergeInfo> infos;
    for (size_t i = 0; i < 170; ++i) {
        SegmentMergeInfo info;
        info.docCount = i;
        auto layerId = strategy.TryInsertBalanceTree(info, balanceTree);
        if (i <= 10) {
            ASSERT_EQ(0, layerId);
        } else if (i <= 20) {
            ASSERT_EQ(1, layerId);

        } else if (i <= 40) {
            ASSERT_EQ(2, layerId);

        } else if (i <= 80) {
            ASSERT_EQ(3, layerId);

        } else if (i <= 160) {
            ASSERT_EQ(4, layerId);
        } else {
            ASSERT_EQ(-1, layerId);
        }
    }
    ASSERT_EQ(-1, strategy.TryInsertBalanceTree(info, balanceTree));
    ASSERT_EQ(5, balanceTree.size());
}

TEST_F(BalanceTreeMergeStrategyTest, TestTrySetNextTreeLevel)
{
    BalanceTreeMergeStrategy strategy;
    int32_t minLayerId = 10;
    // invalid merge id, return current
    ASSERT_EQ(5, strategy.TrySetNextTreeLevel(/*merged*/ -1, /*current*/ 5, minLayerId));
    ASSERT_EQ(10, minLayerId);
    // equal to current, return current
    ASSERT_EQ(5, strategy.TrySetNextTreeLevel(/*merged*/ 5, /*current*/ 5, minLayerId));
    ASSERT_EQ(10, minLayerId);
    // greater than current, return current
    ASSERT_EQ(5, strategy.TrySetNextTreeLevel(/*merged*/ 6, /*current*/ 5, minLayerId));
    ASSERT_EQ(10, minLayerId);
    // less than current, get the min(merged, minLayerId) - 1
    ASSERT_EQ(1, strategy.TrySetNextTreeLevel(/*merged*/ 2, /*current*/ 5, minLayerId));
    ASSERT_EQ(2, minLayerId);
    ASSERT_EQ(1, strategy.TrySetNextTreeLevel(/*merged*/ 3, /*current*/ 5, minLayerId));
    ASSERT_EQ(2, minLayerId);

    ASSERT_EQ(-1, strategy.TrySetNextTreeLevel(/*merged*/ 0, /*current*/ 5, minLayerId));
    ASSERT_EQ(0, minLayerId);
}

TEST_F(BalanceTreeMergeStrategyTest, TestHasLargeDelPercentSegment)
{
    BalanceTreeMergeParams param;
    param.baseDocCount = 10;
    param.conflictSegmentNum = 2;
    param.conflictDelPercent = 70;
    param.maxDocCount = 160;
    param.maxValidDocCount = 160;
    BalanceTreeMergeStrategy strategy;
    strategy._params = param;

    std::vector<SegmentMergeInfo> noDelPercentInfos = {
        {/*segmentId = */ 0, /*docCount = */ 1, /*deletedDocCount = */ 0},
        {/*segmentId = */ 1, /*docCount = */ 100, /*deletedDocCount = */ 10},
        {/*segmentId = */ 2, /*docCount = */ 200, /*deletedDocCount = */ 20}};
    SegmentQueue segmentQueue;
    for (const auto& segmentMergeInfo : noDelPercentInfos) {
        segmentQueue.push(segmentMergeInfo);
    }
    ASSERT_FALSE(strategy.HasLargeDelPercentSegment(segmentQueue));

    std::vector<SegmentMergeInfo> delPercentInfos = {{/*segmentId*/ 0, /*docCount*/ 1, /*deletedDocCount*/ 0},
                                                     {/*segmentId*/ 1, /*docCount*/ 100, /*deletedDocCount*/ 10},
                                                     {/*segmentId*/ 2, /*docCount*/ 200, /*deletedDocCount*/ 150}};
    SegmentQueue tmp;
    segmentQueue.swap(tmp);
    for (const auto& segmentMergeInfo : delPercentInfos) {
        segmentQueue.push(segmentMergeInfo);
    }
    ASSERT_TRUE(strategy.HasLargeDelPercentSegment(segmentQueue));
}

TEST_F(BalanceTreeMergeStrategyTest, TestLargerThanDelPercent)
{
    BalanceTreeMergeStrategy strategy;
    SegmentMergeInfo info;
    info.docCount = 0;

    ASSERT_FALSE(strategy.LargerThanDelPercent(info));

    strategy._params.conflictDelPercent = 50;
    info.docCount = 10;
    info.deletedDocCount = 4;

    ASSERT_FALSE(strategy.LargerThanDelPercent(info));

    info.deletedDocCount = 5;
    ASSERT_FALSE(strategy.LargerThanDelPercent(info));

    info.deletedDocCount = 6;
    ASSERT_TRUE(strategy.LargerThanDelPercent(info));
}
} // namespace indexlibv2::table
