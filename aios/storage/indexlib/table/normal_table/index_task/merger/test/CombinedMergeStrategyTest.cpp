#include "indexlib/table/normal_table/index_task/merger/CombinedMergeStrategy.h"

#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/mock/FakeSegment.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/test/MergeTestHelper.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/index_task/NormalTableCompactPlanCreator.h"
#include "indexlib/table/normal_table/index_task/merger/BalanceTreeMergeStrategy.h"
#include "indexlib/table/normal_table/index_task/merger/PriorityQueueMergeStrategy.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace table {

namespace {
using config::MergeStrategyParameter;
}

class CombinedMergeStrategyTest : public TESTBASE
{
public:
    CombinedMergeStrategyTest() {}
    ~CombinedMergeStrategyTest() {}

    using FakeSegmentInfos = std::vector<FakeSegmentInfo>;

private:
    void setUp() override
    {
        _tabletSchema = table::NormalTabletSchemaMaker::Make("nid:int64", "nid:primarykey64:nid", "", "");
    }
    void tearDown() override {}
    std::unique_ptr<MergeStrategy> CreateMergeStrategy(framework::IndexTaskContext* context);
    void CheckMergePlan(const std::shared_ptr<MergePlan>& mergePlan, size_t idx,
                        const std::vector<segmentid_t>& srcSegmentIds);

private:
    std::shared_ptr<config::ITabletSchema> _tabletSchema;
};

std::unique_ptr<MergeStrategy> CombinedMergeStrategyTest::CreateMergeStrategy(framework::IndexTaskContext* context)
{
    auto mergeConfig = context->GetMergeConfig();
    bool optimize = false;
    if (!context->GetParameter(IS_OPTIMIZE_MERGE, optimize)) {
        optimize = false;
    }
    NormalTableCompactPlanCreator creator;
    auto [_, mergeStrategy] = creator.CreateCompactStrategy(mergeConfig.GetMergeStrategyStr(), optimize);
    return std::move(mergeStrategy);
}

void CombinedMergeStrategyTest::CheckMergePlan(const std::shared_ptr<MergePlan>& mergePlan, size_t idx,
                                               const std::vector<segmentid_t>& srcSegmentIds)
{
    auto& plan = mergePlan->GetSegmentMergePlan(idx);
    ASSERT_EQ(srcSegmentIds.size(), plan.GetSrcSegmentCount());
    for (size_t i = 0; i < srcSegmentIds.size(); i++) {
        EXPECT_EQ(srcSegmentIds[i], plan.GetSrcSegmentId(i));
    }
    EXPECT_EQ(1u, plan.GetTargetSegmentCount());
}

TEST_F(CombinedMergeStrategyTest, TestCreateCombinedMergeStrategyWithBalanceTree)
{
    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 100, .deleteDocCount = 60},
                                 {.id = 1, .isMerged = true, .docCount = 100, .deleteDocCount = 40},
                                 {.id = 3, .isMerged = false, .docCount = 5, .deleteDocCount = 2},
                                 {.id = 4, .isMerged = false, .docCount = 20, .deleteDocCount = 0},
                                 {.id = 5, .isMerged = false, .docCount = 10, .deleteDocCount = 7},
                                 {.id = 6, .isMerged = false, .docCount = 15, .deleteDocCount = 0}};
    auto context = MergeTestHelper::CreateContext(segInfos);
    context->TEST_SetTabletSchema(_tabletSchema);
    auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    std::string mergeParam = "conflict-segment-number=2;base-doc-count=10;max-doc-count=50;conflict-delete-percent=50;"
                             "rt-max-segment-count=3";
    mergeConfig.TEST_SetMergeStrategyStr("balance_tree");
    mergeConfig.TEST_SetMergeStrategyParameterStr(mergeParam);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    auto mergeStrategy = CreateMergeStrategy(context.get());
    auto mergePlan = mergeStrategy->CreateMergePlan(context.get()).second;
    ASSERT_EQ(2u, mergePlan->Size());
    CheckMergePlan(mergePlan, 0, {0});
    CheckMergePlan(mergePlan, 1, {3, 4, 5});
}

TEST_F(CombinedMergeStrategyTest, TestCreateCombinedMergeStategyWithPriorityQueue)
{
    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 1, .deleteDocCount = 0},
                                 {.id = 1, .isMerged = true, .docCount = 2, .deleteDocCount = 0},
                                 {.id = 3, .isMerged = true, .docCount = 3, .deleteDocCount = 0},
                                 {.id = 4, .isMerged = true, .docCount = 2, .deleteDocCount = 0},
                                 {.id = 5, .isMerged = true, .docCount = 1, .deleteDocCount = 0},
                                 {.id = 6, .isMerged = true, .docCount = 0, .deleteDocCount = 0},
                                 {.id = 7, .isMerged = true, .docCount = 3, .deleteDocCount = 0},
                                 {.id = 8, .isMerged = false, .docCount = 10, .deleteDocCount = 0},
                                 {.id = 9, .isMerged = false, .docCount = 20, .deleteDocCount = 0},
                                 {.id = 10, .isMerged = false, .docCount = 30, .deleteDocCount = 0}};
    auto context = MergeTestHelper::CreateContext(segInfos);
    context->TEST_SetTabletSchema(_tabletSchema);
    auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    std::string mergeParam = "rt-max-segment-count=3;"
                             "conflict-segment-count=1;max-merged-segment-doc-count=6";

    mergeConfig.TEST_SetMergeStrategyStr("priority_queue");
    mergeConfig.TEST_SetMergeStrategyParameterStr(mergeParam);
    ASSERT_TRUE(context->SetDesignateTask("merge", "default_merge"));
    auto mergeStrategy = CreateMergeStrategy(context.get());
    auto mergePlan = mergeStrategy->CreateMergePlan(context.get()).second;
    EXPECT_EQ(2u, mergePlan->Size());
    CheckMergePlan(mergePlan, 0, {0, 1, 3, 4, 5, 6, 7});
    CheckMergePlan(mergePlan, 1, {8, 9, 10});
}

TEST_F(CombinedMergeStrategyTest, TestExtractParams)
{
    {
        MergeStrategyParameter params = MergeStrategyParameter::Create(
            /*inputLimit=*/"max-doc-count=100;max-valid-doc-count=80;rt-max-segment-count=3;rt-max-total-merge-size=16",
            /*strategyConditions=*/
            "priority-feature=doc-count#asc;conflict-segment-count=5;conflict-delete-percent=20",
            /*outputLimit=*/
            "max-merged-segment-doc-count=50;max-total-merge-doc-count=50");
        auto [priority_queue_status, priority_queue_params] = PriorityQueueMergeStrategy::ExtractParams(params);
        EXPECT_TRUE(priority_queue_status.IsOK());
        auto [realtime_status, realtime_params] = RealtimeMergeStrategy::ExtractParams(params);
        EXPECT_TRUE(realtime_status.IsOK());
    }
    {
        MergeStrategyParameter params = MergeStrategyParameter::Create(
            /*inputLimit=*/"max-doc-count=100;max-valid-doc-count=80",
            /*strategyConditions=*/
            "priority-feature=doc-count#asc;conflict-segment-count=5;conflict-delete-percent=20",
            /*outputLimit=*/"max-merged-segment-doc-count=50;max-total-merge-doc-count=50");
        auto [priority_queue_status, priority_queue_params] = PriorityQueueMergeStrategy::ExtractParams(params);
        EXPECT_TRUE(priority_queue_status.IsOK());
        auto [realtime_status, realtime_params] = RealtimeMergeStrategy::ExtractParams(params);
        EXPECT_TRUE(realtime_status.IsOK());
    }
    {
        MergeStrategyParameter params = MergeStrategyParameter::Create(
            /*inputLimit=*/"max-doc-count=100;max-valid-doc-count=80",
            /*strategyConditions=*/
            "priority-feature=doc-count#asc;conflict-segment-count=5;conflict-delete-percent=20",
            /*outputLimit=*/
            "max-merged-segment-doc-count=50;max-total-merge-doc-count=50");
        auto [priority_queue_status, priority_queue_params] = PriorityQueueMergeStrategy::ExtractParams(params);
        EXPECT_TRUE(priority_queue_status.IsOK());
        auto [realtime_status, realtime_params] = RealtimeMergeStrategy::ExtractParams(params);
        EXPECT_TRUE(realtime_status.IsOK());
    }
    {
        MergeStrategyParameter params = MergeStrategyParameter::Create(
            /*inputLimit=*/"",
            /*strategyConditions=*/"", /*outputLimit=*/"");
        auto [priority_queue_status, priority_queue_params] = PriorityQueueMergeStrategy::ExtractParams(params);
        EXPECT_TRUE(priority_queue_status.IsOK());
        auto [realtime_status, realtime_params] = RealtimeMergeStrategy::ExtractParams(params);
        EXPECT_TRUE(realtime_status.IsOK());
        EXPECT_FALSE(realtime_status.IsInvalidArgs());
    }

    {
        MergeStrategyParameter params = MergeStrategyParameter::Create(
            /*inputLimit=*/"rt-max-segment-count=0;rt-total-merge-size=456",
            /*strategyConditions=*/"",
            /*outputLimit=*/"");
        auto [priority_queue_status, priority_queue_params] = PriorityQueueMergeStrategy::ExtractParams(params);
        EXPECT_TRUE(priority_queue_status.IsOK());
        auto [realtime_status, realtime_params] = RealtimeMergeStrategy::ExtractParams(params);
        EXPECT_TRUE(realtime_status.IsOK());
    }
}

TEST_F(CombinedMergeStrategyTest, TestCreateMergePlanComplexWithBalanceTreeMerteStrategy)
{
    // for merged segment, adapt balance tree strategy, test max-doc-count & conflict-delete-percent
    // for build segment, adapt realtime strategy, test max-segment-count
    std::unique_ptr<MergeStrategy> strategy(new BalanceTreeMergeStrategy);
    auto mergeStrategy = std::make_unique<CombinedMergeStrategy>(std::move(strategy));
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {
            {2, 3, 4, 5},
            {0},
            {6, 7, 8},
        },
        "rt-max-segment-count=3;"
        "conflict-segment-number=2;base-doc-count=10;max-doc-count=50;conflict-delete-percent=50",
        {
            {.isMerged = true, .docCount = 100, .deleteDocCount = 60},
            {.isMerged = true, .docCount = 100, .deleteDocCount = 40},
            {.isMerged = true, .docCount = 5, .deleteDocCount = 2},
            {.isMerged = true, .docCount = 20, .deleteDocCount = 0},
            {.isMerged = true, .docCount = 10, .deleteDocCount = 7},
            {.isMerged = true, .docCount = 15, .deleteDocCount = 0},
            {.isMerged = false, .docCount = 10, .deleteDocCount = 0},
            {.isMerged = false, .docCount = 20, .deleteDocCount = 0},
            {.isMerged = false, .docCount = 30, .deleteDocCount = 0},
        });
}

TEST_F(CombinedMergeStrategyTest, TestCreateMergePlanComplexWithPriorityQueueMergeStrategy)
{
    // For priority queue(merged segment), test max merged segment doc count
    // For realtime(build segment), test max-segment-count
    std::unique_ptr<MergeStrategy> strategy(new PriorityQueueMergeStrategy);
    auto mergeStrategy = std::make_unique<CombinedMergeStrategy>(std::move(strategy));
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/
                                       {
                                           {0, 1, 3, 4, 5},
                                           {2, 6},
                                           {7, 8, 9},
                                       },
                                       "rt-max-segment-count=3|"
                                       "conflict-segment-count=1|max-merged-segment-doc-count=6",
                                       {
                                           {.isMerged = true, .docCount = 1, .deleteDocCount = 0},
                                           {.isMerged = true, .docCount = 2, .deleteDocCount = 0},
                                           {.isMerged = true, .docCount = 3, .deleteDocCount = 0},
                                           {.isMerged = true, .docCount = 2, .deleteDocCount = 0},
                                           {.isMerged = true, .docCount = 1, .deleteDocCount = 0},
                                           {.isMerged = true, .docCount = 0, .deleteDocCount = 0},
                                           {.isMerged = true, .docCount = 3, .deleteDocCount = 0},
                                           {.isMerged = false, .docCount = 10, .deleteDocCount = 0},
                                           {.isMerged = false, .docCount = 20, .deleteDocCount = 0},
                                           {.isMerged = false, .docCount = 30, .deleteDocCount = 0},
                                       });
}

TEST_F(CombinedMergeStrategyTest, TestCreateMergePlanOnlyMergedSource)
{
    // Only merged segments are eligible for merge
    std::unique_ptr<MergeStrategy> strategy(new PriorityQueueMergeStrategy);
    auto mergeStrategy = std::make_unique<CombinedMergeStrategy>(std::move(strategy));
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/
                                       {
                                           {0, 1, 3, 4, 5},
                                           {2, 6},
                                       },
                                       "rt-max-segment-count=0;|"
                                       "conflict-segment-count=1|max-merged-segment-doc-count=6",
                                       {
                                           {.isMerged = true, .docCount = 1, .deleteDocCount = 0},
                                           {.isMerged = true, .docCount = 2, .deleteDocCount = 0},
                                           {.isMerged = true, .docCount = 3, .deleteDocCount = 0},
                                           {.isMerged = true, .docCount = 2, .deleteDocCount = 0},
                                           {.isMerged = true, .docCount = 1, .deleteDocCount = 0},
                                           {.isMerged = true, .docCount = 0, .deleteDocCount = 0},
                                           {.isMerged = true, .docCount = 3, .deleteDocCount = 0},
                                           {.isMerged = false, .docCount = 10, .deleteDocCount = 0},
                                           {.isMerged = false, .docCount = 20, .deleteDocCount = 0},
                                           {.isMerged = false, .docCount = 30, .deleteDocCount = 0},
                                       });
}

TEST_F(CombinedMergeStrategyTest, TestCreateMergePlanOnlyBuild)
{
    // Only build segments are eligible for merge
    std::unique_ptr<MergeStrategy> strategy(new PriorityQueueMergeStrategy);
    auto mergeStrategy = std::make_unique<CombinedMergeStrategy>(std::move(strategy));
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/
                                       {
                                           {7, 8, 9},
                                       },
                                       "rt-max-total-merge-size=100|"
                                       "conflict-segment-count=10|max-merged-segment-doc-count=6",
                                       {{.isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 40},
                                        {.isMerged = true, .docCount = 2, .deleteDocCount = 0, .segmentSize = 40},
                                        {.isMerged = true, .docCount = 3, .deleteDocCount = 0, .segmentSize = 40},
                                        {.isMerged = true, .docCount = 2, .deleteDocCount = 0, .segmentSize = 40},
                                        {.isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 40},
                                        {.isMerged = true, .docCount = 0, .deleteDocCount = 0, .segmentSize = 40},
                                        {.isMerged = true, .docCount = 3, .deleteDocCount = 0, .segmentSize = 40},
                                        {.isMerged = false, .docCount = 10, .deleteDocCount = 0, .segmentSize = 40},
                                        {.isMerged = false, .docCount = 20, .deleteDocCount = 0, .segmentSize = 40},
                                        {.isMerged = false, .docCount = 30, .deleteDocCount = 0, .segmentSize = 40}});
}

TEST_F(CombinedMergeStrategyTest, TestFullFillSegmentMergePlan)
{
    auto testOnce = [&](const std::vector<std::string>& segmentGroups, const std::string& expectGroup) {
        framework::Version version;
        std::vector<std::shared_ptr<framework::Segment>> segments;
        auto resourceMap = std::make_shared<framework::ResourceMap>();
        auto strategy = std::make_unique<PriorityQueueMergeStrategy>();
        auto mergeStrategy = std::make_unique<CombinedMergeStrategy>(std::move(strategy));
        std::shared_ptr<framework::TabletData> tabletData(new framework::TabletData("demo"));
        SegmentMergePlan segmentMergePlan;
        for (size_t i = 0; i < segmentGroups.size(); ++i) {
            version.AddSegment(i);
            auto segment = std::make_shared<framework::FakeSegment>(framework::Segment::SegmentStatus::ST_BUILT);
            framework::SegmentMeta segmentMeta(i);

            if (!segmentGroups[i].empty()) {
                framework::SegmentStatistics stats;
                stats.SetSegmentId(i);
                stats.AddStatistic(NORMAL_TABLE_GROUP_TAG_KEY, segmentGroups[i]);
                segmentMeta.segmentInfo->SetSegmentStatistics(stats);
            }
            segment->TEST_SetSegmentMeta(segmentMeta);
            segments.push_back(segment);
            segmentMergePlan.AddSrcSegment(i);
        }
        ASSERT_TRUE(tabletData->Init(version, segments, resourceMap).IsOK());
        segmentid_t lastSegmentId = segmentGroups.size();
        ASSERT_TRUE(mergeStrategy->FullFillSegmentMergePlan(tabletData, &lastSegmentId, &segmentMergePlan).IsOK());
        ASSERT_EQ(segmentGroups.size() + 1, lastSegmentId);
        ASSERT_EQ(1, segmentMergePlan.GetTargetSegmentCount());
        auto targetSegmentInfo = segmentMergePlan.GetTargetSegmentInfo(0);
        auto [status, stat] = targetSegmentInfo.GetSegmentStatistics();
        ASSERT_TRUE(status.IsOK());
        std::string realGroup;
        ASSERT_EQ(!expectGroup.empty(), stat.GetStatistic(NORMAL_TABLE_GROUP_TAG_KEY, realGroup));
        ASSERT_EQ(expectGroup, realGroup);
    };
    testOnce({"hot", "hot"}, "hot");
    testOnce({"hot", ""}, "");
    testOnce({"hot", "cold"}, "");
    testOnce({"", ""}, "");
}

}} // namespace indexlibv2::table
