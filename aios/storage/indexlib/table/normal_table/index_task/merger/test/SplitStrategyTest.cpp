#include "indexlib/table/normal_table/index_task/merger/SplitStrategy.h"

#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/mock/FakeSegment.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/NormalSchemaResolver.h"
#include "unittest/unittest.h"

namespace indexlibv2::table {

class FakeSplitStrategy : public SplitStrategy
{
public:
    std::pair<Status, int32_t> GetDocGroupId(segmentid_t segmentId, docid_t docId) const
    {
        auto iter = _groupMap.find(docId);
        if (iter == _groupMap.end()) {
            return {Status::NotFound(), -1};
        }
        return {Status::OK(), iter->second};
    }

    std::pair<Status, std::vector<int32_t>> CalculateGroupCounts(const framework::IndexTaskContext* context,
                                                                 std::shared_ptr<framework::Segment> segment) override
    {
        auto docCount = segment->GetSegmentInfo()->docCount;
        std::vector<int32_t> counts(100, 0);
        for (docid_t docid = 0; docid < docCount; ++docid) {
            auto [status, groupId] = GetDocGroupId(segment->GetSegmentId(), docid);
            RETURN2_IF_STATUS_ERROR(status, std::vector<int32_t>(), "calculate groupd id for docid [%d] failed", docid);
            assert(groupId < counts.size());
            counts[groupId]++;
        }
        return {Status::OK(), counts};
    }

    void SetGroupMap(const std::map<docid_t, int32_t>& groupMap) { _groupMap = groupMap; }

private:
    std::map<docid_t, int32_t> _groupMap;
};

class SplitStrategyTest : public TESTBASE
{
public:
    SplitStrategyTest() = default;
    ~SplitStrategyTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void SplitStrategyTest::setUp() {}

void SplitStrategyTest::tearDown() {}

TEST_F(SplitStrategyTest, TestCreateMergePlan)
{
    std::shared_ptr<config::ITabletSchema> tabletSchema =
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema.json");
    ASSERT_TRUE(tabletSchema);
    // segment 0   [0, 2] HOT, [3, 4] WARM, [5, 6] COLD
    // segment 1   [0, 2] COLD
    std::map<docid_t, int32_t> groupMap(
        {{0, 0}, {1, 0}, {2, 0}, {3, 1}, {4, 1}, {5, 2}, {6, 2}, {7, 2}, {8, 2}, {9, 2}});
    std::shared_ptr<framework::TabletData> tabletData(new framework::TabletData("tableName"));
    framework::Version version(100);
    version.AddSegment(0);
    version.AddSegment(1);
    std::shared_ptr<framework::Segment> segment0(
        new framework::FakeSegment(framework::Segment::SegmentStatus::ST_BUILT)),
        segment1(new framework::FakeSegment(framework::Segment::SegmentStatus::ST_BUILT));
    std::vector<std::shared_ptr<framework::Segment>> segments({segment0, segment1});
    framework::SegmentMeta meta0(0), meta1(1);
    meta0.segmentInfo->docCount = 7;
    meta1.segmentInfo->docCount = 3;
    segment0->TEST_SetSegmentMeta(meta0);
    segment1->TEST_SetSegmentMeta(meta1);
    std::shared_ptr<framework::ResourceMap> resourceMap(new framework::ResourceMap);
    ASSERT_TRUE(tabletData->Init(version, segments, resourceMap).IsOK());
    std::shared_ptr<config::TabletOptions> tabletOption(new config::TabletOptions);
    // TODO(xiaohao.yxh) add output limit
    config::MergeStrategyParameter mergeParam;
    mergeParam._impl->outputLimitParam = "max-total-merge-doc-count=8";
    tabletOption->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyParameter(mergeParam);
    framework::IndexTaskContext context;
    context._tabletData = tabletData;
    context._tabletSchema = tabletSchema;
    context._tabletOptions = tabletOption;
    context._maxMergedSegmentId = 1;
    ASSERT_TRUE(context.SetDesignateTask("merge", "default_merge"));
    FakeSplitStrategy* fakeSplitStrategy = new FakeSplitStrategy;
    fakeSplitStrategy->SetGroupMap(groupMap);
    std::shared_ptr<MergeStrategy> splitStrategy(fakeSplitStrategy);
    auto [status, mergePlan] = splitStrategy->CreateMergePlan(&context);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(mergePlan);
    ASSERT_EQ(1, mergePlan->Size());
    auto segmentMergePlan = mergePlan->GetSegmentMergePlan(0);
    ASSERT_EQ(1, segmentMergePlan.GetSrcSegmentCount());
    ASSERT_EQ(0, segmentMergePlan.GetSrcSegmentId(0));
    ASSERT_EQ(3, segmentMergePlan.GetTargetSegmentCount());
    auto checkFunc = [&](int32_t idx, segmentid_t expectSegmentId, const std::string& expectSegmentGroup) {
        ASSERT_EQ(expectSegmentId, segmentMergePlan.GetTargetSegmentId(idx));
        auto& segmentInfo = segmentMergePlan.GetTargetSegmentInfo(idx);
        auto descriptions = segmentInfo.descriptions;
        auto statisticStr = descriptions[framework::SegmentStatistics::JSON_KEY];
        framework::SegmentStatistics segmentStatistics;
        ASSERT_NO_THROW(FromJsonString(segmentStatistics, statisticStr));
        auto strStats = segmentStatistics.GetStringStats();
        ASSERT_EQ(expectSegmentGroup, strStats[NORMAL_TABLE_GROUP_TAG_KEY]);
    };
    checkFunc(0, 2, "HOT");
    checkFunc(1, 3, "WARM");
    checkFunc(2, 4, "COLD");
}
} // namespace indexlibv2::table
