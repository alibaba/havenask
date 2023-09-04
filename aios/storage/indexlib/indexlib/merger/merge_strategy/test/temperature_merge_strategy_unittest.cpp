#include "indexlib/merger/merge_strategy/temperature_merge_strategy.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil::legacy;
using namespace indexlib::index_base;
using namespace indexlib::index;

namespace indexlib { namespace merger {

class TemperatureMergeStrategyTest : public INDEXLIB_TESTBASE
{
public:
    TemperatureMergeStrategyTest();
    ~TemperatureMergeStrategyTest();

    DECLARE_CLASS_NAME(TemperatureMergeStrategyTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestStrategyTemperatureConflict();
    void TestStrategyDocCount();

private:
    SegmentMergeInfo CreateSegmentMergeInfo(string lifeCycle, string lifeCycleDetail, uint64_t docCount,
                                            uint64_t segmentSize, segmentid_t segmentId);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TemperatureMergeStrategyTest, TestStrategyTemperatureConflict);
INDEXLIB_UNIT_TEST_CASE(TemperatureMergeStrategyTest, TestStrategyDocCount);
IE_LOG_SETUP(merger, TemperatureMergeStrategyTest);

TemperatureMergeStrategyTest::TemperatureMergeStrategyTest() {}

TemperatureMergeStrategyTest::~TemperatureMergeStrategyTest() {}

void TemperatureMergeStrategyTest::CaseSetUp() {}

SegmentMergeInfo TemperatureMergeStrategyTest::CreateSegmentMergeInfo(string lifeCycle, string lifeCycleDetail,
                                                                      uint64_t docCount, uint64_t segmentSize,
                                                                      segmentid_t segmentId)
{
    SegmentMergeInfo mergeInfo;
    mergeInfo.segmentId = segmentId;
    indexlib::framework::SegmentMetrics& segmentMetrics = mergeInfo.segmentMetrics;
    json::JsonMap temperature, group;
    temperature[LIFECYCLE] = lifeCycle;
    temperature[LIFECYCLE_DETAIL] = lifeCycleDetail;
    group[TEMPERATURE_SEGMENT_METIRC_KRY] = temperature;
    segmentMetrics.SetSegmentGroupMetrics(SEGMENT_CUSTOMIZE_METRICS_GROUP, group);
    mergeInfo.segmentSize = segmentSize;
    mergeInfo.segmentInfo.docCount = docCount;
    return mergeInfo;
}

void TemperatureMergeStrategyTest::CaseTearDown() {}

void TemperatureMergeStrategyTest::TestStrategyDocCount()
{
    string configStr = R"(
    {
        "output_limits" : "max_hot_segment_size=100;max_warm_segment_size=200;max_cold_segment_size=300",
        "strategy_conditions" : "priority-feature=doc-count;"
    })";
    config::MergeStrategyParameter param;
    FromJsonString(param, configStr);
    TemperatureMergeStrategy strategy;
    ASSERT_NO_THROW(strategy.SetParameter(param));
    SegmentMergeInfos mergeInfos;

    mergeInfos.push_back(CreateSegmentMergeInfo("HOT", "HOT:2;WARM:3;COLD:5", 10, 100 * 1024 * 1024, 1));
    mergeInfos.push_back(CreateSegmentMergeInfo("WARM", "HOT:0;WARM:2;COLD:2", 4, 200 * 1024 * 1024, 2));
    mergeInfos.push_back(CreateSegmentMergeInfo("HOT", "HOT:1;WARM:2;COLD:1", 4, 300 * 1024 * 1024, 0));
    mergeInfos.push_back(CreateSegmentMergeInfo("COLD", "HOT:0;WARM:0;COLD:8", 8, 10 * 1024 * 1024, 5));
    mergeInfos.push_back(CreateSegmentMergeInfo("WARM", "HOT:0;WARM:3;COLD:7", 10, 100 * 1024 * 1024, 3));
    mergeInfos.push_back(CreateSegmentMergeInfo("COLD", "HOT:0;WARM:0;COLD:20", 20, 10 * 1024 * 1024, 4));

    vector<segmentid_t> expectMergeSegment = {0, 1, 5, 4};
    indexlibv2::framework::LevelInfo levelInfo;
    MergeTask task = strategy.CreateMergeTask(mergeInfos, levelInfo);
    ASSERT_EQ(1, task.GetPlanCount());
    MergePlan plan = task[0];
    ASSERT_EQ(4, plan.GetSegmentCount());
    for (auto mergedSegment : expectMergeSegment) {
        ASSERT_TRUE(plan.HasSegment(mergedSegment));
    }
}

void TemperatureMergeStrategyTest::TestStrategyTemperatureConflict()
{
    {
        CaseTearDown();
        CaseSetUp();
        string configStr = R"(
        {
            "output_limits" : "max_hot_segment_size=100;max_warm_segment_size=200;max_cold_segment_size=300",
            "strategy_conditions" : "priority-feature=temperature_conflict;select-sequence=HOT,WARM,COLD"
        })";
        config::MergeStrategyParameter param;
        FromJsonString(param, configStr);
        TemperatureMergeStrategy strategy;
        ASSERT_NO_THROW(strategy.SetParameter(param));
        SegmentMergeInfos mergeInfos;
        mergeInfos.push_back(CreateSegmentMergeInfo("WARM", "HOT:0;WARM:2;COLD:8", 10, 200 * 1024 * 1024, 3));
        mergeInfos.push_back(CreateSegmentMergeInfo("COLD", "HOT:0;WARM:0;COLD:10", 10, 10 * 1024 * 1024, 5));
        mergeInfos.push_back(CreateSegmentMergeInfo("HOT", "HOT:2;WARM:3;COLD:5", 10, 100 * 1024 * 1024, 0));
        mergeInfos.push_back(
            CreateSegmentMergeInfo("HOT", "HOT:1;WARM:1;COLD:8", 10, (uint64_t)10000 * 1024 * 1024, 2));
        mergeInfos.push_back(CreateSegmentMergeInfo("HOT", "HOT:4;WARM:3;COLD:3", 10, 100 * 1024 * 1024, 1));
        mergeInfos.push_back(CreateSegmentMergeInfo("WARM", "HOT:0;WARM:3;COLD:7", 10, 100 * 1024 * 1024, 4));
        mergeInfos.push_back(CreateSegmentMergeInfo("COLD", "HOT:0;WARM:0;COLD:10", 10, 70 * 1024 * 1024, 6));
        vector<segmentid_t> expectMergeSegment = {0, 1, 3, 5};
        indexlibv2::framework::LevelInfo levelInfo;
        MergeTask task = strategy.CreateMergeTask(mergeInfos, levelInfo);
        ASSERT_EQ(1, task.GetPlanCount());
        MergePlan plan = task[0];
        ASSERT_EQ(4, plan.GetSegmentCount());
        for (auto mergedSegment : expectMergeSegment) {
            ASSERT_TRUE(plan.HasSegment(mergedSegment));
        }
    }
    {
        CaseTearDown();
        CaseSetUp();
        string configStr = R"(
        {
            "output_limits" : "max_hot_segment_size=100;max_warm_segment_size=200;max_cold_segment_size=300",
            "strategy_conditions" : "priority-feature=temperature_conflict;select-sequence=HOT,WARM"
        })";
        config::MergeStrategyParameter param;
        FromJsonString(param, configStr);
        TemperatureMergeStrategy strategy;
        ASSERT_NO_THROW(strategy.SetParameter(param));
        SegmentMergeInfos mergeInfos;
        mergeInfos.push_back(CreateSegmentMergeInfo("WARM", "HOT:0;WARM:2;COLD:8", 10, 200 * 1024 * 1024, 3));
        mergeInfos.push_back(CreateSegmentMergeInfo("COLD", "HOT:0;WARM:0;COLD:10", 10, 10 * 1024 * 1024, 5));
        mergeInfos.push_back(CreateSegmentMergeInfo("HOT", "HOT:2;WARM:3;COLD:5", 10, 100 * 1024 * 1024, 0));
        mergeInfos.push_back(
            CreateSegmentMergeInfo("HOT", "HOT:1;WARM:1;COLD:8", 10, (uint64_t)10000 * 1024 * 1024, 2));
        mergeInfos.push_back(CreateSegmentMergeInfo("HOT", "HOT:4;WARM:3;COLD:3", 10, 100 * 1024 * 1024, 1));
        mergeInfos.push_back(CreateSegmentMergeInfo("WARM", "HOT:0;WARM:3;COLD:7", 10, 100 * 1024 * 1024, 4));
        mergeInfos.push_back(CreateSegmentMergeInfo("COLD", "HOT:0;WARM:0;COLD:10", 10, 70 * 1024 * 1024, 6));
        vector<segmentid_t> expectMergeSegment = {0, 1, 3};
        indexlibv2::framework::LevelInfo levelInfo;
        MergeTask task = strategy.CreateMergeTask(mergeInfos, levelInfo);
        ASSERT_EQ(1, task.GetPlanCount());
        MergePlan plan = task[0];
        ASSERT_EQ(3, plan.GetSegmentCount());
        for (auto mergedSegment : expectMergeSegment) {
            ASSERT_TRUE(plan.HasSegment(mergedSegment));
        }
    }
}
}} // namespace indexlib::merger
