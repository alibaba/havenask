#include "indexlib/framework/SegmentStatistics.h"

#include "autil/legacy/legacy_jsonizable.h"
#include "unittest/unittest.h"
using namespace std;

namespace indexlibv2::framework {

using IntegerRangeType = SegmentStatistics::IntegerRangeType;

class SegmentStatisticsTest : public TESTBASE
{
public:
    SegmentStatisticsTest() = default;
    ~SegmentStatisticsTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(SegmentStatisticsTest, TestSimpleProcess)
{
    SegmentStatistics segStats1;
    std::string jsonStr = R"(
        {
            "segment_id": 1,
            "integer_stats": {
                "eventtime": [100, 200],
                "statistic_key1": [1, 2],
                "statistic_key2": [2, 3]
            },
            "string_stats": {
                "segment_group": "hot",
                "user_tag": "foo"
            }
        }
    )";
    std::map<std::string, std::string> expectStringStats({{"segment_group", "hot"}, {"user_tag", "foo"}});
    autil::legacy::FromJsonString(segStats1, jsonStr);
    const auto& stringStats1 = segStats1.GetStringStats();
    const std::map<std::string, IntegerRangeType>& integerStats = segStats1.GetIntegerStats();
    ASSERT_EQ(1, segStats1.GetSegmentId());
    ASSERT_EQ(expectStringStats, stringStats1);
    ASSERT_EQ(3, integerStats.size());
    auto it = integerStats.find("eventtime");
    ASSERT_TRUE(it != integerStats.end());
    ASSERT_EQ((std::make_pair<int64_t, int64_t>(100, 200)), it->second);
    it = integerStats.find("statistic_key1");
    ASSERT_TRUE(it != integerStats.end());
    ASSERT_EQ((std::make_pair<int64_t, int64_t>(1, 2)), it->second);
    it = integerStats.find("statistic_key2");
    ASSERT_TRUE(it != integerStats.end());
    ASSERT_EQ((std::make_pair<int64_t, int64_t>(2, 3)), it->second);

    SegmentStatistics segStats2;
    autil::legacy::FromJsonString(segStats2, autil::legacy::ToJsonString(segStats1));
    const auto& stringStats2 = segStats2.GetStringStats();
    const std::map<std::string, IntegerRangeType>& integerStats2 = segStats2.GetIntegerStats();
    ASSERT_EQ(1, segStats2.GetSegmentId());
    ASSERT_EQ(expectStringStats, stringStats2);
    ASSERT_EQ(3, integerStats2.size());
    it = integerStats2.find("eventtime");
    ASSERT_TRUE(it != integerStats2.end());
    ASSERT_EQ((std::make_pair<int64_t, int64_t>(100, 200)), it->second);
    it = integerStats2.find("statistic_key1");
    ASSERT_TRUE(it != integerStats2.end());
    ASSERT_EQ((std::make_pair<int64_t, int64_t>(1, 2)), it->second);
    it = integerStats2.find("statistic_key2");
    ASSERT_TRUE(it != integerStats2.end());
    ASSERT_EQ((std::make_pair<int64_t, int64_t>(2, 3)), it->second);

    ASSERT_TRUE(segStats1 == segStats2);
    SegmentStatistics segStats3;
    segStats3 = segStats1;
    ASSERT_TRUE(segStats3 == segStats1);
    SegmentStatistics segStats4(segStats1);
    ASSERT_TRUE(segStats4 == segStats1);
}

TEST_F(SegmentStatisticsTest, TestReduceStatistics)
{
    SegmentStatistics segStats1(1, {{"key1", {100, 200}}, {"key2", {100, 200}}}, {});
    SegmentStatistics segStats2(2, {{"key1", {100, 190}}, {"key2", {90, 210}}, {"key3", {1, 2}}}, {});
    SegmentStatistics segStats3(3, {{"key3", {-1, 0}}, {"key4", {1, 2}}}, {});

    std::vector<SegmentStatistics> segStatsVec {segStats1, segStats2, segStats3};
    auto outputStats = SegmentStatistics::ReduceIntegerStatistics(segStatsVec, SegmentStatistics::RangeUnionAcc);
    SegmentStatistics segStats4(4, {{"key1", {100, 200}}, {"key2", {90, 210}}, {"key3", {-1, 2}}, {"key4", {1, 2}}},
                                {});
    SegmentStatistics mergedSegStats;
    mergedSegStats.SetSegmentId(4);
    mergedSegStats.GetIntegerStats() = outputStats;
    EXPECT_EQ(segStats4, mergedSegStats);
}

} // namespace indexlibv2::framework
