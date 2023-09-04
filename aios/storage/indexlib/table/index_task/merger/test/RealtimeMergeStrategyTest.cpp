#include "indexlib/table/index_task/merger/RealtimeMergeStrategy.h"

#include "indexlib/table/index_task/merger/test/MergeTestHelper.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace table {

class RealtimeMergeStrategyTest : public TESTBASE
{
public:
    RealtimeMergeStrategyTest() {}
    ~RealtimeMergeStrategyTest() {}

public:
    void setUp() override {}
    void tearDown() override {}

private:
    static void CheckStatus(std::pair<Status, struct RealtimeMergeParams> pair, bool expectOk)
    {
        if (expectOk) {
            EXPECT_TRUE(pair.first.IsOK());
        } else {
            EXPECT_FALSE(pair.first.IsOK());
            EXPECT_TRUE(pair.first.IsInvalidArgs());
        }
    }
};

TEST_F(RealtimeMergeStrategyTest, TestExtractParameter)
{
    auto CreateParam = [](const std::string& paramStr) {
        config::MergeStrategyParameter param;
        param.SetLegacyString(paramStr);
        return param;
    };

    std::string badParam1 = "rt-max-segment-count=-1;"
                            "rt-max-total-merge-size=128";
    CheckStatus(RealtimeMergeStrategy::ExtractParams(CreateParam(badParam1)), /*expectOk=*/false);

    std::string badParam2 = "rt-max-segment-count=4;"
                            "rt-max-total-merge-size=-1";
    CheckStatus(RealtimeMergeStrategy::ExtractParams(CreateParam(badParam2)), /*expectOk=*/false);

    std::string badParam3 = "rt-max-segment-count=-1;"
                            "rt-max-total-merge-size=-1";
    CheckStatus(RealtimeMergeStrategy::ExtractParams(CreateParam(badParam3)), /*expectOk=*/false);

    std::string goodParam1 = "rt-max-segment-count=0;"
                             "rt-max-total-merge-size=128";
    CheckStatus(RealtimeMergeStrategy::ExtractParams(CreateParam(goodParam1)), /*expectOk=*/true);

    std::string goodParam2 = "rt-max-segment-count=3;"
                             "rt-max-total-merge-size=0";
    CheckStatus(RealtimeMergeStrategy::ExtractParams(CreateParam(goodParam2)), /*expectOk=*/true);

    std::string goodParam3 = "rt-max-segment-count=3;"
                             "rt-max-total-merge-size=128";
    CheckStatus(RealtimeMergeStrategy::ExtractParams(CreateParam(goodParam3)), /*expectOk=*/true);
}

// TEST default params
TEST_F(RealtimeMergeStrategyTest, TestDefaultParams)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1, 2, 3}}, "",
                                       {{.docCount = 100, .segmentSize = 81},
                                        {.docCount = 100, .segmentSize = 70},
                                        {.docCount = 100, .segmentSize = 25},
                                        {.docCount = 100, .segmentSize = 60}});
}

// TEST rt-max-segment-count
TEST_F(RealtimeMergeStrategyTest, TestMaxSegmentCount0)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {}, "rt-max-segment-count=4;", {});
}
TEST_F(RealtimeMergeStrategyTest, TestMaxSegmentCount1)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {}, "rt-max-segment-count=0;",
                                       {{.docCount = 100, .segmentSize = 81},
                                        {.docCount = 100, .segmentSize = 70},
                                        {.docCount = 100, .segmentSize = 25},
                                        {.docCount = 100, .segmentSize = 60}});
}
TEST_F(RealtimeMergeStrategyTest, TestMaxSegmentCount2)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1}}, "rt-max-segment-count=2;",
                                       {{.docCount = 100, .segmentSize = 60},
                                        {.docCount = 100, .segmentSize = 50},
                                        {.docCount = 100, .segmentSize = 81},
                                        {.docCount = 100, .segmentSize = 40}});
}
TEST_F(RealtimeMergeStrategyTest, TestMaxSegmentCount3)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0}}, "rt-max-segment-count=4;",
                                       {{.docCount = 100, .segmentSize = 40}});
}
TEST_F(RealtimeMergeStrategyTest, TestMaxSegmentCount4)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1, 5, 6}},
                                       "rt-max-segment-count=4;",
                                       {{.docCount = 100, .segmentSize = 60},
                                        {.docCount = 100, .segmentSize = 50},
                                        {.id = 5, .docCount = 100, .segmentSize = 81},
                                        {.docCount = 100, .segmentSize = 40}});
}

// TEST rt-max-total-merge-size
TEST_F(RealtimeMergeStrategyTest, TestMaxTotalMergeSize0)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {}, "rt-max-total-merge-size=128;",
                                       {});
}
TEST_F(RealtimeMergeStrategyTest, TestMaxTotalMergeSize1)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {}, "rt-max-total-merge-size=0;",
                                       {{.docCount = 100, .segmentSize = 81},
                                        {.docCount = 100, .segmentSize = 70},
                                        {.docCount = 100, .segmentSize = 25},
                                        {.docCount = 100, .segmentSize = 60}});
}
TEST_F(RealtimeMergeStrategyTest, TestMaxTotalMergeSize2)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0}}, "rt-max-total-merge-size=1;",
                                       {{.docCount = 100, .segmentSize = 81},
                                        {.docCount = 100, .segmentSize = 70},
                                        {.docCount = 100, .segmentSize = 25},
                                        {.docCount = 100, .segmentSize = 60}});
}
TEST_F(RealtimeMergeStrategyTest, TestMaxTotalMergeSize3)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0}},
                                       "rt-max-total-merge-size=99999;", {{.docCount = 100, .segmentSize = 40}});
}
TEST_F(RealtimeMergeStrategyTest, TestMaxTotalMergeSize4)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1, 5, 6}},
                                       "rt-max-total-merge-size=220;",
                                       {{.docCount = 100, .segmentSize = 60},
                                        {.docCount = 100, .segmentSize = 50},
                                        {.id = 5, .docCount = 100, .segmentSize = 70},
                                        {.docCount = 100, .segmentSize = 40}});
}
TEST_F(RealtimeMergeStrategyTest, TestMaxTotalMergeSize5)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0}}, "rt-max-total-merge-size=60;",
                                       {{.docCount = 100, .segmentSize = 60},
                                        {.docCount = 100, .segmentSize = 50},
                                        {.docCount = 100, .segmentSize = 70},
                                        {.docCount = 100, .segmentSize = 40}});
}
TEST_F(RealtimeMergeStrategyTest, TestMaxTotalMergeSize6)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1}},
                                       "rt-max-total-merge-size=61;",
                                       {{.docCount = 100, .segmentSize = 60},
                                        {.docCount = 100, .segmentSize = 50},
                                        {.docCount = 100, .segmentSize = 70},
                                        {.docCount = 100, .segmentSize = 40}});
}
TEST_F(RealtimeMergeStrategyTest, TestMaxTotalMergeSize7)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1}},
                                       "rt-max-total-merge-size=110;",
                                       {{.docCount = 100, .segmentSize = 60},
                                        {.docCount = 100, .segmentSize = 50},
                                        {.docCount = 100, .segmentSize = 70},
                                        {.docCount = 100, .segmentSize = 40}});
}
TEST_F(RealtimeMergeStrategyTest, TestMaxTotalMergeSize8)
{
    auto mergeStrategy = std::make_unique<RealtimeMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1, 2}},
                                       "rt-max-total-merge-size=111;",
                                       {{.docCount = 100, .segmentSize = 60},
                                        {.docCount = 100, .segmentSize = 50},
                                        {.docCount = 100, .segmentSize = 70},
                                        {.docCount = 100, .segmentSize = 40}});
}

}} // namespace indexlibv2::table
