#include "indexlib/table/normal_table/index_task/merger/NormalTabletOptimizeMergeStrategy.h"

#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/test/MergeTestHelper.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace table {

namespace {
using config::MergeStrategyParameter;
using FakeSegmentInfos = std::vector<FakeSegmentInfo>;
} // namespace
class NormalTabletOptimizeMergeStrategyTest : public TESTBASE
{
public:
    NormalTabletOptimizeMergeStrategyTest() {}
    ~NormalTabletOptimizeMergeStrategyTest() {}

private:
    void TestNormalTabletOptimizeMergeStrategy(const std::vector<std::vector<segmentid_t>>& expectSrcSegments,
                                               const std::string& rawStrategyParamString,
                                               const std::vector<FakeSegmentInfo>& segInfos, bool isOptimizeMerge);

public:
    void setUp() override {}
    void tearDown() override {}
};

void NormalTabletOptimizeMergeStrategyTest::TestNormalTabletOptimizeMergeStrategy(
    const std::vector<std::vector<segmentid_t>>& expectSrcSegments, const std::string& rawStrategyParamString,
    const std::vector<FakeSegmentInfo>& segInfos, bool isOptimizeMerge)
{
    NormalTabletOptimizeMergeStrategy ms;
    MergeTestHelper::TestMergeStrategy(&ms, expectSrcSegments, rawStrategyParamString, segInfos, isOptimizeMerge);
}

TEST_F(NormalTabletOptimizeMergeStrategyTest, TestCaseForExtractParams)
{
    {
        auto [status, params] =
            NormalTabletOptimizeMergeStrategy::ExtractParams(MergeStrategyParameter::CreateFromLegacyString(" "));
        EXPECT_TRUE(status.IsOK());
    }
    {
        auto [status, params] = NormalTabletOptimizeMergeStrategy::ExtractParams(
            MergeStrategyParameter::CreateFromLegacyString("after-merge-max-doc-count=10"));
        EXPECT_TRUE(status.IsOK());
    }
    {
        auto [status, params] = NormalTabletOptimizeMergeStrategy::ExtractParams(
            MergeStrategyParameter::CreateFromLegacyString("after-merge-max-segment-count=10"));
        EXPECT_TRUE(status.IsOK());
    }
    {
        auto [status, params] =
            NormalTabletOptimizeMergeStrategy::ExtractParams(MergeStrategyParameter::CreateFromLegacyString(
                "after-merge-max-segment-count=10;after-merge-max-doc-count=10;"));
        EXPECT_FALSE(status.IsOK());
        EXPECT_TRUE(status.IsInvalidArgs());
    }
}

// TODO: move to plan creator
// void NormalTabletOptimizeMergeStrategyTest::TestCaseForSetParameterForMultiSegment()
// {
//     {
//         IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
//         schema->SetAdaptiveDictSchema(AdaptiveDictionarySchemaPtr(new AdaptiveDictionarySchema));

//         NormalTabletOptimizeMergeStrategy oms(mSegDir, schema);
//         ASSERT_THROW(oms.SetParameter("after-merge-max-doc-count=10"), BadParameterException);
//     }

//     {
//         IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
//         schema->SetTruncateProfileSchema(TruncateProfileSchemaPtr(new TruncateProfileSchema));

//         NormalTabletOptimizeMergeStrategy oms(mSegDir, schema);
//         ASSERT_THROW(oms.SetParameter("after-merge-max-doc-count=10"), BadParameterException);
//     }
// }

TEST_F(NormalTabletOptimizeMergeStrategyTest, TestCaseForOptimizeWithMaxSegmentDocCount)
{
    std::string strategyString = "max-doc-count=10";
    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = false, .docCount = 10, .deleteDocCount = 1},
                                 {.id = 1, .isMerged = false, .docCount = 11, .deleteDocCount = 0}};
    // for inc build
    TestNormalTabletOptimizeMergeStrategy({{0}}, strategyString, segInfos, /*isOptimizeMerge=*/false);
    // for full build
    TestNormalTabletOptimizeMergeStrategy({{0, 1}}, strategyString, segInfos, /*isOptimizeMerge=*/true);
}

TEST_F(NormalTabletOptimizeMergeStrategyTest, TestCaseForOptimizeWithAfterMergeMaxSegmentCount)
{
    NormalTabletOptimizeMergeStrategy oms;
    std::string strategyString = "after-merge-max-segment-count=2";
    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = false, .docCount = 9, .deleteDocCount = 8},
                                 {.id = 1, .isMerged = false, .docCount = 5, .deleteDocCount = 4},
                                 {.id = 2, .isMerged = false, .docCount = 1, .deleteDocCount = 0}};
    TestNormalTabletOptimizeMergeStrategy({{0, 1}, {2}}, strategyString, segInfos, /*isOptimizeMerge=*/false);
}

TEST_F(NormalTabletOptimizeMergeStrategyTest, TestRepeatMerge)
{
    {
        // optimize merge to one segment
        // no need merge
        FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 10, .deleteDocCount = 0}};
        TestNormalTabletOptimizeMergeStrategy({}, "", segInfos, /*isOptimizeMerge=*/false);
    }
    {
        // optimize merge to one segment
        // need merge when optimizeSingleSegment=false
        FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 10, .deleteDocCount = 0}};
        TestNormalTabletOptimizeMergeStrategy({{0}}, "skip-single-merged-segment=false", segInfos,
                                              /*isOptimizeMerge=*/false);
    }
    {
        // optimize merge to one segment
        // need merge to one segment
        FakeSegmentInfos segInfos = {{.id = 0, .isMerged = false, .docCount = 10, .deleteDocCount = 0}};
        TestNormalTabletOptimizeMergeStrategy({{0}}, "skip-single-merged-segment=false", segInfos,
                                              /*isOptimizeMerge=*/false);
    }
    {
        // optimize merge to one segment
        // need merge to one segment
        FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 10, .deleteDocCount = 0},
                                     {.id = 1, .isMerged = true, .docCount = 10, .deleteDocCount = 0}};
        TestNormalTabletOptimizeMergeStrategy({{0, 1}}, "", segInfos, /*isOptimizeMerge=*/false);
    }
    {
        // optimize merge to multi segment
        // after-merge-max-doc-count is 10, no need merge
        FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 10, .deleteDocCount = 0},
                                     {.id = 1, .isMerged = true, .docCount = 10, .deleteDocCount = 0}};
        TestNormalTabletOptimizeMergeStrategy({}, "after-merge-max-doc-count=10", segInfos, /*isOptimizeMerge=*/false);
    }
    {
        // optimize merge to multi segment
        // after-merge-max-doc-count is 20, need merge to two segments
        FakeSegmentInfos segInfos = {{.id = 0, .isMerged = true, .docCount = 10, .deleteDocCount = 0},
                                     {.id = 1, .isMerged = true, .docCount = 10, .deleteDocCount = 0},
                                     {.id = 2, .isMerged = true, .docCount = 10, .deleteDocCount = 0},
                                     {.id = 3, .isMerged = true, .docCount = 10, .deleteDocCount = 0}};
        TestNormalTabletOptimizeMergeStrategy({{0, 1}, {2, 3}}, "after-merge-max-doc-count=20", segInfos,
                                              /*isOptimizeMerge=*/false);
    }
}

TEST_F(NormalTabletOptimizeMergeStrategyTest, TestCaseForOptimizeWithAfterMergeMaxDocCount)
{
    std::string strategyString = "after-merge-max-doc-count=10";
    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = false, .docCount = 9, .deleteDocCount = 1},
                                 {.id = 1, .isMerged = false, .docCount = 5, .deleteDocCount = 3},
                                 {.id = 2, .isMerged = false, .docCount = 4, .deleteDocCount = 2},
                                 {.id = 3, .isMerged = false, .docCount = 3, .deleteDocCount = 1},
                                 {.id = 4, .isMerged = false, .docCount = 7, .deleteDocCount = 0},
                                 {.id = 5, .isMerged = false, .docCount = 15, .deleteDocCount = 2},
                                 {.id = 6, .isMerged = false, .docCount = 4, .deleteDocCount = 1},
                                 {.id = 7, .isMerged = false, .docCount = 8, .deleteDocCount = 0}};
    {
        TestNormalTabletOptimizeMergeStrategy({{0, 1}, {2, 3, 4}, {5}, {6, 7}}, strategyString, segInfos,
                                              /*isOptimizeMerge=*/false);
    }
    {
        TestNormalTabletOptimizeMergeStrategy({{0, 1}, {2, 3, 4}, {5}, {6, 7}}, strategyString, segInfos,
                                              /*isOptimizeMerge=*/true);
    }
}

}} // namespace indexlibv2::table
