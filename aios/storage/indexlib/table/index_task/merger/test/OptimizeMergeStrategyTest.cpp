#include "indexlib/table/index_task/merger/OptimizeMergeStrategy.h"

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
class OptimizeMergeStrategyTest : public TESTBASE
{
public:
    OptimizeMergeStrategyTest() {}
    ~OptimizeMergeStrategyTest() {}

private:
    void TestOptimizeMergeStrategy(const std::vector<std::vector<segmentid_t>>& expectSrcSegments,
                                   const std::string& rawStrategyParamString,
                                   const std::vector<FakeSegmentInfo>& segInfos, bool isOptimizeMerge);

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(OptimizeMergeStrategyTest, TestDocCount)
{
    std::string strategyString = "";
    FakeSegmentInfos segInfos = {{.id = 0, .isMerged = false, .docCount = 3, .deleteDocCount = 0},
                                 {.id = 1, .isMerged = false, .docCount = 2147483640, .deleteDocCount = 0},
                                 {.id = 2, .isMerged = false, .docCount = 3, .deleteDocCount = 0},
                                 {.id = 3, .isMerged = false, .docCount = 100, .deleteDocCount = 0},
                                 {.id = 4, .isMerged = false, .docCount = 200, .deleteDocCount = 0},
                                 {.id = 5, .isMerged = false, .docCount = 15, .deleteDocCount = 0},
                                 {.id = 6, .isMerged = false, .docCount = 4, .deleteDocCount = 0},
                                 {.id = 7, .isMerged = false, .docCount = 8, .deleteDocCount = 0}};
    OptimizeMergeStrategy ms;
    std::vector<std::vector<segmentid_t>> expectSrcSegments({{0, 1, 2}, {3, 4, 5, 6, 7}});
    MergeTestHelper::TestMergeStrategy(&ms, expectSrcSegments, strategyString, segInfos, true);
}

}} // namespace indexlibv2::table
