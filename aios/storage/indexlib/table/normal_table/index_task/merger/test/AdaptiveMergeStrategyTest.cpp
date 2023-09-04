#include "indexlib/table/normal_table/index_task/merger/AdaptiveMergeStrategy.h"

#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/test/MergeTestHelper.h"
#include "unittest/unittest.h"

namespace indexlibv2::table {

class AdaptiveMergeStrategyTest : public TESTBASE
{
public:
    AdaptiveMergeStrategyTest() = default;
    ~AdaptiveMergeStrategyTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void AdaptiveMergeStrategyTest::setUp() {}

void AdaptiveMergeStrategyTest::tearDown() {}

TEST_F(AdaptiveMergeStrategyTest, testUsage)
{
    auto mergeStrategy = std::make_unique<AdaptiveMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(),
        /*expectSrcSegments=*/ {{0, 13}}, "",
        {{.id = 0, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 2500, .segmentSize = 1024},
         {.id = 1, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 2000, .segmentSize = 1024},
         {.id = 2, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1500, .segmentSize = 1024},
         {.id = 3, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1500, .segmentSize = 1024},
         {.id = 4, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 5, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 6, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 7, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 8, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 9, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 10, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 11, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 12, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 13, .isMerged = true, .docCount = 1024, .deleteDocCount = 50, .segmentSize = 10}});
}

TEST_F(AdaptiveMergeStrategyTest, testEmergency)
{
    auto mergeStrategy = std::make_unique<AdaptiveMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(),
        /*expectSrcSegments=*/ {{0}, {7, 8, 9, 10}}, "",
        {{.id = 0, .isMerged = true, .docCount = 1 << 26, .deleteDocCount = (1 << 25) + (1 << 24), .segmentSize = 4096},
         {.id = 1, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 2000, .segmentSize = 1024},
         {.id = 2, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1500, .segmentSize = 1024},
         {.id = 3, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1500, .segmentSize = 1024},
         {.id = 4, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 5, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 6, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 7, .isMerged = true, .docCount = 1024, .deleteDocCount = 50, .segmentSize = 10},
         {.id = 8, .isMerged = true, .docCount = 1024, .deleteDocCount = 50, .segmentSize = 10},
         {.id = 9, .isMerged = true, .docCount = 1024, .deleteDocCount = 50, .segmentSize = 10},
         {.id = 10, .isMerged = true, .docCount = 1024, .deleteDocCount = 50, .segmentSize = 10}});
}

TEST_F(AdaptiveMergeStrategyTest, testRealtime)
{
    auto mergeStrategy = std::make_unique<AdaptiveMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(),
        /*expectSrcSegments=*/ {{1, 7, 8, 9}, {10, 11, 12, 13}}, "",
        {{.id = 0, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 4096},
         {.id = 1, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 2000, .segmentSize = 1024},
         {.id = 2, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1500, .segmentSize = 1024},
         {.id = 3, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1500, .segmentSize = 1024},
         {.id = 4, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 5, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 6, .isMerged = true, .docCount = 1 << 21, .deleteDocCount = 1000, .segmentSize = 1024},
         {.id = 7, .isMerged = true, .docCount = 1024, .deleteDocCount = 50, .segmentSize = 10},
         {.id = 8, .isMerged = true, .docCount = 1024, .deleteDocCount = 50, .segmentSize = 10},
         {.id = 9, .isMerged = true, .docCount = 1024, .deleteDocCount = 50, .segmentSize = 10},
         {.id = 10, .isMerged = false, .docCount = 1024, .deleteDocCount = 0, .segmentSize = 10},
         {.id = 11, .isMerged = false, .docCount = 1024, .deleteDocCount = 0, .segmentSize = 10},
         {.id = 12, .isMerged = false, .docCount = 1024, .deleteDocCount = 0, .segmentSize = 10},
         {.id = 13, .isMerged = false, .docCount = 1024, .deleteDocCount = 0, .segmentSize = 10}});
}

TEST_F(AdaptiveMergeStrategyTest, testManySegments)
{
    auto mergeStrategy = std::make_unique<AdaptiveMergeStrategy>();
    std::vector<FakeSegmentInfo> segmentInfos;
    for (segmentid_t i = 0; i < 60; ++i) {
        segmentInfos.push_back({.id = i, .isMerged = true, .docCount = 1 << 25, .segmentSize = 10240});
    }
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(),
                                       /*expectSrcSegments=*/ {{57, 58, 59}}, "", segmentInfos);
}

TEST_F(AdaptiveMergeStrategyTest, testManySegments_2)
{
    auto mergeStrategy = std::make_unique<AdaptiveMergeStrategy>();
    std::vector<FakeSegmentInfo> segmentInfos;
    for (segmentid_t i = 0; i < 120; ++i) {
        segmentInfos.push_back(
            {.id = i, .isMerged = true, .docCount = 1 << 24, .deleteDocCount = (120 - i) * 1000, .segmentSize = 10240});
    }
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(),
                                       /*expectSrcSegments=*/ {{0, 1, 2, 3, 4, 5}}, "", segmentInfos);
}
} // namespace indexlibv2::table
