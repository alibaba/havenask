#include "indexlib/table/normal_table/index_task/merger/PriorityQueueMergeStrategy.h"

#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/table/index_task/merger/test/MergeTestHelper.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace table {

class PriorityQueueMergeStrategyTest : public TESTBASE
{
public:
    PriorityQueueMergeStrategyTest() {}
    ~PriorityQueueMergeStrategyTest() {}

public:
    void setUp() override {};
    void tearDown() override {};

private:
    void InnerTestExtractParameter(const std::string& mergeParams, bool expectBadParam);
};

TEST_F(PriorityQueueMergeStrategyTest, TestSimpleProcess)
{
    std::string mergeParams = "max-doc-count=100;max-valid-doc-count=80|"
                              "priority-feature=doc-count#asc;conflict-segment-count=5;conflict-delete-percent=20|"
                              "max-merged-segment-doc-count=50;max-total-merge-doc-count=50";
    auto mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(),
        /*expectSrcSegments=*/ {{0}}, mergeParams,
        {{.id = 0, .isMerged = true, .docCount = 20, .deleteDocCount = 10, .timestamp = 1}});
}

TEST_F(PriorityQueueMergeStrategyTest, TestInputLimits)
{
    // test max doc count
    auto mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1, 2}},
                                       "max-doc-count=20|conflict-segment-count=1|",
                                       {{.id = 0, .isMerged = true, .docCount = 20, .deleteDocCount = 0},
                                        {.id = 1, .isMerged = true, .docCount = 0, .deleteDocCount = 0},
                                        {.id = 2, .isMerged = true, .docCount = 10, .deleteDocCount = 0},
                                        {.id = 3, .isMerged = true, .docCount = 21, .deleteDocCount = 0}});
    // test max valid doc count
    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1, 2, 4}},
                                       "max-valid-doc-count=20|conflict-segment-count=1|",
                                       {{.id = 0, .isMerged = true, .docCount = 20, .deleteDocCount = 0},
                                        {.id = 1, .isMerged = true, .docCount = 0, .deleteDocCount = 0},
                                        {.id = 2, .isMerged = true, .docCount = 10, .deleteDocCount = 0},
                                        {.id = 3, .isMerged = true, .docCount = 21, .deleteDocCount = 0},
                                        {.id = 4, .isMerged = true, .docCount = 21, .deleteDocCount = 1}});

    // test input limit combo
    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{1, 2, 5}},
                                       "max-doc-count=20;max-valid-doc-count=18|conflict-segment-count=1|",
                                       {{.id = 0, .isMerged = true, .docCount = 20, .deleteDocCount = 0},
                                        {.id = 1, .isMerged = true, .docCount = 0, .deleteDocCount = 0},
                                        {.id = 2, .isMerged = true, .docCount = 10, .deleteDocCount = 0},
                                        {.id = 3, .isMerged = true, .docCount = 21, .deleteDocCount = 0},
                                        {.id = 4, .isMerged = true, .docCount = 21, .deleteDocCount = 5},
                                        {.id = 5, .isMerged = true, .docCount = 19, .deleteDocCount = 1},
                                        {.id = 6, .isMerged = true, .docCount = 19, .deleteDocCount = 0}});
}

TEST_F(PriorityQueueMergeStrategyTest, TestSelectMergeSegments)
{
    // test conflict segments
    auto mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1, 2, 3}},
                                       "|conflict-segment-count=2|",
                                       {{.id = 0, .isMerged = true, .docCount = 20, .deleteDocCount = 0},
                                        {.id = 1, .isMerged = true, .docCount = 0, .deleteDocCount = 0},
                                        {.id = 2, .isMerged = true, .docCount = 10, .deleteDocCount = 0},
                                        {.id = 3, .isMerged = true, .docCount = 21, .deleteDocCount = 0}});
    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1, 2}},
                                       "|conflict-segment-count=3|",
                                       {{.id = 0, .isMerged = true, .docCount = 20, .deleteDocCount = 0},
                                        {.id = 1, .isMerged = true, .docCount = 0, .deleteDocCount = 0},
                                        {.id = 2, .isMerged = true, .docCount = 10, .deleteDocCount = 0},
                                        {.id = 3, .isMerged = true, .docCount = 21, .deleteDocCount = 0}});

    // test delete percent
    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0}},
                                       "|conflict-delete-percent=50|",
                                       {{.isMerged = true, .docCount = 20, .deleteDocCount = 11},
                                        {.isMerged = true, .docCount = 0, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 10, .deleteDocCount = 5},
                                        {.isMerged = true, .docCount = 21, .deleteDocCount = 10}});

    // test conflict segment & delete percent
    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1, 2, 3, 4}},
                                       "|conflict-segment-count=2;conflict-delete-percent=50|",
                                       {{.isMerged = true, .docCount = 20, .deleteDocCount = 11},
                                        {.isMerged = true, .docCount = 0, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 10, .deleteDocCount = 5},
                                        {.isMerged = true, .docCount = 21, .deleteDocCount = 10},
                                        {.isMerged = true, .docCount = 28, .deleteDocCount = 15}});

    std::vector<FakeSegmentInfo> segmentInfos = {
        {.id = 0, .isMerged = true, .docCount = 20, .deleteDocCount = 10, .timestamp = 5},
        {.id = 1, .isMerged = true, .docCount = 0, .deleteDocCount = 0, .timestamp = 3},
        {.id = 2, .isMerged = true, .docCount = 40, .deleteDocCount = 20, .timestamp = 2},
        {.id = 3, .isMerged = true, .docCount = 30, .deleteDocCount = 15, .timestamp = 4},
        {.id = 4, .isMerged = true, .docCount = 28, .deleteDocCount = 15, .timestamp = 1}};
    // test priority queue: default doc count
    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1}, {4}, {3}, {2}},
                                       "|conflict-segment-count=1;conflict-delete-percent=0"
                                       "|max-merged-segment-doc-count=20",
                                       segmentInfos);

    // test priority queue: timestamp
    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {{1, 4}, {2}, {3}, {0}},
        "|priority-feature=timestamp#asc;conflict-segment-count=1;conflict-delete-percent=0"
        "|max-merged-segment-doc-count=20",
        segmentInfos);

    // test priority queue: delete doc
    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {{1, 2}, {3}, {4}, {0}},
        "|priority-feature=delete-doc-count#desc;conflict-segment-count=1;conflict-delete-percent=0"
        "|max-merged-segment-doc-count=20",
        segmentInfos);

    // test priority queue: valid doc count
    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {{0, 1}, {4}, {3}, {2}},
        "|priority-feature=valid-doc-count#asc;conflict-segment-count=1;conflict-delete-percent=0"
        "|max-merged-segment-doc-count=20",
        segmentInfos);
}

TEST_F(PriorityQueueMergeStrategyTest, TestOutputLimits)
{
    // test max merged segment doc count
    auto mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1, 3, 4, 5}, {2, 6}},
                                       "|conflict-segment-count=1|max-merged-segment-doc-count=6",
                                       {{.isMerged = true, .docCount = 1, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 2, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 3, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 2, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 1, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 0, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 3, .deleteDocCount = 0}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{1, 2}},
                                       "|conflict-segment-count=1|max-merged-segment-doc-count=5",
                                       {{.isMerged = true, .docCount = 5, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 2, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 3, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 4, .deleteDocCount = 0}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {{1, 2}, {0}},
        "|priority-feature=valid-doc-count;conflict-segment-count=1;conflict-delete-percent=40"
        "|max-merged-segment-doc-count=5",
        {{.isMerged = true, .docCount = 10, .deleteDocCount = 5},
         {.isMerged = true, .docCount = 2, .deleteDocCount = 0},
         {.isMerged = true, .docCount = 6, .deleteDocCount = 3},
         {.isMerged = true, .docCount = 4, .deleteDocCount = 0}});

    // test max total merge doc count
    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1, 3, 4, 5}},
                                       "|conflict-segment-count=1|max-total-merge-doc-count=6",
                                       {{.isMerged = true, .docCount = 1, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 2, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 3, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 2, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 1, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 0, .deleteDocCount = 0},
                                        {.isMerged = true, .docCount = 3, .deleteDocCount = 0}});

    // test output combo
    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {{0, 1, 4, 5}, {6}},
        "|priority-feature=valid-doc-count#asc;conflict-segment-count=1;conflict-delete-percent=40|"
        "max-merged-segment-doc-count=4;max-total-merge-doc-count=7",
        {{.isMerged = true, .docCount = 1, .deleteDocCount = 0},
         {.isMerged = true, .docCount = 2, .deleteDocCount = 0},
         {.isMerged = true, .docCount = 3, .deleteDocCount = 0},
         {.isMerged = true, .docCount = 2, .deleteDocCount = 0},
         {.isMerged = true, .docCount = 1, .deleteDocCount = 0},
         {.isMerged = true, .docCount = 0, .deleteDocCount = 0},
         {.isMerged = true, .docCount = 6, .deleteDocCount = 3}});
}

TEST_F(PriorityQueueMergeStrategyTest, TestMaxSegmentSize)
{
    auto mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/ {{0}}, "max-segment-size=150|conflict-delete-percent=50|",
        {{.isMerged = true, .docCount = 100, .deleteDocCount = 100, .segmentSize = 100},
         {.isMerged = true, .docCount = 200, .deleteDocCount = 200, .segmentSize = 200}});
}

TEST_F(PriorityQueueMergeStrategyTest, TestMaxValidSegmentSize)
{
    auto mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {{0}}, "max-valid-segment-size=90|conflict-delete-percent=20|",
        {{.isMerged = true, .docCount = 200, .deleteDocCount = 100, .segmentSize = 100},
         {.isMerged = true, .docCount = 200, .deleteDocCount = 100, .segmentSize = 200}});
}

TEST_F(PriorityQueueMergeStrategyTest, TestMaxTotalMergeSize)
{
    auto mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {{0}}, "max-total-merge-size=150|conflict-delete-percent=50|",
        {{.isMerged = true, .docCount = 100, .deleteDocCount = 100, .segmentSize = 100},
         {.isMerged = true, .docCount = 200, .deleteDocCount = 200, .segmentSize = 200}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{1, 2}},
                                       "max-total-merge-size=300|conflict-segment-count=1|",
                                       {{.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 400},
                                        {.isMerged = true, .docCount = 200, .deleteDocCount = 0, .segmentSize = 200},
                                        {.isMerged = true, .docCount = 300, .deleteDocCount = 0, .segmentSize = 100}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1}, {2, 3}},
                                       "max-total-merge-size=400|conflict-segment-count=1|max-merged-segment-size=200",
                                       {{.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100}});
}

TEST_F(PriorityQueueMergeStrategyTest, TestMaxMergedSegmentSize)
{
    auto mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1}, {2, 3}},
                                       "|conflict-segment-count=1|max-merged-segment-size=250",
                                       {{.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {}, "|conflict-delete-percent=50|max-merged-segment-size=99",
        {{.isMerged = true, .docCount = 400, .deleteDocCount = 300, .segmentSize = 400}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 3}, {1, 2}},
                                       "|conflict-segment-count=1|max-merged-segment-size=500",
                                       {{.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 400},
                                        {.isMerged = true, .docCount = 101, .deleteDocCount = 0, .segmentSize = 300},
                                        {.isMerged = true, .docCount = 102, .deleteDocCount = 0, .segmentSize = 200},
                                        {.isMerged = true, .docCount = 103, .deleteDocCount = 0, .segmentSize = 100}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {{0, 2}}, "|conflict-segment-count=1|max-merged-segment-size=100",
        {{.isMerged = true, .docCount = 100, .deleteDocCount = 90, .segmentSize = 100},
         {.isMerged = true, .docCount = 200, .deleteDocCount = 0, .segmentSize = 200},
         {.isMerged = true, .docCount = 300, .deleteDocCount = 250, .segmentSize = 300}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {{0, 2}, {1, 3}}, "|conflict-segment-count=1|max-merged-segment-size=5120",
        {{.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 2048},
         {.isMerged = true, .docCount = 200, .deleteDocCount = 10, .segmentSize = 3872},
         {.isMerged = true, .docCount = 300, .deleteDocCount = 10, .segmentSize = 2848},
         {.isMerged = true, .docCount = 400, .deleteDocCount = 10, .segmentSize = 1024}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {{0, 2}, {1, 3}}, "|conflict-segment-count=1|max-merged-segment-size=5120",
        {{.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 2048},
         {.isMerged = true, .docCount = 200, .deleteDocCount = 10, .segmentSize = 3872},
         {.isMerged = true, .docCount = 300, .deleteDocCount = 10, .segmentSize = 2848},
         {.isMerged = true, .docCount = 400, .deleteDocCount = 10, .segmentSize = 1024}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {{1, 2}}, "|conflict-segment-count=1|max-merged-segment-size=5120",
        {{.isMerged = true, .docCount = 85983314, .deleteDocCount = 0, .segmentSize = 204800},
         {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 200},
         {.isMerged = true, .docCount = 200, .deleteDocCount = 0, .segmentSize = 200}});
}

TEST_F(PriorityQueueMergeStrategyTest, TestConflictSegmentCount)
{
    auto mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1}},
                                       "|conflict-segment-count=0|max-total-merged-size=250",
                                       {{.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100}});
}

TEST_F(PriorityQueueMergeStrategyTest, TestMaxTotalMergedSize)
{
    auto mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1}},
                                       "|conflict-segment-count=1|max-total-merged-size=250",
                                       {{.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {{0, 1}}, "|conflict-segment-count=1|max-total-merged-size=250;max-merged-segment-size=500",
        {{.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
         {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
         {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100},
         {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 100}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(
        mergeStrategy.get(), /*expectSrcSegments=*/
        {{0, 2}}, "|conflict-segment-count=1|max-total-merged-size=100",
        {{.isMerged = true, .docCount = 100, .deleteDocCount = 90, .segmentSize = 100},
         {.isMerged = true, .docCount = 200, .deleteDocCount = 0, .segmentSize = 200},
         {.isMerged = true, .docCount = 300, .deleteDocCount = 250, .segmentSize = 300}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 2}, {1, 3}},
                                       "|conflict-segment-count=1|max-total-merged-size=12;max-merged-segment-size=6",
                                       {{.isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 5},
                                        {.isMerged = true, .docCount = 2, .deleteDocCount = 0, .segmentSize = 2},
                                        {.isMerged = true, .docCount = 3, .deleteDocCount = 0, .segmentSize = 1},
                                        {.isMerged = true, .docCount = 4, .deleteDocCount = 0, .segmentSize = 4},
                                        {.isMerged = true, .docCount = 5, .deleteDocCount = 0, .segmentSize = 3},
                                        {.isMerged = true, .docCount = 6, .deleteDocCount = 0, .segmentSize = 2}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 2}, {1, 3}},
                                       "|conflict-segment-count=1|max-total-merged-size=12;max-merged-segment-size=6",
                                       {{.isMerged = true, .docCount = 1, .deleteDocCount = 0, .segmentSize = 5},
                                        {.isMerged = true, .docCount = 2, .deleteDocCount = 0, .segmentSize = 2},
                                        {.isMerged = true, .docCount = 3, .deleteDocCount = 0, .segmentSize = 1},
                                        {.isMerged = true, .docCount = 4, .deleteDocCount = 0, .segmentSize = 4},
                                        {.isMerged = true, .docCount = 5, .deleteDocCount = 0, .segmentSize = 3},
                                        {.isMerged = true, .docCount = 6, .deleteDocCount = 0, .segmentSize = 2}});

    mergeStrategy = std::make_unique<PriorityQueueMergeStrategy>();
    MergeTestHelper::TestMergeStrategy(mergeStrategy.get(), /*expectSrcSegments=*/ {{0, 1}},
                                       "|conflict-segment-count=1|max-total-merged-size=8192",
                                       {{.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 3072},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 3072},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 3072},
                                        {.isMerged = true, .docCount = 100, .deleteDocCount = 0, .segmentSize = 3072}});
}

TEST_F(PriorityQueueMergeStrategyTest, TestExtractParameter)
{
    // test normal
    InnerTestExtractParameter("||", /*expectBadParam=*/false);
    InnerTestExtractParameter("max-doc-count=1000;max-valid-doc-count=20000|"
                              "conflict-segment-count=10;conflict-delete-percent=50|",
                              /*expectBadParam=*/false);

    InnerTestExtractParameter("max-doc-count=1000;max-valid-doc-count=20000|"
                              "priority-feature=doc-count#desc;conflict-segment-count=10;conflict-delete-percent=50|"
                              "max-merged-segment-doc-count=50000;max-total-merge-doc-count=100000",
                              /*expectBadParam=*/false);

    // not-exist-item might be configs for other merge strategy.
    InnerTestExtractParameter("not-exist-item=1000||", /*expectBadParam=*/false);
    InnerTestExtractParameter("|priority-feature=doc-count#asc;not-exist-item=123|", /*expectBadParam=*/false);
    InnerTestExtractParameter("||not-exist-item=123", /*expectBadParam=*/false);

    // test invalid
    InnerTestExtractParameter("max-doc-count=abc||", /*expectBadParam=*/true);
    InnerTestExtractParameter("max-valid-doc-count=abc||", /*expectBadParam=*/true);
    InnerTestExtractParameter("|priority-feature=not_support|", /*expectBadParam=*/true);
    InnerTestExtractParameter("|priority-feature=doc-count#abc|", /*expectBadParam=*/true);
    InnerTestExtractParameter("|priority-feature=doc-count#asc;conflict-segment-count=abc|", /*expectBadParam=*/true);
    InnerTestExtractParameter("|priority-feature=doc-count#asc;conflict-delete-percent=150|", /*expectBadParam=*/true);
    InnerTestExtractParameter("|priority-feature=doc-count#asc;conflict-delete-percent=-3|", /*expectBadParam=*/true);
    InnerTestExtractParameter("||max-merged-segment-doc-count=abc", /*expectBadParam=*/true);
    InnerTestExtractParameter("||max-total-merge-doc-count=abc", /*expectBadParam=*/true);
}

void PriorityQueueMergeStrategyTest::InnerTestExtractParameter(const std::string& mergeParams, bool expectBadParam)
{
    std::vector<std::string> param = autil::StringUtil::split(mergeParams, "|", /*ignoreEmpty=*/false);
    const std::string inputLimit = param[0];
    const std::string strategyConditions = param[1];
    const std::string outputLimit = param[2];

    config::MergeStrategyParameter parameter =
        config::MergeStrategyParameter::Create(inputLimit, strategyConditions, outputLimit);

    auto [status, params] = PriorityQueueMergeStrategy::ExtractParams(parameter);
    if (expectBadParam) {
        EXPECT_FALSE(status.IsOK());
        EXPECT_TRUE(status.IsInvalidArgs());
    } else {
        EXPECT_TRUE(status.IsOK());
    }
}

}} // namespace indexlibv2::table
