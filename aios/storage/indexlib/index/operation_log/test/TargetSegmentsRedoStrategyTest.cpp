#include "indexlib/index/operation_log/TargetSegmentsRedoStrategy.h"

#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/mock/FakeSegment.h"
#include "indexlib/index/operation_log/RemoveOperation.h"
#include "indexlib/index/operation_log/UpdateFieldOperation.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib::index {

class TargetSegmentsRedoStrategyTest : public INDEXLIB_TESTBASE
{
public:
    TargetSegmentsRedoStrategyTest();
    ~TargetSegmentsRedoStrategyTest();

    DECLARE_CLASS_NAME(TargetSegmentsRedoStrategyTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestInvalidArgs();
    void TestRedoDataConsistent();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TargetSegmentsRedoStrategyTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(TargetSegmentsRedoStrategyTest, TestInvalidArgs);
INDEXLIB_UNIT_TEST_CASE(TargetSegmentsRedoStrategyTest, TestRedoDataConsistent);
AUTIL_LOG_SETUP(indexlib.index, TargetSegmentsRedoStrategyTest);

TargetSegmentsRedoStrategyTest::TargetSegmentsRedoStrategyTest() {}

TargetSegmentsRedoStrategyTest::~TargetSegmentsRedoStrategyTest() {}

void TargetSegmentsRedoStrategyTest::CaseSetUp() {}

void TargetSegmentsRedoStrategyTest::CaseTearDown() {}

void TargetSegmentsRedoStrategyTest::TestSimpleProcess()
{
    indexlibv2::framework::TabletData tabletData("demo");
    indexlibv2::framework::Version version;
    std::vector<std::shared_ptr<indexlibv2::framework::Segment>> segments;
    for (size_t i = 0; i < 10; ++i) {
        indexlibv2::framework::SegmentMeta segMeta(i);
        segMeta.segmentInfo->docCount = 2;
        auto segment = std::make_shared<indexlibv2::framework::FakeSegment>(
            indexlibv2::framework::Segment::SegmentStatus::ST_BUILT);
        segment->TEST_SetSegmentMeta(segMeta);
        segments.push_back(segment);
        version.AddSegment(i);
    }

    ASSERT_TRUE(
        tabletData.Init(std::move(version), segments, std::make_shared<indexlibv2::framework::ResourceMap>()).IsOK());
    {
        std::vector<segmentid_t> targetSegments;
        TargetSegmentsRedoStrategy strategy(true);
        ASSERT_TRUE(strategy.Init(&tabletData, 0, targetSegments).IsOK());
        std::vector<std::pair<docid_t, docid_t>> targetDocRange;
        RemoveOperation<uint64_t> op({0, 0, 0, 0});
        ASSERT_FALSE(strategy.NeedRedo(0, &op, targetDocRange));
    }
    {
        std::vector<segmentid_t> targetSegments({1, 2, 3});
        TargetSegmentsRedoStrategy strategy(true);
        ASSERT_TRUE(strategy.Init(&tabletData, 0, targetSegments).IsOK());
        std::vector<std::pair<docid_t, docid_t>> targetDocRange, expected({{2, 8}});
        RemoveOperation<uint64_t> op({0, 0, 0, 0});
        ASSERT_TRUE(strategy.NeedRedo(0, &op, targetDocRange));
        ASSERT_EQ(expected, targetDocRange);
    }
    {
        std::vector<segmentid_t> targetSegments({1, 3});
        TargetSegmentsRedoStrategy strategy(true);
        ASSERT_TRUE(strategy.Init(&tabletData, 0, targetSegments).IsOK());
        std::vector<std::pair<docid_t, docid_t>> targetDocRange, expected({{2, 4}, {6, 8}});
        RemoveOperation<uint64_t> op({0, 0, 0, 0});
        ASSERT_TRUE(strategy.NeedRedo(0, &op, targetDocRange));
        ASSERT_EQ(expected, targetDocRange);
    }
}

void TargetSegmentsRedoStrategyTest::TestRedoDataConsistent()
{
    indexlibv2::framework::TabletData tabletData("demo");
    indexlibv2::framework::Version version;
    std::vector<std::shared_ptr<indexlibv2::framework::Segment>> segments;
    for (size_t i = 0; i < 10; ++i) {
        indexlibv2::framework::SegmentMeta segMeta(i);
        segMeta.segmentInfo->docCount = 2;
        auto segment = std::make_shared<indexlibv2::framework::FakeSegment>(
            indexlibv2::framework::Segment::SegmentStatus::ST_BUILT);
        segment->TEST_SetSegmentMeta(segMeta);
        segments.push_back(segment);
        version.AddSegment(i);
    }

    ASSERT_TRUE(
        tabletData.Init(std::move(version), segments, std::make_shared<indexlibv2::framework::ResourceMap>()).IsOK());

    auto check = [&](bool isConsistent, segmentid_t redoSegmentId,
                     const std::vector<std::pair<docid_t, docid_t>>& expectedUpdate,
                     const std::vector<std::pair<docid_t, docid_t>>& expectedDelete) {
        std::vector<segmentid_t> targetSegments({1, 2, 3});
        TargetSegmentsRedoStrategy strategy(isConsistent);
        ASSERT_TRUE(strategy.Init(&tabletData, redoSegmentId, targetSegments).IsOK());
        std::vector<std::pair<docid_t, docid_t>> targetDocRange;
        RemoveOperation<uint64_t> deleteOp({0, 0, 0, 0});
        ASSERT_TRUE(strategy.NeedRedo(1, &deleteOp, targetDocRange));
        ASSERT_EQ(expectedDelete, targetDocRange);

        UpdateFieldOperation<uint64_t> updateOp({0, 0, 0, 0});
        ASSERT_TRUE(strategy.NeedRedo(1, &updateOp, targetDocRange));
        ASSERT_EQ(expectedUpdate, targetDocRange);
    };
    check(false, 1, {{2, 8}}, {{0, 2}});
    check(true, 1, {{2, 8}}, {{2, 8}});

    check(false, 2, {{2, 8}}, {{0, 4}});
    check(true, 2, {{2, 8}}, {{2, 8}});

    check(false, 5, {{2, 8}}, {{0, 10}});
    check(true, 5, {{2, 8}}, {{2, 8}});
}

void TargetSegmentsRedoStrategyTest::TestInvalidArgs()
{
    indexlibv2::framework::TabletData tabletData("demo");
    indexlibv2::framework::Version version;
    std::vector<std::shared_ptr<indexlibv2::framework::Segment>> segments;
    for (size_t i = 0; i < 10; ++i) {
        indexlibv2::framework::SegmentMeta segMeta(i);
        segMeta.segmentInfo->docCount = 2;
        auto segment = std::make_shared<indexlibv2::framework::FakeSegment>(
            indexlibv2::framework::Segment::SegmentStatus::ST_BUILT);
        segment->TEST_SetSegmentMeta(segMeta);
        segments.push_back(segment);
        version.AddSegment(i);
    }

    indexlibv2::framework::SegmentMeta segMeta(10);
    segMeta.segmentInfo->docCount = 2;
    auto segment = std::make_shared<indexlibv2::framework::FakeSegment>(
        indexlibv2::framework::Segment::SegmentStatus::ST_BUILDING);
    segment->TEST_SetSegmentMeta(segMeta);
    segments.push_back(segment);

    ASSERT_TRUE(
        tabletData.Init(std::move(version), segments, std::make_shared<indexlibv2::framework::ResourceMap>()).IsOK());
    {
        std::vector<segmentid_t> targetSegments;
        targetSegments.push_back(10);
        TargetSegmentsRedoStrategy strategy(true);
        ASSERT_FALSE(strategy.Init(&tabletData, 0, targetSegments).IsOK());
    }
}

} // namespace indexlib::index
