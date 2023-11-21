#include "indexlib/index/common/PlainDocMapper.h"

#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/mock/FakeDiskSegment.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class PlainDocMapperTest : public TESTBASE
{
public:
    PlainDocMapperTest() = default;
    ~PlainDocMapperTest() = default;

private:
    std::shared_ptr<framework::SegmentMeta> MakeSegmentMeta(segmentid_t segmentId, size_t docCount)
    {
        auto segmentMeta = std::make_shared<framework::SegmentMeta>(segmentId);
        segmentMeta->segmentInfo->docCount = docCount;
        return segmentMeta;
    }
    IIndexMerger::SourceSegment MakeSourceSegment(segmentid_t segmentId, size_t docCount, docid64_t baseDocid)
    {
        IIndexMerger::SourceSegment srcSegment;
        srcSegment.baseDocid = baseDocid;
        auto segmentMeta = MakeSegmentMeta(segmentId, docCount);
        auto fakeSegment = std::make_shared<framework::FakeDiskSegment>(*segmentMeta);
        srcSegment.segment = fakeSegment;
        return srcSegment;
    }

public:
    void setUp() override;
    void tearDown() override;
};

void PlainDocMapperTest::setUp() {}

void PlainDocMapperTest::tearDown() {}

TEST_F(PlainDocMapperTest, testUsage)
{
    IIndexMerger::SegmentMergeInfos segmentMergeInfos;
    segmentMergeInfos.srcSegments.push_back(MakeSourceSegment(0, 100, 0));
    segmentMergeInfos.srcSegments.push_back(MakeSourceSegment(1, 200, 100));
    segmentMergeInfos.srcSegments.push_back(MakeSourceSegment(2, 100, 300));
    segmentMergeInfos.srcSegments.push_back(MakeSourceSegment(3, 100, 400));

    segmentMergeInfos.targetSegments.push_back(MakeSegmentMeta(4, 300));
    segmentMergeInfos.targetSegments.push_back(MakeSegmentMeta(5, 200));
    PlainDocMapper docMapper(segmentMergeInfos);

    ASSERT_EQ(300, docMapper.GetTargetSegmentDocCount(4));
    ASSERT_EQ(200, docMapper.GetTargetSegmentDocCount(5));
    ASSERT_EQ((std::pair<segmentid_t, docid32_t>(4, 0)), docMapper.Map(0));
    ASSERT_EQ((std::pair<segmentid_t, docid32_t>(4, 50)), docMapper.Map(50));
    ASSERT_EQ((std::pair<segmentid_t, docid32_t>(4, 130)), docMapper.Map(130));
    ASSERT_EQ((std::pair<segmentid_t, docid32_t>(5, 30)), docMapper.Map(330));
    ASSERT_EQ((std::pair<segmentid_t, docid32_t>(4, 299)), docMapper.Map(299));
    ASSERT_EQ((std::pair<segmentid_t, docid32_t>(5, 0)), docMapper.Map(300));

    ASSERT_EQ((std::pair<segmentid_t, docid32_t>(0, 0)), docMapper.ReverseMap(0));
    ASSERT_EQ((std::pair<segmentid_t, docid32_t>(0, 50)), docMapper.ReverseMap(50));
    ASSERT_EQ((std::pair<segmentid_t, docid32_t>(1, 10)), docMapper.ReverseMap(110));
    ASSERT_EQ((std::pair<segmentid_t, docid32_t>(2, 0)), docMapper.ReverseMap(300));
    ASSERT_EQ((std::pair<segmentid_t, docid32_t>(3, 50)), docMapper.ReverseMap(450));

    for (docid64_t i = 0; i < 500; ++i) {
        ASSERT_EQ(i, docMapper.GetNewId(i));
    }
}

} // namespace indexlibv2::index
