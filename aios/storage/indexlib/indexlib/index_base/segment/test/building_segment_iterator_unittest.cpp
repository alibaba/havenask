#include "indexlib/index_base/segment/test/building_segment_iterator_unittest.h"

#include "indexlib/partition/segment/test/segment_iterator_helper.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::partition;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, BuildingSegmentIteratorTest);

BuildingSegmentIteratorTest::BuildingSegmentIteratorTest() {}

BuildingSegmentIteratorTest::~BuildingSegmentIteratorTest() {}

void BuildingSegmentIteratorTest::CaseSetUp() {}

void BuildingSegmentIteratorTest::CaseTearDown() {}

void BuildingSegmentIteratorTest::TestSimpleProcess()
{
    // segmentId:docCount;...,...
    string inMemInfoStr = "0:10;1:20;2:30";
    docid_t baseDocId = 25;

    vector<InMemorySegmentPtr> inMemSegments;
    SegmentIteratorHelper::PrepareInMemSegments(inMemInfoStr, inMemSegments);

    BuildingSegmentIterator iter;
    iter.Init(inMemSegments, baseDocId);
    CheckIterator(iter, inMemInfoStr, baseDocId);
}

void BuildingSegmentIteratorTest::CheckIterator(BuildingSegmentIterator& iter, const string& inMemInfoStr,
                                                docid_t baseDocId)
{
    ASSERT_EQ(SIT_BUILDING, iter.GetType());
    vector<vector<int32_t>> segmentInfoVec;
    StringUtil::fromString(inMemInfoStr, segmentInfoVec, ":", ";");

    for (size_t i = 0; i < segmentInfoVec.size(); i++) {
        assert(segmentInfoVec[i].size() == 2);
        ASSERT_TRUE(iter.IsValid());

        ASSERT_EQ(baseDocId, iter.GetBaseDocId());
        ASSERT_EQ((segmentid_t)segmentInfoVec[i][0], iter.GetSegmentId());
        ASSERT_EQ((segmentid_t)segmentInfoVec[i][0], iter.GetSegmentData().GetSegmentId());
        ASSERT_EQ(segmentInfoVec[i][1], iter.GetSegmentData().GetSegmentInfo()->docCount);

        baseDocId += segmentInfoVec[i][1];
        iter.MoveToNext();
    }
    ASSERT_FALSE(iter.IsValid());
}
}} // namespace indexlib::index_base
