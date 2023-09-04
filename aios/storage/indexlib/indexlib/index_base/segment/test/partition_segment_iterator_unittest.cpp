#include "indexlib/index_base/segment/test/partition_segment_iterator_unittest.h"

#include "indexlib/partition/segment/test/segment_iterator_helper.h"

using namespace std;
using namespace autil;

using namespace indexlib::partition;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, PartitionSegmentIteratorTest);

PartitionSegmentIteratorTest::PartitionSegmentIteratorTest() {}

PartitionSegmentIteratorTest::~PartitionSegmentIteratorTest() {}

void PartitionSegmentIteratorTest::CaseSetUp() {}

void PartitionSegmentIteratorTest::CaseTearDown() {}

void PartitionSegmentIteratorTest::TestSimpleProcess()
{
    string builtInfoStr = "0:10;1:20;2:30";
    vector<SegmentData> segmentDatas;
    SegmentIteratorHelper::PrepareBuiltSegmentDatas(builtInfoStr, segmentDatas);

    string inMemInfoStr = "4:40;5:50;6:60";
    vector<InMemorySegmentPtr> inMemSegments;
    SegmentIteratorHelper::PrepareInMemSegments(inMemInfoStr, inMemSegments);

    InMemorySegmentPtr inMemSegment = CreateBuildingSegment(7, 70);
    PartitionSegmentIterator iter;
    iter.Init(segmentDatas, inMemSegments, inMemSegment, INVALID_SEGMENTID);

    CheckIterator(iter, "0:10:built;1:20:built;2:30:built;4:40:building;"
                        "5:50:building;6:60:building;7:70:building");
}

void PartitionSegmentIteratorTest::CheckIterator(PartitionSegmentIterator& iter, const string& segInfoStr)
{
    vector<vector<string>> segmentInfoVec;
    StringUtil::fromString(segInfoStr, segmentInfoVec, ":", ";");

    docid_t baseDocId = 0;
    for (size_t i = 0; i < segmentInfoVec.size(); i++) {
        assert(segmentInfoVec[i].size() == 3);
        ASSERT_TRUE(iter.IsValid());
        ASSERT_EQ(baseDocId, iter.GetBaseDocId());

        segmentid_t segId = INVALID_SEGMENTID;
        StringUtil::fromString(segmentInfoVec[i][0], segId);
        uint32_t docCount = 0;
        StringUtil::fromString(segmentInfoVec[i][1], docCount);
        SegmentIteratorType type = (segmentInfoVec[i][2] == "building") ? SIT_BUILDING : SIT_BUILT;

        ASSERT_EQ(segId, iter.GetSegmentId());

        SegmentData segData = iter.GetSegmentData();
        ASSERT_EQ(segId, segData.GetSegmentId());
        ASSERT_EQ(baseDocId, segData.GetBaseDocId());
        ASSERT_EQ(docCount, segData.GetSegmentInfo()->docCount);

        ASSERT_EQ(type, iter.GetType());
        if (type == SIT_BUILDING) {
            ASSERT_TRUE(iter.GetInMemSegment());
        } else {
            ASSERT_FALSE(iter.GetInMemSegment());
        }

        baseDocId += docCount;
        iter.MoveToNext();
    }
    ASSERT_FALSE(iter.IsValid());
}

InMemorySegmentPtr PartitionSegmentIteratorTest::CreateBuildingSegment(segmentid_t segId, int32_t docCount)
{
    BuildingSegmentData segData;
    segData.SetSegmentId(segId);

    index_base::SegmentInfo segInfo;
    segInfo.docCount = docCount;
    segData.SetSegmentInfo(segInfo);

    util::BlockMemoryQuotaControllerPtr memController =
        util::MemoryQuotaControllerCreator::CreateBlockMemoryController();
    config::BuildConfig buildConfig;
    InMemorySegmentPtr inMemSegment(new InMemorySegment(buildConfig, memController, util::CounterMapPtr()));
    inMemSegment->Init(segData, false);
    return inMemSegment;
}
}} // namespace indexlib::index_base
