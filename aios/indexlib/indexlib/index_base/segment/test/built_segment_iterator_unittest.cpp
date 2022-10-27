#include "indexlib/index_base/segment/test/built_segment_iterator_unittest.h"
#include "indexlib/partition/segment/test/segment_iterator_helper.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(segment, BuiltSegmentIteratorTest);

BuiltSegmentIteratorTest::BuiltSegmentIteratorTest()
{
}

BuiltSegmentIteratorTest::~BuiltSegmentIteratorTest()
{
}

void BuiltSegmentIteratorTest::CaseSetUp()
{
}

void BuiltSegmentIteratorTest::CaseTearDown()
{
}

void BuiltSegmentIteratorTest::TestSimpleProcess()
{
    // segmentId:docCount;,...,...
    string inMemInfoStr = "0:10;1:20;2:30";
    
    vector<SegmentData> segmentDatas;
    SegmentIteratorHelper::PrepareBuiltSegmentDatas(inMemInfoStr, segmentDatas);

    BuiltSegmentIterator iter;
    iter.Init(segmentDatas);
    CheckIterator(iter, inMemInfoStr);
}

void BuiltSegmentIteratorTest::CheckIterator(
        BuiltSegmentIterator& iter, const string& inMemInfoStr)
{
    ASSERT_EQ(SIT_BUILT, iter.GetType());
    vector<vector<int32_t> > segmentInfoVec;
    StringUtil::fromString(inMemInfoStr, segmentInfoVec, ":", ";");

    docid_t baseDocId = 0;
    for (size_t i = 0; i < segmentInfoVec.size(); i++)
    {
        assert(segmentInfoVec[i].size() == 2);
        ASSERT_TRUE(iter.IsValid());
        ASSERT_EQ(baseDocId, iter.GetBaseDocId());
        ASSERT_EQ((segmentid_t)segmentInfoVec[i][0], iter.GetSegmentId());

        SegmentData segData = iter.GetSegmentData();
        ASSERT_EQ((segmentid_t)segmentInfoVec[i][0], segData.GetSegmentId());
        ASSERT_EQ(baseDocId, segData.GetBaseDocId());
        ASSERT_EQ(segmentInfoVec[i][1], segData.GetSegmentInfo().docCount);
        
        baseDocId += segmentInfoVec[i][1];
        iter.MoveToNext();
    }
    ASSERT_FALSE(iter.IsValid());
}


IE_NAMESPACE_END(index_base);

