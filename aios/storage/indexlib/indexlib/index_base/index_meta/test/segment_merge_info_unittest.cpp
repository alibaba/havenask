#include "indexlib/index_base/index_meta/test/segment_merge_info_unittest.h"

using namespace std;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SegmentMergeInfoTest);

SegmentMergeInfoTest::SegmentMergeInfoTest() {}

SegmentMergeInfoTest::~SegmentMergeInfoTest() {}

void SegmentMergeInfoTest::CaseSetUp() {}

void SegmentMergeInfoTest::CaseTearDown() {}

void SegmentMergeInfoTest::TestJsonize()
{
    string jsonString;
    {
        SegmentMergeInfo info;
        info.segmentId = 1;
        info.deletedDocCount = 2;
        info.baseDocId = 3;
        info.segmentSize = 4;
        info.segmentInfo.docCount = 5;
        info.levelIdx = 1;
        info.inLevelIdx = 1;
        jsonString = ToJsonString(info);
    }
    {
        SegmentMergeInfo info;
        FromJsonString(info, jsonString);
        EXPECT_EQ(segmentid_t(1), info.segmentId);
        EXPECT_EQ(uint32_t(2), info.deletedDocCount);
        EXPECT_EQ(docid_t(3), info.baseDocId);
        EXPECT_EQ(int64_t(4), info.segmentSize);
        EXPECT_EQ(uint32_t(5), info.segmentInfo.docCount);
        EXPECT_EQ(1u, info.levelIdx);
        EXPECT_EQ(1u, info.inLevelIdx);
    }
}

void SegmentMergeInfoTest::TestComparation()
{
    SegmentMergeInfo lhs;
    SegmentMergeInfo rhs;
    lhs.levelIdx = 1;
    rhs.levelIdx = 0;
    ASSERT_LT(lhs, rhs);

    lhs.levelIdx = 0;
    rhs.levelIdx = 0;
    lhs.inLevelIdx = 1;
    rhs.inLevelIdx = 2;
    ASSERT_LT(lhs, rhs);
}
}} // namespace indexlib::index_base
