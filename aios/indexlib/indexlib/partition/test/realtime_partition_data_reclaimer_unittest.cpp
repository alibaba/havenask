#include "indexlib/partition/test/realtime_partition_data_reclaimer_unittest.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);

namespace 
{
class MockDeletionMapReader : public DeletionMapReader
{
public:
    MOCK_CONST_METHOD1(GetDeletedDocCount, uint32_t(segmentid_t));
};
}

IE_LOG_SETUP(partition, RealtimePartitionDataReclaimerTest);

RealtimePartitionDataReclaimerTest::RealtimePartitionDataReclaimerTest()
{
}

RealtimePartitionDataReclaimerTest::~RealtimePartitionDataReclaimerTest()
{
}

void RealtimePartitionDataReclaimerTest::CaseSetUp()
{
}

void RealtimePartitionDataReclaimerTest::CaseTearDown()
{
}

void RealtimePartitionDataReclaimerTest::TestSimpleProcess()
{
}

void RealtimePartitionDataReclaimerTest::TestExtractSegmentsToReclaim()
{
    SimpleSegmentMetas segmentMetas;
    MakeSegmentMetas("0:2:4,1:2:6,2:2:5", segmentMetas);

    MockDeletionMapReader* mockReader(new MockDeletionMapReader);
    DeletionMapReaderPtr deletionMap(mockReader);

    EXPECT_CALL(*mockReader, GetDeletedDocCount(0))
        .WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(1))
        .WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(2))
        .WillRepeatedly(Return(1));

    vector<segmentid_t> allDeletedRtSegs;
    vector<segmentid_t> expectSegs;
    expectSegs.push_back(0);
    expectSegs.push_back(1);

    RealtimePartitionDataReclaimer::ExtractSegmentsToReclaim(
            segmentMetas, 1, deletionMap, allDeletedRtSegs);
    ASSERT_EQ(expectSegs, allDeletedRtSegs);
}

void RealtimePartitionDataReclaimerTest::TestExtractSegmentsToReclaimForSegmentWithOperationQueue()
{
    SimpleSegmentMetas segmentMetas;
    MakeSegmentMetas("0:2:4,1:2:6:true,2:2:5", segmentMetas);

    MockDeletionMapReader* mockReader(new MockDeletionMapReader);
    DeletionMapReaderPtr deletionMap(mockReader);

    EXPECT_CALL(*mockReader, GetDeletedDocCount(0))
        .WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(1))
        .WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(2))
        .WillRepeatedly(Return(1));

    vector<segmentid_t> allDeletedRtSegs;
    vector<segmentid_t> expectSegs;
    expectSegs.push_back(0);

    // segment_1 will not be reclaim because it has Operation directory

    RealtimePartitionDataReclaimer::ExtractSegmentsToReclaim(
            segmentMetas, 1, deletionMap, allDeletedRtSegs);
    ASSERT_EQ(expectSegs, allDeletedRtSegs);
}

void RealtimePartitionDataReclaimerTest::TestExtractSegmentsToReclaimForBreakInTime()
{
    SimpleSegmentMetas segmentMetas;
    MakeSegmentMetas("0:2:4,1:2:6,2:2:5", segmentMetas);

    MockDeletionMapReader* mockReader(new MockDeletionMapReader);
    DeletionMapReaderPtr deletionMap(mockReader);

    EXPECT_CALL(*mockReader, GetDeletedDocCount(0))
        .WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(1))
        .WillRepeatedly(Return(1));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(2))
        .WillRepeatedly(Return(2));

    vector<segmentid_t> allDeletedRtSegs;
    vector<segmentid_t> expectSegs;
    expectSegs.push_back(0);

    RealtimePartitionDataReclaimer::ExtractSegmentsToReclaim(
            segmentMetas, 1, deletionMap, allDeletedRtSegs);
    ASSERT_EQ(expectSegs, allDeletedRtSegs);
}

void RealtimePartitionDataReclaimerTest::TestExtractSegmentsToReclaimForAllDelSegs()
{
    SimpleSegmentMetas segmentMetas;
    MakeSegmentMetas("0:2:4,1:2:5,2:2:6", segmentMetas);

    MockDeletionMapReader* mockReader(new MockDeletionMapReader);
    DeletionMapReaderPtr deletionMap(mockReader);

    EXPECT_CALL(*mockReader, GetDeletedDocCount(0))
        .WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(1))
        .WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(2))
        .WillRepeatedly(Return(2));

    vector<segmentid_t> allDeletedRtSegs;
    vector<segmentid_t> expectSegs = {0, 1, 2};

    RealtimePartitionDataReclaimer::ExtractSegmentsToReclaim(
            segmentMetas, 1, deletionMap, allDeletedRtSegs);
    ASSERT_EQ(expectSegs, allDeletedRtSegs);
}

void RealtimePartitionDataReclaimerTest::TestExtractSegmentsToReclaimForObsoleteTimestamp()
{
    SimpleSegmentMetas segmentMetas;
    MakeSegmentMetas("0:2:4,1:2:6,2:2:5,3:2:7", segmentMetas);

    MockDeletionMapReader* mockReader(new MockDeletionMapReader);
    DeletionMapReaderPtr deletionMap(mockReader);

    EXPECT_CALL(*mockReader, GetDeletedDocCount(0))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(1))
        .WillRepeatedly(Return(1));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(2))
        .WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(3))
        .WillRepeatedly(Return(1));

    vector<segmentid_t> allDeletedRtSegs;
    vector<segmentid_t> expectSegs;

    RealtimePartitionDataReclaimer::ExtractSegmentsToReclaim(
            segmentMetas, 6, deletionMap, allDeletedRtSegs);
    expectSegs.push_back(0);
    ASSERT_TRUE(expectSegs == allDeletedRtSegs);

    RealtimePartitionDataReclaimer::ExtractSegmentsToReclaim(
            segmentMetas, 7, deletionMap, allDeletedRtSegs);
    expectSegs.push_back(1);
    expectSegs.push_back(2);
    ASSERT_TRUE(expectSegs == allDeletedRtSegs);

    expectSegs.push_back(3);
    RealtimePartitionDataReclaimer::ExtractSegmentsToReclaim(
            segmentMetas, 8, deletionMap, allDeletedRtSegs);
    ASSERT_EQ(expectSegs, allDeletedRtSegs);
}

void RealtimePartitionDataReclaimerTest::MakeSegmentMetas(
        const string& segments, SimpleSegmentMetas& segmentMetas)
{
    vector<vector<string> > segMetaVec;

    StringUtil::fromString(segments, segMetaVec, ":", ",");
    for (size_t i = 0; i < segMetaVec.size(); ++i)
    {
        assert(segMetaVec[i].size() >= 3);
        SimpleSegmentMeta segMeta;
        segMeta.segmentId = StringUtil::fromString<segmentid_t>(segMetaVec[i][0]);
        segMeta.docCount = StringUtil::fromString<uint32_t>(segMetaVec[i][1]);
        segMeta.timestamp = StringUtil::fromString<int64_t>(segMetaVec[i][2]);
        segMeta.hasOperation = false;
        if (segMetaVec[i].size() == 4)
        {
            segMeta.hasOperation = (segMetaVec[i][3] == "true");
        }
        segmentMetas.push_back(segMeta);
    }
}

IE_NAMESPACE_END(partition);
