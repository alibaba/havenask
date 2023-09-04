#include "indexlib/partition/test/realtime_partition_data_reclaimer_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/partition/building_partition_data.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::config;

namespace indexlib { namespace partition {

namespace {
class MockDeletionMapReader : public DeletionMapReader
{
public:
    MOCK_METHOD(uint32_t, GetDeletedDocCount, (segmentid_t), (const, override));
};
} // namespace

IE_LOG_SETUP(partition, RealtimePartitionDataReclaimerTest);

RealtimePartitionDataReclaimerTest::RealtimePartitionDataReclaimerTest() {}

RealtimePartitionDataReclaimerTest::~RealtimePartitionDataReclaimerTest() {}

void RealtimePartitionDataReclaimerTest::CaseSetUp() {}

void RealtimePartitionDataReclaimerTest::CaseTearDown() {}

void RealtimePartitionDataReclaimerTest::TestSimpleProcess() {}

void RealtimePartitionDataReclaimerTest::TestExtractSegmentsToReclaim()
{
    SimpleSegmentMetas segmentMetas;
    MakeSegmentMetas("0:2:4,1:2:6,2:2:5", segmentMetas);

    MockDeletionMapReader* mockReader(new MockDeletionMapReader);
    DeletionMapReaderPtr deletionMap(mockReader);

    EXPECT_CALL(*mockReader, GetDeletedDocCount(0)).WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(1)).WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(2)).WillRepeatedly(Return(1));

    vector<segmentid_t> allDeletedRtSegs;
    vector<segmentid_t> expectSegs;
    expectSegs.push_back(0);
    expectSegs.push_back(1);
    config::IndexPartitionOptions options;
    RealtimePartitionDataReclaimer reclaimer(nullptr, options);
    reclaimer.ExtractSegmentsToReclaim(segmentMetas, 1, deletionMap, allDeletedRtSegs);
    ASSERT_EQ(expectSegs, allDeletedRtSegs);
}

void RealtimePartitionDataReclaimerTest::TestExtractSegmentsToReclaimForSegmentWithOperationQueue()
{
    SimpleSegmentMetas segmentMetas;
    MakeSegmentMetas("0:2:4,1:2:6:true,2:2:5", segmentMetas);

    MockDeletionMapReader* mockReader(new MockDeletionMapReader);
    DeletionMapReaderPtr deletionMap(mockReader);

    EXPECT_CALL(*mockReader, GetDeletedDocCount(0)).WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(1)).WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(2)).WillRepeatedly(Return(1));

    vector<segmentid_t> allDeletedRtSegs;
    vector<segmentid_t> expectSegs;
    expectSegs.push_back(0);

    // segment_1 will not be reclaim because it has Operation directory
    config::IndexPartitionOptions options;
    RealtimePartitionDataReclaimer reclaimer(nullptr, options);
    reclaimer.ExtractSegmentsToReclaim(segmentMetas, 1, deletionMap, allDeletedRtSegs);
    ASSERT_EQ(expectSegs, allDeletedRtSegs);
}

void RealtimePartitionDataReclaimerTest::TestExtractSegmentsToReclaimForBreakInTime()
{
    SimpleSegmentMetas segmentMetas;
    MakeSegmentMetas("0:2:4,1:2:6,2:2:5", segmentMetas);

    MockDeletionMapReader* mockReader(new MockDeletionMapReader);
    DeletionMapReaderPtr deletionMap(mockReader);

    EXPECT_CALL(*mockReader, GetDeletedDocCount(0)).WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(1)).WillRepeatedly(Return(1));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(2)).WillRepeatedly(Return(2));

    vector<segmentid_t> allDeletedRtSegs;
    vector<segmentid_t> expectSegs;
    expectSegs.push_back(0);
    config::IndexPartitionOptions options;
    RealtimePartitionDataReclaimer reclaimer(nullptr, options);
    reclaimer.ExtractSegmentsToReclaim(segmentMetas, 1, deletionMap, allDeletedRtSegs);
    ASSERT_EQ(expectSegs, allDeletedRtSegs);
}

void RealtimePartitionDataReclaimerTest::TestExtractSegmentsToReclaimForAllDelSegs()
{
    SimpleSegmentMetas segmentMetas;
    MakeSegmentMetas("0:2:4,1:2:5,2:2:6", segmentMetas);

    MockDeletionMapReader* mockReader(new MockDeletionMapReader);
    DeletionMapReaderPtr deletionMap(mockReader);

    EXPECT_CALL(*mockReader, GetDeletedDocCount(0)).WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(1)).WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(2)).WillRepeatedly(Return(2));

    vector<segmentid_t> allDeletedRtSegs;
    vector<segmentid_t> expectSegs = {0, 1, 2};
    config::IndexPartitionOptions options;
    RealtimePartitionDataReclaimer reclaimer(nullptr, options);
    reclaimer.ExtractSegmentsToReclaim(segmentMetas, 1, deletionMap, allDeletedRtSegs);
    ASSERT_EQ(expectSegs, allDeletedRtSegs);
}

void RealtimePartitionDataReclaimerTest::TestExtractSegmentsToReclaimForObsoleteTimestamp()
{
    SimpleSegmentMetas segmentMetas;
    MakeSegmentMetas("0:2:4,1:2:6,2:2:5,3:2:7", segmentMetas);

    MockDeletionMapReader* mockReader(new MockDeletionMapReader);
    DeletionMapReaderPtr deletionMap(mockReader);

    EXPECT_CALL(*mockReader, GetDeletedDocCount(0)).WillRepeatedly(Return(0));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(1)).WillRepeatedly(Return(1));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(2)).WillRepeatedly(Return(2));
    EXPECT_CALL(*mockReader, GetDeletedDocCount(3)).WillRepeatedly(Return(1));

    vector<segmentid_t> allDeletedRtSegs;
    vector<segmentid_t> expectSegs;
    config::IndexPartitionOptions option;
    RealtimePartitionDataReclaimer reclaimer(nullptr, option);
    reclaimer.ExtractSegmentsToReclaim(segmentMetas, 6, deletionMap, allDeletedRtSegs);
    expectSegs.push_back(0);
    ASSERT_TRUE(expectSegs == allDeletedRtSegs);
    reclaimer.ExtractSegmentsToReclaim(segmentMetas, 7, deletionMap, allDeletedRtSegs);
    expectSegs.push_back(1);
    expectSegs.push_back(2);
    ASSERT_TRUE(expectSegs == allDeletedRtSegs);

    expectSegs.push_back(3);
    reclaimer.ExtractSegmentsToReclaim(segmentMetas, 8, deletionMap, allDeletedRtSegs);
    ASSERT_EQ(expectSegs, allDeletedRtSegs);
}

void RealtimePartitionDataReclaimerTest::MakeSegmentMetas(const string& segments, SimpleSegmentMetas& segmentMetas)
{
    vector<vector<string>> segMetaVec;

    StringUtil::fromString(segments, segMetaVec, ":", ",");
    for (size_t i = 0; i < segMetaVec.size(); ++i) {
        assert(segMetaVec[i].size() >= 3);
        SimpleSegmentMeta segMeta;
        segMeta.segmentId = StringUtil::fromString<segmentid_t>(segMetaVec[i][0]);
        segMeta.docCount = StringUtil::fromString<uint32_t>(segMetaVec[i][1]);
        segMeta.timestamp = StringUtil::fromString<int64_t>(segMetaVec[i][2]);
        segMeta.hasOperation = false;
        if (segMetaVec[i].size() == 4) {
            segMeta.hasOperation = (segMetaVec[i][3] == "true");
        }
        segmentMetas.push_back(segMeta);
    }
}
}} // namespace indexlib::partition
