#include "indexlib/index/test/partition_info_unittest.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/util/counter/counter_map.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);

void PartitionInfoTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mCreator.reset(new PartitionInfoCreator(mRootDir));
}

void PartitionInfoTest::CaseTearDown()
{
    mCreator.reset();
}

void PartitionInfoTest::TestGetGlobalId()
{
    // segment_0 has 2 docs, segment_1 has 3 docs
    // rt segment_0 has 3 docs, rt segment_2 has 2 docs,
    PartitionInfoPtr partInfo = mCreator->CreatePartitionInfo("2,3", "3,2");

    ASSERT_EQ((globalid_t)0, partInfo->GetGlobalId(0));
    ASSERT_EQ((globalid_t)1, partInfo->GetGlobalId(1));
    ASSERT_EQ((globalid_t)(2|((globalid_t)1<<32)), partInfo->GetGlobalId(4));

    segmentid_t rtSegId = RealtimeSegmentDirectory::ConvertToRtSegmentId(0);
    ASSERT_EQ((globalid_t)(1|((globalid_t)rtSegId<<32)), partInfo->GetGlobalId(6));

    // not exist docid
    ASSERT_EQ(INVALID_GLOBALID, partInfo->GetGlobalId(20));
    ASSERT_EQ(INVALID_GLOBALID, partInfo->GetGlobalId(INVALID_DOCID));

    // deleted doc
    partInfo->mDeletionMapReader->Delete(4);
    ASSERT_EQ(INVALID_GLOBALID, partInfo->GetGlobalId(4));
}

void PartitionInfoTest::TestGetDocId()
{
    // segment_0 has 2 docs, segment_1 has 3 docs
    PartitionInfoPtr partInfo = mCreator->CreatePartitionInfo("2,3", "3,2");

    ASSERT_EQ((docid_t)0, partInfo->GetDocId(0));
    ASSERT_EQ((docid_t)1, partInfo->GetDocId(1));
    ASSERT_EQ((docid_t)4, partInfo->GetDocId(2|((globalid_t)1<<32)));

    segmentid_t rtSegId = RealtimeSegmentDirectory::ConvertToRtSegmentId(0);
    ASSERT_EQ((docid_t)6, partInfo->GetDocId(1|((globalid_t)rtSegId<<32)));

    // not exist segment id
    ASSERT_EQ(INVALID_GLOBALID, partInfo->GetDocId(1|((globalid_t)3<<32)));
    // not exist docid
    ASSERT_EQ(INVALID_GLOBALID, partInfo->GetDocId(5|((globalid_t)1<<32)));
    ASSERT_EQ(INVALID_DOCID, partInfo->GetDocId(INVALID_GLOBALID));

    // deleted doc
    partInfo->mDeletionMapReader->Delete(4);
    ASSERT_EQ(INVALID_GLOBALID, partInfo->GetDocId(2|((globalid_t)1<<32)));
}

void PartitionInfoTest::TestGetSegmentId()
{
    PartitionInfo partInfo = mCreator->CreatePartitionInfo(1, "");
    ASSERT_EQ(INVALID_SEGMENTID, partInfo.GetSegmentId(0));
    ASSERT_EQ(INVALID_SEGMENTID, partInfo.GetSegmentId(INVALID_DOCID));

    partInfo = mCreator->CreatePartitionInfo(1, "1:10,3:10");
    ASSERT_EQ(INVALID_SEGMENTID, partInfo.GetSegmentId(INVALID_DOCID));
    ASSERT_EQ((segmentid_t)1, partInfo.GetSegmentId(0));
    ASSERT_EQ((segmentid_t)3, partInfo.GetSegmentId(10));
    ASSERT_EQ(INVALID_SEGMENTID, partInfo.GetSegmentId(20));
}

void PartitionInfoTest::TestGetLocalDocId()
{
    PartitionInfo partInfo = mCreator->CreatePartitionInfo(1, "1:10,3:10");
    ASSERT_EQ(make_pair((segmentid_t)INVALID_SEGMENTID, (docid_t)INVALID_DOCID), partInfo.GetLocalDocInfo(-2));
    ASSERT_EQ(make_pair((segmentid_t)INVALID_SEGMENTID, (docid_t)INVALID_DOCID), partInfo.GetLocalDocInfo(INVALID_DOCID));
    ASSERT_EQ(make_pair((segmentid_t)1, (docid_t)0), partInfo.GetLocalDocInfo(0));
    ASSERT_EQ(make_pair((segmentid_t)1, (docid_t)9), partInfo.GetLocalDocInfo(9));
    ASSERT_EQ(make_pair((segmentid_t)3, (docid_t)0), partInfo.GetLocalDocInfo(10));
    ASSERT_EQ(make_pair((segmentid_t)3, (docid_t)5), partInfo.GetLocalDocInfo(15));
    ASSERT_EQ(make_pair((segmentid_t)3, (docid_t)9), partInfo.GetLocalDocInfo(19));
    ASSERT_EQ(make_pair((segmentid_t)INVALID_SEGMENTID, (docid_t)INVALID_DOCID), partInfo.GetLocalDocInfo(20));
    ASSERT_EQ(make_pair((segmentid_t)INVALID_SEGMENTID, (docid_t)INVALID_DOCID), partInfo.GetLocalDocInfo(25));
}

void PartitionInfoTest::TestGetPartitionInfoHint()
{
    // has both inc and rt segments
    PartitionInfo partInfo = mCreator->CreatePartitionInfo(0, "1:5,2:10", "2:5,3:10");
    ASSERT_EQ((segmentid_t)2, partInfo.GetPartitionInfoHint().lastIncSegmentId);
    segmentid_t rtSegId = RealtimeSegmentDirectory::ConvertToRtSegmentId(3);
    ASSERT_EQ(rtSegId, partInfo.GetPartitionInfoHint().lastRtSegmentId);
    ASSERT_EQ((uint32_t)10, partInfo.GetPartitionInfoHint().lastRtSegmentDocCount);

    // only has inc segments
    partInfo = mCreator->CreatePartitionInfo(0, "1,3");
    ASSERT_EQ((segmentid_t)3, partInfo.GetPartitionInfoHint().lastIncSegmentId);
    ASSERT_EQ(INVALID_SEGMENTID, partInfo.GetPartitionInfoHint().lastRtSegmentId);
    ASSERT_EQ((uint32_t)0, partInfo.GetPartitionInfoHint().lastRtSegmentDocCount);
}

void PartitionInfoTest::TestGetDiffDocIdRangesForIncChanged()
{
    PartitionInfo partInfo = mCreator->CreatePartitionInfo(0, "1,2", "2,3");
    const PartitionInfoHint& partInfoHint = partInfo.GetPartitionInfoHint();

    PartitionInfo newPartInfo = mCreator->CreatePartitionInfo(1, "1,4", "2,3");
    DocIdRangeVector docIdRanges;
    ASSERT_TRUE(newPartInfo.GetDiffDocIdRanges(partInfoHint, docIdRanges));
    ASSERT_EQ((size_t)1, docIdRanges.size());
    ASSERT_EQ((uint32_t)1, (uint32_t)docIdRanges[0].first);
    ASSERT_EQ((uint32_t)2, (uint32_t)docIdRanges[0].second);
}

void PartitionInfoTest::TestGetDiffDocIdRangesForRtBuiltChanged()
{
    PartitionInfo partInfo = mCreator->CreatePartitionInfo(0, "1,2", "2,3");
    const PartitionInfoHint& partInfoHint = partInfo.GetPartitionInfoHint();

    PartitionInfo newPartInfo = mCreator->CreatePartitionInfo(1, "1,2", "3,4");
    DocIdRangeVector docIdRanges;
    ASSERT_TRUE(newPartInfo.GetDiffDocIdRanges(partInfoHint, docIdRanges));
    ASSERT_EQ((size_t)1, docIdRanges.size());
    ASSERT_EQ((uint32_t)3, (uint32_t)docIdRanges[0].first);
    ASSERT_EQ((uint32_t)4, (uint32_t)docIdRanges[0].second);
}
namespace {
class MockInMemorySegment : public InMemorySegment
{
public:
    MockInMemorySegment()
        : InMemorySegment(BuildConfig(),
                          util::MemoryQuotaControllerCreator::CreateBlockMemoryController(),
                          util::CounterMapPtr())
    {}
public:
    MOCK_CONST_METHOD0(GetSegmentInfo, const SegmentInfoPtr&());
    MOCK_CONST_METHOD0(GetSegmentId, segmentid_t());
};
DEFINE_SHARED_PTR(MockInMemorySegment);
}

void PartitionInfoTest::TestGetDiffDocIdRangesForRtBuildingChanged()
{
    PartitionInfo partInfo = mCreator->CreatePartitionInfo(0, "1,2", "2,3");

    MockInMemorySegmentPtr inMemSeg(new MockInMemorySegment);
    SegmentInfoPtr segInfo(new SegmentInfo);
    segInfo->docCount = 0;
    EXPECT_CALL(*inMemSeg, GetSegmentInfo())
        .WillRepeatedly(ReturnRef(segInfo));
    EXPECT_CALL(*inMemSeg, GetSegmentId())
        .WillRepeatedly(Return(RealtimeSegmentDirectory::ConvertToRtSegmentId(4)));
    partInfo.AddInMemorySegment(inMemSeg);
    partInfo.UpdateInMemorySegment(inMemSeg);

    segInfo->docCount = 1;
    PartitionInfoHint partInfoHint = partInfo.GetPartitionInfoHint();

    PartitionInfo partInfo2 = mCreator->CreatePartitionInfo(0, "2,3", "4,5");

    MockInMemorySegmentPtr inMemSeg2(new MockInMemorySegment);
    SegmentInfoPtr segInfo2(new SegmentInfo);
    segInfo2->docCount = 0;
    EXPECT_CALL(*inMemSeg2, GetSegmentInfo())
        .WillRepeatedly(ReturnRef(segInfo2));
    EXPECT_CALL(*inMemSeg2, GetSegmentId())
        .WillRepeatedly(Return(RealtimeSegmentDirectory::ConvertToRtSegmentId(6)));
    partInfo2.AddInMemorySegment(inMemSeg2);
    segInfo2->docCount = 2;
    partInfo2.UpdateInMemorySegment(inMemSeg2);


    DocIdRangeVector docIdRanges;
    ASSERT_TRUE(partInfo2.GetDiffDocIdRanges(partInfoHint, docIdRanges));
    ASSERT_EQ((size_t)2, docIdRanges.size());
    ASSERT_EQ((uint32_t)1, (uint32_t)docIdRanges[0].first);
    ASSERT_EQ((uint32_t)2, (uint32_t)docIdRanges[0].second);
    ASSERT_EQ((uint32_t)2, (uint32_t)docIdRanges[1].first);
    ASSERT_EQ((uint32_t)6, (uint32_t)docIdRanges[1].second);
}

void PartitionInfoTest::TestGetDiffDocIdRangesForAllChanged()
{
    PartitionInfo partInfo = mCreator->CreatePartitionInfo(0, "1,2", "2,3");

    MockInMemorySegmentPtr inMemSeg(new MockInMemorySegment);
    SegmentInfoPtr segInfo(new SegmentInfo);
    segInfo->docCount = 0;
    EXPECT_CALL(*inMemSeg, GetSegmentInfo())
        .WillRepeatedly(ReturnRef(segInfo));
    EXPECT_CALL(*inMemSeg, GetSegmentId())
        .WillRepeatedly(Return(RealtimeSegmentDirectory::ConvertToRtSegmentId(4)));
    partInfo.AddInMemorySegment(inMemSeg);

    PartitionInfoHint partInfoHint = partInfo.GetPartitionInfoHint();

    // building segment changed
    segInfo->docCount = 1;
    partInfo.UpdateInMemorySegment(inMemSeg);

    DocIdRangeVector docIdRanges;
    ASSERT_TRUE(partInfo.GetDiffDocIdRanges(partInfoHint, docIdRanges));
    ASSERT_EQ((size_t)1, docIdRanges.size());
    ASSERT_EQ((uint32_t)4, (uint32_t)docIdRanges[0].first);
    ASSERT_EQ((uint32_t)5, (uint32_t)docIdRanges[0].second);
}

void PartitionInfoTest::TestGetOrderedAndUnorderDocIdRanges()
{
    DocIdRangeVector ranges;
    DocIdRange unorderRange;

    // empty PartitionMeta
    PartitionInfo partInfo = mCreator->CreatePartitionInfo(0, "1,2", "2,3");
    ASSERT_FALSE(partInfo.GetOrderedDocIdRanges(ranges));
    ASSERT_TRUE(partInfo.GetUnorderedDocIdRange(unorderRange));
    ASSERT_EQ(DocIdRange(0, 4), unorderRange);

    // has sort
    SortDescriptions sortDescriptions;
    sortDescriptions.push_back(SortDescription("abc", SortPattern()));
    PartitionInfo partInfo2 = mCreator->CreatePartitionInfo(0, "1,2", "2,3", 
            PartitionMetaPtr(new PartitionMeta(sortDescriptions)));

    ASSERT_TRUE(partInfo2.GetOrderedDocIdRanges(ranges));
    ASSERT_EQ((size_t)2, ranges.size());
    ASSERT_EQ(DocIdRange(0, 1), ranges[0]);
    ASSERT_EQ(DocIdRange(1, 2), ranges[1]);
    ASSERT_TRUE(partInfo2.GetUnorderedDocIdRange(unorderRange));
    ASSERT_EQ(DocIdRange(2, 4), unorderRange);
}

void PartitionInfoTest::TestInitWithDumpingSegments()
{
    std::vector<InMemorySegmentPtr> dumpingSegments;
    std::vector<MockInMemorySegmentPtr> mockDumpingSegments;
    std::vector<SegmentInfoPtr> segmentInfos;
    for (size_t i = 0; i < 2; i ++)
    {
        MockInMemorySegmentPtr inMemSeg(new MockInMemorySegment);
        SegmentInfoPtr segInfo(new SegmentInfo);
        segInfo->docCount = 3;
        segmentInfos.push_back(segInfo);
        dumpingSegments.push_back(inMemSeg);
        mockDumpingSegments.push_back(inMemSeg);
    }

    EXPECT_CALL(*mockDumpingSegments[0], GetSegmentInfo())
        .WillRepeatedly(ReturnRef(segmentInfos[0]));
    EXPECT_CALL(*mockDumpingSegments[0], GetSegmentId())
        .WillRepeatedly(Return(RealtimeSegmentDirectory::ConvertToRtSegmentId(4)));

    EXPECT_CALL(*mockDumpingSegments[1], GetSegmentInfo())
        .WillRepeatedly(ReturnRef(segmentInfos[1]));
    EXPECT_CALL(*mockDumpingSegments[1], GetSegmentId())
        .WillRepeatedly(Return(RealtimeSegmentDirectory::ConvertToRtSegmentId(5)));


    PartitionInfo partInfo = mCreator->CreatePartitionInfo(0, "1:5,2:10", "2:5,3:10", PartitionMetaPtr(), dumpingSegments);
    ASSERT_EQ((segmentid_t)2, partInfo.GetPartitionInfoHint().lastIncSegmentId);
    segmentid_t rtSegId = RealtimeSegmentDirectory::ConvertToRtSegmentId(5);
    ASSERT_EQ(rtSegId, partInfo.GetPartitionInfoHint().lastRtSegmentId);
    ASSERT_EQ((uint32_t)3, partInfo.GetPartitionInfoHint().lastRtSegmentDocCount);
    // test get docid


    ASSERT_EQ((docid_t)7, partInfo.GetDocId(2|((globalid_t)2<<32)));

    segmentid_t rtSegId2 = RealtimeSegmentDirectory::ConvertToRtSegmentId(2);
    ASSERT_EQ((docid_t)16, partInfo.GetDocId(1|((globalid_t)rtSegId2<<32)));
    segmentid_t rtSegId4 = RealtimeSegmentDirectory::ConvertToRtSegmentId(4);
    ASSERT_EQ((docid_t)32, partInfo.GetDocId(2|((globalid_t)rtSegId4<<32)));
    segmentid_t rtSegId5 = RealtimeSegmentDirectory::ConvertToRtSegmentId(5);
    ASSERT_EQ((docid_t)34, partInfo.GetDocId(1|((globalid_t)rtSegId5<<32)));

    ASSERT_EQ(1|((globalid_t)rtSegId2<<32), partInfo.GetGlobalId((docid_t)16));
    ASSERT_EQ(2|((globalid_t)rtSegId4<<32), partInfo.GetGlobalId((docid_t)32));
    ASSERT_EQ(1|((globalid_t)rtSegId5<<32), partInfo.GetGlobalId((docid_t)34));

    ASSERT_EQ(rtSegId2, partInfo.GetSegmentId((docid_t)16));
    ASSERT_EQ(rtSegId4, partInfo.GetSegmentId((docid_t)32));
    ASSERT_EQ(rtSegId5, partInfo.GetSegmentId((docid_t)34));  
}

void PartitionInfoTest::TestKVKKVTable() {
    PartitionInfo partInfo = CreatePartitionInfo(1, "", "", index_base::PartitionMetaPtr(), {}, false);
    ASSERT_TRUE(partInfo.IsKeyValueTable());
    const auto& infoHint = partInfo.GetPartitionInfoHint();

    (void)infoHint;

    DocIdRangeVector docIdRanges;
    EXPECT_FALSE(partInfo.GetDiffDocIdRanges(infoHint, docIdRanges));
    EXPECT_FALSE(partInfo.GetOrderedDocIdRanges(docIdRanges));
    DocIdRange range;
    EXPECT_FALSE(partInfo.GetUnorderedDocIdRange(range));
    EXPECT_EQ(INVALID_GLOBALID, partInfo.GetGlobalId(0));
    EXPECT_EQ(INVALID_DOCID, partInfo.GetDocId(0));
    EXPECT_EQ(INVALID_SEGMENTID, partInfo.GetSegmentId(0));
    auto baseDocIds = partInfo.GetBaseDocIds();
    EXPECT_TRUE(baseDocIds.empty());

    size_t totalDocCount = 0;
    EXPECT_EQ(totalDocCount, partInfo.GetTotalDocCount());

}

// PartitionInfo PartitionInfoTest::CreateKVPartitionInfo(versionid_t versionId, size_t columnCount, size_t 

PartitionInfo PartitionInfoTest::CreatePartitionInfo(versionid_t versionId, 
                                                     const string& incSegmentIds, const string& rtSegmentIds,
                                                     PartitionMetaPtr partMeta, 
                                                     std::vector<InMemorySegmentPtr> dumpingSegments,
                                                     bool needDelMapReader)
{
    Version version = VersionMaker::Make(versionId, incSegmentIds, rtSegmentIds);

    vector<docid_t> docCountVec;
    ExtractDocCount(incSegmentIds, docCountVec);
    ExtractDocCount(rtSegmentIds, docCountVec);
    assert(docCountVec.size() == version.GetSegmentCount());
    int64_t totalDocCount = 0;
    SegmentDataVector segmentDatas;
    for (size_t i = 0; i < version.GetSegmentCount(); ++i)
    {
        SegmentData segData;
        segData.SetSegmentId(version[i]);
        segData.SetBaseDocId(totalDocCount);
        SegmentInfo segInfo;
        segInfo.docCount = docCountVec[i];
        segData.SetSegmentInfo(segInfo);
        segmentDatas.push_back(segData);
        totalDocCount += segInfo.docCount;
    }

    for (size_t i = 0; i < dumpingSegments.size(); i++)
    {
        totalDocCount += dumpingSegments[i]->GetSegmentInfo()->docCount;
    }
    PartitionInfo partInfo;
    PartitionMetaPtr partitionMeta = partMeta;
    if (!partitionMeta)
    {
        partitionMeta.reset(new PartitionMeta);
    }

    DeletionMapReaderPtr delReader;
    if (needDelMapReader)
    {
        delReader.reset(new DeletionMapReader(totalDocCount));
    }
    partInfo.Init(version, partitionMeta, segmentDatas, dumpingSegments, delReader);
    return partInfo;
}

PartitionInfoPtr PartitionInfoTest::CreatePartitionInfo(const string& incDocCounts,
        const string& rtDocCounts)
{
    mProvider.reset(new SingleFieldPartitionDataProvider);
    mProvider->Init(mRootDir, "int32", SFP_ATTRIBUTE);
    mProvider->Build(ExtractDocString(incDocCounts), SFP_OFFLINE);
    mProvider->Build(ExtractDocString(rtDocCounts), SFP_REALTIME);
    PartitionDataPtr partData = mProvider->GetPartitionData();
    return partData->GetPartitionInfo();
}

string PartitionInfoTest::ExtractDocString(const string& docCountsStr)
{
    if (docCountsStr.empty())
    {
        return "";
    }
    vector<size_t> docCounts;
    StringUtil::fromString(docCountsStr, docCounts, ",");
    string docString = "";
    for (size_t i = 0; i < docCounts.size(); ++i)
    {
        for (size_t j = 0; j < docCounts[i]; ++j)
        {
            docString += "0,";
        }
        docString[docString.size() - 1] = '#';
    }
    docString.erase(docString.end() - 1);
    return docString;
}

void PartitionInfoTest::ExtractDocCount(const string& segmentInfos, 
                                        vector<docid_t> &docCount)
{
    vector<string> segInfoStrs = StringUtil::split(segmentInfos, ",");
    for (size_t i = 0; i < segInfoStrs.size(); ++i)
    {
        vector<int> segInfo;
        StringUtil::fromString(segInfoStrs[i], segInfo, ":");
        if (segInfo.size() == 1)
        {
            docCount.push_back(1);
        }
        else if (segInfo.size() == 2)
        {
            docCount.push_back(segInfo[1]);
        }
        else
        {
            assert(false);
        }
    }
}

IE_NAMESPACE_END(index);
