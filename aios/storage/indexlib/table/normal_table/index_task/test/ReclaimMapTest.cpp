#include "indexlib/table/normal_table/index_task/ReclaimMap.h"

#include <string>
#include <vector>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/base/Status.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/table/normal_table/index_task/test/ReclaimMapUtil.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlibv2 { namespace table {

class ReclaimMapTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    ReclaimMapTest();
    ~ReclaimMapTest();

    DECLARE_CLASS_NAME(ReclaimMapTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    void MakeSegmentMergeInfos(const std::vector<index::IIndexMerger::SourceSegment>& sourceSegments,
                               const std::vector<segmentid_t>& seg2Merge, bool multiTargetSegment,
                               index::IIndexMerger::SegmentMergeInfos& segMergeInfos);

    void TestMergePartWithoutDelete(bool needReverseMapping, bool multiTargetSegment);
    void TestMergePartWithDelete(bool needReverseMapping, bool multiTargetSegment);
    void TestMergePartIntenal(index::IndexTestUtil::ToDelete toDel, const std::string& expectedDocIdStr,
                              bool needReverseMapping = false, const std::string& expectedOldDocIdStr = "",
                              const std::string& expectedSegIdStr = "", bool splitSegment = false);

    void TestMergeAllWithoutDelete(bool needReverseMapping, bool multiTargetSegment);
    void TestMergeAllWithDelete(bool needReverseMapping, bool multiTargetSegment);
    void TestMergeAllInternal(index::IndexTestUtil::ToDelete toDel, const std::string& expectedDocIdStr,
                              bool needReverseMapping = false, const std::string& expectedOldDocIdStr = "",
                              const std::string& expectedSegIdStr = "", bool multiTargetSegment = false);

    void TestMergeSegmentApartWithoutDelete(bool multiTargetSegment);
    void TestMergeSegmentApartWithDelete(bool multiTargetSegment);
    void TestMergeSegmentApartInternal(index::IndexTestUtil::ToDelete toDel, const std::string& expectedDocIdStr,
                                       bool needReverseMapping = false, bool multiTargetSegment = false);

    void InnerTestMerge(const std::vector<index::IIndexMerger::SourceSegment>& sourceSegments,
                        const std::vector<segmentid_t>& seg2Merge, bool needReverseMapping,
                        index::IndexTestUtil::ToDelete toDel,
                        const std::string& expectedDocIdStr,        // reclaimed docids
                        const std::string& expectedReverseDocIdStr, // old docids
                        const std::string& expectedSegIdStr,        // old segids
                        bool multiTargetSegment = false);

private:
    std::string _rootPath;
    std::shared_ptr<indexlib::file_system::IFileSystem> _fs;
    std::shared_ptr<indexlib::file_system::Directory> _rootDir;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.table, ReclaimMapTest);

ReclaimMapTest::ReclaimMapTest() {}

ReclaimMapTest::~ReclaimMapTest() {}

void ReclaimMapTest::CaseSetUp()
{
    indexlib::file_system::FileSystemOptions fsOptions;
    _rootPath = GET_TEMP_DATA_PATH();
    _fs = indexlib::file_system::FileSystemCreator::Create("test", _rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(_fs);
}

void ReclaimMapTest::CaseTearDown() {}

void ReclaimMapTest::MakeSegmentMergeInfos(const std::vector<index::IIndexMerger::SourceSegment>& sourceSegments,
                                           const std::vector<segmentid_t>& seg2Merge, bool multiTargetSegment,
                                           index::IIndexMerger::SegmentMergeInfos& segMergeInfos)
{
    segMergeInfos.srcSegments.clear();
    docid_t newBaseDocId = 0;
    for (size_t i = 0; i < sourceSegments.size(); i++) {
        if (std::find(seg2Merge.begin(), seg2Merge.end(), sourceSegments[i].segment->GetSegmentId()) !=
            seg2Merge.end()) {
            auto newSourceSegment = sourceSegments[i];
            newSourceSegment.baseDocid = newBaseDocId;
            segMergeInfos.srcSegments.push_back(newSourceSegment);
            newBaseDocId += newSourceSegment.segment->GetSegmentInfo()->docCount;
        }
    }
    auto maxSegId = sourceSegments.back().segment->GetSegmentId();
    auto lastSegmentInfo = sourceSegments.back().segment->GetSegmentInfo();
    ReclaimMapUtil::CreateTargetSegment(_rootDir, ++maxSegId, lastSegmentInfo, segMergeInfos);
    if (multiTargetSegment) {
        ReclaimMapUtil::CreateTargetSegment(_rootDir, ++maxSegId, lastSegmentInfo, segMergeInfos);
    }
}

void ReclaimMapTest::InnerTestMerge(const std::vector<index::IIndexMerger::SourceSegment>& sourceSegments,
                                    const std::vector<segmentid_t>& seg2Merge, bool needReverseMapping,
                                    index::IndexTestUtil::ToDelete toDel,
                                    const std::string& expectedDocIdStr,        // reclaimed docids
                                    const std::string& expectedReverseDocIdStr, // old docids
                                    const std::string& expectedSegIdStr,        // old segids
                                    bool multiTargetSegment)

{
    std::function<std::pair<Status, segmentindex_t>(segmentid_t, docid_t)> segmentSplitHandler = nullptr;
    if (multiTargetSegment) {
        size_t cnt = 0;
        segmentSplitHandler = [cnt](segmentid_t, docid_t) mutable -> std::pair<Status, segmentindex_t> {
            auto ret = cnt % 2;
            ++cnt;
            return {Status::OK(), ret};
        };
    }

    index::IIndexMerger::SegmentMergeInfos segMergeInfos;
    MakeSegmentMergeInfos(sourceSegments, seg2Merge, multiTargetSegment, segMergeInfos);
    ASSERT_EQ(segMergeInfos.srcSegments.size(), seg2Merge.size());
    ASSERT_EQ(segMergeInfos.targetSegments.size(), multiTargetSegment ? 2 : 1);

    const auto& targetSegment = segMergeInfos.targetSegments[0];
    std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>> srcSegments;
    for (const auto& srcSegment : segMergeInfos.srcSegments) {
        srcSegments.emplace_back(srcSegment.baseDocid, srcSegment.segment);
    }
    ReclaimMap reclaimMap("reclaimMap", index::DocMapper::GetDocMapperType());
    auto status = reclaimMap.Calculate(0, targetSegment->segmentId, srcSegments, segmentSplitHandler);
    assert(status.IsOK());

    ReclaimMapUtil::CheckAnswer(reclaimMap, expectedDocIdStr, needReverseMapping, expectedReverseDocIdStr,
                                expectedSegIdStr);
}

void ReclaimMapTest::TestMergePartWithoutDelete(bool needReverseMapping, bool splitSegment)
{
    std::string expectedDocIdStr = "-2,-2,0,1,2,3,4,5,6,7,-2,-2,-2";
    std::string expectedOldDocIdStr = "";
    std::string expectedSegIdStr = "";
    if (needReverseMapping) {
        expectedOldDocIdStr = "0,1,2,0,1,2,3,4";
        expectedSegIdStr = "1,1,1,2,2,2,2,2";
    }
    if (splitSegment) {
        // 2,3,5,3
        expectedDocIdStr = "-2,-2,0,4,1,5,2,6,3,7,-2,-2,-2";
        if (needReverseMapping) {
            expectedOldDocIdStr = "0,2,1,3,1,0,2,4";
            expectedSegIdStr = "1,1,2,2,1,2,2,2";
        }
    }
    TestMergePartIntenal(index::IndexTestUtil::NoDelete, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                         expectedSegIdStr, splitSegment);
}

void ReclaimMapTest::TestMergePartWithDelete(bool needReverseMapping, bool splitSegment)
{
    std::string expectedDocIdStr = "-2,-2,-1,0,-1,-1,1,-1,2,-1,-2,-2,-2";
    std::string expectedOldDocIdStr = "";
    std::string expectedSegIdStr = "";
    if (needReverseMapping) {
        expectedOldDocIdStr = "1,1,3";
        expectedSegIdStr = "1,2,2";
    }
    if (splitSegment) {
        // 2,3,5,3
        expectedDocIdStr = "-2,-2,-1,0,-1,-1,2,-1,1,-1,-2,-2,-2";
        if (needReverseMapping) {
            expectedOldDocIdStr = "1,3,1";
            expectedSegIdStr = "1,2,2";
        }
    }

    TestMergePartIntenal(index::IndexTestUtil::DeleteEven, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                         expectedSegIdStr, splitSegment);
}

void ReclaimMapTest::TestMergePartIntenal(index::IndexTestUtil::ToDelete toDel, const std::string& expectedDocIdStr,
                                          bool needReverseMapping, const std::string& expectedOldDocIdStr,
                                          const std::string& expectedSegIdStr, bool splitSegment)
{
    std::vector<uint32_t> docCounts {2, 3, 5, 3};
    std::vector<segmentid_t> segIds = {0, 1, 2, 3};
    std::vector<segmentid_t> seg2Merge = {1, 2};
    ASSERT_EQ(docCounts.size(), segIds.size());
    ASSERT_TRUE(seg2Merge.size() <= segIds.size());

    std::vector<index::IIndexMerger::SourceSegment> sourceSegments;
    ReclaimMapUtil::PrepareSegments(_rootDir, docCounts, segIds, toDel, sourceSegments);

    InnerTestMerge(sourceSegments, seg2Merge, needReverseMapping, toDel, expectedDocIdStr, expectedOldDocIdStr,
                   expectedSegIdStr, splitSegment);
}

void ReclaimMapTest::TestMergeAllWithoutDelete(bool needReverseMapping, bool multiTargetSegment)
{
    std::string expectedDocIdStr = "0,1,2,3,4,5,6,7,8,9,10,11,12";
    std::string expectedOldDocIdStr = "";
    std::string expectedSegIdStr = "";
    if (needReverseMapping) {
        expectedOldDocIdStr = "0,1,0,1,2,0,1,2,3,4,0,1,2";
        expectedSegIdStr = "0,0,1,1,1,2,2,2,2,2,3,3,3";
    }

    if (multiTargetSegment) {
        expectedDocIdStr = "0,7,1,8,2,9,3,10,4,11,5,12,6";
        if (needReverseMapping) {
            // 2,3,5,3
            expectedOldDocIdStr = "0,0,2,1,3,0,2,1,1,0,2,4,1";
            expectedSegIdStr = "0,1,1,2,2,3,3,0,1,2,2,2,3";
        }
    }
    TestMergeAllInternal(index::IndexTestUtil::NoDelete, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                         expectedSegIdStr, multiTargetSegment);
}

void ReclaimMapTest::TestMergeAllWithDelete(bool needReverseMapping, bool multiTargetSegment)
{
    std::string expectedDocIdStr = "-1,0,-1,1,-1,-1,2,-1,3,-1,-1,4,-1";
    std::string expectedOldDocIdStr = "";
    std::string expectedSegIdStr = "";
    if (needReverseMapping) {
        expectedOldDocIdStr = "1,1,1,3,1";
        expectedSegIdStr = "0,1,2,2,3";
    }

    if (multiTargetSegment) {
        // 2,3,5,3
        expectedDocIdStr = "-1,0,-1,3,-1,-1,1,-1,4,-1,-1,2,-1";
        if (needReverseMapping) {
            expectedOldDocIdStr = "1,1,1,1,3";
            expectedSegIdStr = "0,2,3,1,2";
        }
    }
    TestMergeAllInternal(index::IndexTestUtil::DeleteEven, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                         expectedSegIdStr, multiTargetSegment);
}

void ReclaimMapTest::TestMergeAllInternal(index::IndexTestUtil::ToDelete toDel, const std::string& expectedDocIdStr,
                                          bool needReverseMapping, const std::string& expectedOldDocIdStr,
                                          const std::string& expectedSegIdStr, bool multiTargetSegment)
{
    std::string docCountsStr = "2,3,5,3";
    std::string segIdStr = "0,1,2,3";
    std::string seg2MergeStr = "0,1,2,3";
    std::vector<uint32_t> docCounts;
    std::vector<segmentid_t> segIds;
    std::vector<segmentid_t> seg2Merge;
    autil::StringUtil::fromString(docCountsStr, docCounts, ",");
    autil::StringUtil::fromString(segIdStr, segIds, ",");
    autil::StringUtil::fromString(seg2MergeStr, seg2Merge, ",");
    ASSERT_EQ(docCounts.size(), segIds.size());
    ASSERT_TRUE(seg2Merge.size() <= segIds.size());

    std::vector<index::IIndexMerger::SourceSegment> sourceSegments;
    ReclaimMapUtil::PrepareSegments(_rootDir, docCounts, segIds, toDel, sourceSegments);

    InnerTestMerge(sourceSegments, seg2Merge, needReverseMapping, toDel, expectedDocIdStr, expectedOldDocIdStr,
                   expectedSegIdStr, multiTargetSegment);
}

void ReclaimMapTest::TestMergeSegmentApartWithoutDelete(bool splitSegment)
{
    std::string expectedDocIdStr = "-2,-2,0,1,2,-2,-2,-2,-2,-2,3,4,5";
    if (splitSegment) {
        expectedDocIdStr = "-2,-2,0,3,1,-2,-2,-2,-2,-2,4,2,5";
    }
    TestMergeSegmentApartInternal(index::IndexTestUtil::NoDelete, expectedDocIdStr, false, splitSegment);
}

void ReclaimMapTest::TestMergeSegmentApartWithDelete(bool splitSegment)
{
    std::string expectedDocIdStr = "-2,-2,-1,0,-1,-2,-2,-2,-2,-2,-1,1,-1";
    if (splitSegment) {
        expectedDocIdStr = "-2,-2,-1,0,-1,-2,-2,-2,-2,-2,-1,1,-1";
    }
    TestMergeSegmentApartInternal(index::IndexTestUtil::DeleteEven, expectedDocIdStr, false, splitSegment);
}

void ReclaimMapTest::TestMergeSegmentApartInternal(index::IndexTestUtil::ToDelete toDel,
                                                   const std::string& expectedDocIdStr, bool needReverseMapping,
                                                   bool splitSegment)
{
    std::string docCountsStr = "2,3,5,3";
    std::string segIdStr = "1,3,4,6";
    std::string seg2MergeStr = "3,6";
    std::vector<uint32_t> docCounts;
    std::vector<segmentid_t> segIds;
    std::vector<segmentid_t> seg2Merge;
    autil::StringUtil::fromString(docCountsStr, docCounts, ",");
    autil::StringUtil::fromString(segIdStr, segIds, ",");
    autil::StringUtil::fromString(seg2MergeStr, seg2Merge, ",");
    ASSERT_EQ(docCounts.size(), segIds.size());
    ASSERT_TRUE(seg2Merge.size() <= segIds.size());

    std::vector<index::IIndexMerger::SourceSegment> sourceSegments;
    ReclaimMapUtil::PrepareSegments(_rootDir, docCounts, segIds, toDel, sourceSegments);

    InnerTestMerge(sourceSegments, seg2Merge, needReverseMapping, toDel, expectedDocIdStr, "", "", splitSegment);
}

TEST_F(ReclaimMapTest, TestStoreAndLoad)
{
    std::string docCountsStr = "2,3,5,3";
    std::string segIdStr = "0,1,2,3";
    std::string seg2MergeStr = "1,2";
    std::vector<uint32_t> docCounts;
    std::vector<segmentid_t> segIds;
    std::vector<segmentid_t> seg2Merge;
    autil::StringUtil::fromString(docCountsStr, docCounts, ",");
    autil::StringUtil::fromString(segIdStr, segIds, ",");
    autil::StringUtil::fromString(seg2MergeStr, seg2Merge, ",");
    ASSERT_EQ(docCounts.size(), segIds.size());
    ASSERT_TRUE(seg2Merge.size() <= segIds.size());

    std::vector<index::IIndexMerger::SourceSegment> sourceSegments;
    ReclaimMapUtil::PrepareSegments(_rootDir, docCounts, segIds, index::IndexTestUtil::DeleteEven, sourceSegments);

    index::IIndexMerger::SegmentMergeInfos segMergeInfos;
    MakeSegmentMergeInfos(sourceSegments, seg2Merge, /*multiTargetSegment*/ false, segMergeInfos);
    ASSERT_EQ(segMergeInfos.srcSegments.size(), seg2Merge.size());
    ASSERT_EQ(segMergeInfos.targetSegments.size(), 1);
    const auto& targetSegmentMeta = segMergeInfos.targetSegments[0];
    ASSERT_EQ(targetSegmentMeta->segmentId, 4);

    const auto& targetSegment = segMergeInfos.targetSegments[0];
    std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>> srcSegments;
    for (const auto& srcSegment : segMergeInfos.srcSegments) {
        srcSegments.emplace_back(srcSegment.baseDocid, srcSegment.segment);
    }
    ReclaimMap reclaimMap("reclaimMap", index::DocMapper::GetDocMapperType());
    auto status = reclaimMap.Calculate(0, targetSegment->segmentId, srcSegments, nullptr);
    assert(status.IsOK());
    auto resourceDirectory = _rootDir->MakeDirectory("work", indexlib::file_system::DirectoryOption::Mem());
    status = reclaimMap.Store(resourceDirectory);
    ASSERT_TRUE(status.IsOK());
    _rootDir->Sync(true);

    ReclaimMap reclaimMap2("reclaimMap", index::DocMapper::GetDocMapperType());
    status = reclaimMap2.Load(resourceDirectory);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(reclaimMap.GetDeletedDocCount(), reclaimMap2.GetDeletedDocCount());
    ASSERT_EQ(reclaimMap.GetTargetSegmentCount(), reclaimMap2.GetTargetSegmentCount());
    ASSERT_EQ(reclaimMap.GetNewDocCount(), reclaimMap2.GetNewDocCount());
    for (uint32_t i = 0; i < reclaimMap2.GetNewDocCount(); i++) {
        docid_t docId = (docid_t)i;
        auto [expectedSegId, expectedDocId] = reclaimMap.ReverseMap(docId);
        auto [actualSegId, actualDocId] = reclaimMap2.ReverseMap(docId);

        ASSERT_EQ(expectedSegId, actualSegId);
        ASSERT_EQ(expectedDocId, actualDocId);
    }
}

TEST_F(ReclaimMapTest, TestMergePart)
{
    TestMergePartWithoutDelete(/*needReverseMapping*/ false, /*multiTargetSegment*/ false);
    TestMergePartWithoutDelete(/*needReverseMapping*/ true, /*multiTargetSegment*/ false);
    TestMergePartWithDelete(/*needReverseMapping*/ false, /*multiTargetSegment*/ false);
    TestMergePartWithDelete(/*needReverseMapping*/ true, /*multiTargetSegment*/ false);

    TestMergePartWithoutDelete(/*needReverseMapping*/ false, /*multiTargetSegment*/ true);
    TestMergePartWithoutDelete(/*needReverseMapping*/ true, /*multiTargetSegment*/ true);
    TestMergePartWithDelete(/*needReverseMapping*/ false, /*multiTargetSegment*/ true);
    TestMergePartWithDelete(/*needReverseMapping*/ true, /*multiTargetSegment*/ true);
}

TEST_F(ReclaimMapTest, TestMergeAll)
{
    TestMergeAllWithoutDelete(/*needReverseMapping*/ false, /*multiTargetSegment*/ false);
    TestMergeAllWithoutDelete(/*needReverseMapping*/ true, /*multiTargetSegment*/ false);
    TestMergeAllWithDelete(/*needReverseMapping*/ false, /*multiTargetSegment*/ false);
    TestMergeAllWithDelete(/*needReverseMapping*/ true, /*multiTargetSegment*/ false);

    TestMergeAllWithoutDelete(/*needReverseMapping*/ false, /*multiTargetSegment*/ true);
    TestMergeAllWithoutDelete(/*needReverseMapping*/ true, /*multiTargetSegment*/ true);
    TestMergeAllWithDelete(/*needReverseMapping*/ false, /*multiTargetSegment*/ true);
    TestMergeAllWithDelete(/*needReverseMapping*/ true, /*multiTargetSegment*/ true);
}

TEST_F(ReclaimMapTest, TestMergeSegmentApart)
{
    TestMergeSegmentApartWithoutDelete(/*multiTargetSegment*/ false);
    TestMergeSegmentApartWithDelete(/*multiTargetSegment*/ false);

    TestMergeSegmentApartWithoutDelete(/*multiTargetSegment*/ true);
    TestMergeSegmentApartWithDelete(/*multiTargetSegment*/ true);
}

}} // namespace indexlibv2::table
