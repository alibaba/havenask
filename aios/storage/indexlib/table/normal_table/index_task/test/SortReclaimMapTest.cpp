#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/table/normal_table/index_task/SortedReclaimMap.h"
#include "indexlib/table/normal_table/index_task/test/ReclaimMapUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::table {
class FakeSingleAttributeDiskIndexer : public index::AttributeDiskIndexer
{
public:
    FakeSingleAttributeDiskIndexer(const std::vector<int32_t>& attrValues) : _attrValues(attrValues) {}
    bool IsInMemory() const override { return false; }
    bool Updatable() const override { return false; }
    bool UpdateField(docid_t, const autil::StringView&, bool, const uint64_t*) override { return false; }
    std::pair<Status, uint64_t> GetOffset(docid_t, const std::shared_ptr<ReadContextBase>&) const override
    {
        return {Status::OK(), 0};
    }
    std::shared_ptr<ReadContextBase> CreateReadContextPtr(autil::mem_pool::Pool*) const override { return nullptr; };
    Status Open(const std::shared_ptr<config::IIndexConfig>&,
                const std::shared_ptr<indexlib::file_system::IDirectory>&) override
    {
        return Status::OK();
    }
    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>&,
                           const std::shared_ptr<indexlib::file_system::IDirectory>&) override
    {
        return 0;
    };
    size_t EvaluateCurrentMemUsed() override { return 0; }
    Status SetPatchReader(const std::shared_ptr<indexlibv2::index::AttributePatchReader>&, docid_t) override
    {
        return Status::OK();
    }
    bool Read(docid_t docId, const std::shared_ptr<ReadContextBase>&, uint8_t* buf, uint32_t, uint32_t& dataLen,
              bool& isNull) override
    {
        using T = int32_t;
        *((T*)buf) = _attrValues[docId];
        isNull = false;
        dataLen = sizeof(T);
        return true;
    }
    bool Read(docid_t docId, std::string* value, autil::mem_pool::Pool* pool) override { return false; }
    uint32_t TEST_GetDataLength(docid_t, autil::mem_pool::Pool*) const override { return sizeof(int32_t); }

private:
    std::vector<int32_t> _attrValues;
};

class SortReclaimMapTest : public TESTBASE
{
public:
    SortReclaimMapTest() = default;
    ~SortReclaimMapTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    void TestCaseForMergePart();
    void TestSortMergePartWithoutDelete(bool needReverseMapping, bool splitSegment);
    void TestSortMergePartWithDelete(bool needReverseMapping, bool splitSegment);
    void TestSortMergeAllWithoutDelete(bool needReverseMapping, bool splitSegment);
    void TestSortMergeAllWithDelete(bool needReverseMapping, bool splitSegment);
    void InnerTestMerge(const std::vector<index::IIndexMerger::SourceSegment>& sourceSegments,
                        const std::vector<segmentid_t>& seg2Merge, bool needReverseMapping,
                        index::IndexTestUtil::ToDelete toDel,
                        const std::string& expectedDocIdStr,        // reclaimed docids
                        const std::string& expectedReverseDocIdStr, // old docids
                        const std::string& expectedSegIdStr,        // old segids
                        bool multiTargetSegment);
    void TestMergeAll(index::IndexTestUtil::ToDelete toDel, const std::string& expectedDocIdStr,
                      bool needReverseMapping, const std::string& expectedOldDocIdStr,
                      const std::string& expectedSegIdStr, bool splitSegment);

    void TestMergePartIntenal(index::IndexTestUtil::ToDelete toDel, const std::string& expectedDocIdStr,
                              bool needReverseMapping, const std::string& expectedOldDocIdStr,
                              const std::string& expectedSegIdStr, bool splitSegment);
    void MakeSegmentMergeInfos(const std::vector<index::IIndexMerger::SourceSegment>& sourceSegments,
                               const std::vector<segmentid_t>& seg2Merge, bool multiTargetSegment,
                               index::IIndexMerger::SegmentMergeInfos& segMergeInfos);

private:
    std::string _rootPath;
    std::shared_ptr<indexlib::file_system::IFileSystem> _fs;
    indexlib::file_system::DirectoryPtr _rootDir;
    std::shared_ptr<index::AttributeConfig> _attrConfig1;
    std::shared_ptr<index::AttributeConfig> _attrConfig2;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.table, SortReclaimMapTest);
const static std::string ATTRIBUTE_NAME1 = "attr_name1";
const static std::string ATTRIBUTE_NAME2 = "attr_name2";

void SortReclaimMapTest::setUp()
{
    indexlib::file_system::FileSystemOptions fsOptions;
    _rootPath = GET_TEMP_DATA_PATH();
    _fs = indexlib::file_system::FileSystemCreator::Create("test", _rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(_fs);

    _attrConfig1 = std::make_shared<index::AttributeConfig>();
    auto status = _attrConfig1->Init(std::make_shared<config::FieldConfig>(ATTRIBUTE_NAME1, ft_integer, false));
    ASSERT_TRUE(status.IsOK());
    _attrConfig2 = std::make_shared<index::AttributeConfig>();
    status = _attrConfig2->Init(std::make_shared<config::FieldConfig>(ATTRIBUTE_NAME2, ft_integer, false));
    ASSERT_TRUE(status.IsOK());
}

void SortReclaimMapTest::tearDown() {}

void SortReclaimMapTest::TestSortMergePartWithoutDelete(bool needReverseMapping, bool splitSegment)
{
    // 2,3,5,3
    // 31, 3 |  [19], [11], [8] | [21], [19], [16], [2], [1] | 13, 12, 11
    std::string expectedDocIdStr = "-2,-2,1,4,5,0,2,3,6,7,-2,-2,-2";
    std::string expectedOldDocIdStr = "";
    std::string expectedSegIdStr = "";
    if (needReverseMapping) {
        // 21 19 19 16 11 8 2 1
        expectedOldDocIdStr = "0,0,1,2,1,2,3,4";
        expectedSegIdStr = "2,1,2,2,1,1,2,2";
    }
    if (splitSegment) {
        // 21 19 11 2 | 19 16 8 1
        expectedDocIdStr = "-2,-2,4,2,6,0,1,5,3,7,-2,-2,-2";
        if (needReverseMapping) {
            expectedOldDocIdStr = "0,1,1,3,0,2,2,4";
            expectedSegIdStr = "2,2,1,2,1,2,1,2";
        }
    }

    TestMergePartIntenal(index::IndexTestUtil::NoDelete, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                         expectedSegIdStr, splitSegment);
}

void SortReclaimMapTest::TestSortMergePartWithDelete(bool needReverseMapping, bool splitSegment)
{
    // 31, 3 |  19, [11], 8 | 21, [19], 16, [2], 1 | 13, 12, 11
    AUTIL_LOG(INFO, "ut begin with needReverseMapping [%d], splitSegment[%d]", needReverseMapping, splitSegment);
    std::string expectedDocIdStr = "-2,-2,-1,1,-1,-1,0,-1,2,-1, -2,-2,-2";
    std::string expectedOldDocIdStr = "";
    std::string expectedSegIdStr = "";
    if (needReverseMapping) {
        // 19 11 2
        expectedOldDocIdStr = "1,1,3";
        expectedSegIdStr = "2,1,2";
    }
    if (splitSegment) {
        // 19 2 | 11
        expectedDocIdStr = "-2,-2,-1,2,-1,-1,0,-1,1,-1, -2,-2,-2";
        if (needReverseMapping) {
            expectedOldDocIdStr = "1,3,1";
            expectedSegIdStr = "2,2,1";
        }
    }
    TestMergePartIntenal(index::IndexTestUtil::DeleteEven, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                         expectedSegIdStr, splitSegment);
}

void SortReclaimMapTest::TestMergePartIntenal(index::IndexTestUtil::ToDelete toDel, const std::string& expectedDocIdStr,
                                              bool needReverseMapping, const std::string& expectedOldDocIdStr,
                                              const std::string& expectedSegIdStr, bool splitSegment)
{
    std::vector<uint32_t> docCounts = {2, 3, 5, 3};
    std::vector<segmentid_t> segIds = {0, 1, 2, 3};
    std::vector<segmentid_t> seg2Merge = {1, 2};

    std::vector<index::IIndexMerger::SourceSegment> sourceSegments;
    ReclaimMapUtil::PrepareSegments(_rootDir, docCounts, segIds, toDel, sourceSegments);
    InnerTestMerge(sourceSegments, seg2Merge, needReverseMapping, toDel, expectedDocIdStr, expectedOldDocIdStr,
                   expectedSegIdStr, splitSegment);
}

void SortReclaimMapTest::TestSortMergeAllWithoutDelete(bool needReverseMapping, bool splitSegment)
{
    // [31], [3] | [19], [11], [8] | [21], [19], [16], [2], [1] | [13], [12], [11]
    std::string expectedDocIdStr = "0,10,2,7,9,1,3,4,11,12,5,6,8";
    std::string expectedOldDocIdStr = "";
    std::string expectedSegIdStr = "";
    if (needReverseMapping) {
        // 31 21 19 19 16 13 12 11 11 8 3 2 1
        expectedOldDocIdStr = "0,0,0,1,2,0,1,1,2,2,1,3,4";
        expectedSegIdStr = "0,2,1,2,2,3,3,1,3,1,0,2,2";
    }
    if (splitSegment) {
        // 31 19 16 12 11 3 1 | 21 19 13 11 8 2
        expectedDocIdStr = "0,5,1,10,11,7,8,2,12,6,9,3,4";
        if (needReverseMapping) {
            expectedOldDocIdStr = "0,0,2,1,2,1,4,0,1,0,1,2,3";
            expectedSegIdStr = "0,1,2,3,3,0,2,2,2,3,1,1,2";
        }
    }

    TestMergeAll(index::IndexTestUtil::NoDelete, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                 expectedSegIdStr, splitSegment);
}

void SortReclaimMapTest::TestSortMergeAllWithDelete(bool needReverseMapping, bool splitSegment)
{
    // 31, [3], 19, [11], 8, 21, [19], 16, [2], 1, 13, [12], 11
    std::string expectedDocIdStr = "-1,3,-1,2,-1,-1,0,-1,4,-1,-1,1,-1";
    std::string expectedOldDocIdStr = "";
    std::string expectedSegIdStr = "";
    if (needReverseMapping) {
        // 19 12 11 3 2
        expectedOldDocIdStr = "1,1,1,1,3";
        expectedSegIdStr = "2,3,1,0,2";
    }
    if (splitSegment) {
        // 19 11 2 | 12 3
        expectedDocIdStr = "-1,4,-1,1,-1,-1,0,-1,2,-1,-1,3,-1";
        if (needReverseMapping) {
            expectedOldDocIdStr = "1,1,3,1,1";
            expectedSegIdStr = "2,1,2,3,0";
        }
    }
    TestMergeAll(index::IndexTestUtil::DeleteEven, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                 expectedSegIdStr, splitSegment);
}

void SortReclaimMapTest::TestMergeAll(index::IndexTestUtil::ToDelete toDel, const std::string& expectedDocIdStr,
                                      bool needReverseMapping, const std::string& expectedOldDocIdStr,
                                      const std::string& expectedSegIdStr, bool splitSegment)
{
    std::vector<uint32_t> docCounts = {2, 3, 5, 3};
    std::vector<segmentid_t> segIds = {0, 1, 2, 3};
    std::vector<segmentid_t> seg2Merge = {0, 1, 2, 3};

    std::vector<index::IIndexMerger::SourceSegment> sourceSegments;
    ReclaimMapUtil::PrepareSegments(_rootDir, docCounts, segIds, toDel, sourceSegments);
    InnerTestMerge(sourceSegments, seg2Merge, needReverseMapping, toDel, expectedDocIdStr, expectedOldDocIdStr,
                   expectedSegIdStr, splitSegment);
}

void SortReclaimMapTest::InnerTestMerge(const std::vector<index::IIndexMerger::SourceSegment>& sourceSegments,
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

    std::vector<std::shared_ptr<indexlibv2::index::AttributeConfig>> attrConfigs = {_attrConfig1, _attrConfig2};
    SortedReclaimMap sortedReclaimMap("sortReclaimMap", index::DocMapper::GetDocMapperType());
    auto status = sortedReclaimMap.TEST_Init(targetSegment->segmentId, srcSegments, attrConfigs,
                                             {config::sp_desc, config::sp_asc}, segmentSplitHandler);
    assert(status.IsOK());

    ReclaimMapUtil::CheckAnswer(sortedReclaimMap, expectedDocIdStr, needReverseMapping, expectedReverseDocIdStr,
                                expectedSegIdStr);
}

void SortReclaimMapTest::MakeSegmentMergeInfos(const std::vector<index::IIndexMerger::SourceSegment>& sourceSegments,
                                               const std::vector<segmentid_t>& seg2Merge, bool multiTargetSegment,
                                               index::IIndexMerger::SegmentMergeInfos& segMergeInfos)
{
    std::vector<int32_t> attrValues1 = {31, 3, /**/ 19, 11, 8, /**/ 21, 19, 16, 2, 1, /**/ 13, 12, 11};
    std::vector<int32_t> attrValues2 = attrValues1;

    segMergeInfos.srcSegments.clear();
    docid_t newBaseDocId = 0;
    docid_t oldBaseDocId = 0;
    for (size_t i = 0; i < sourceSegments.size(); i++) {
        size_t docCount = sourceSegments[i].segment->GetSegmentInfo()->docCount;
        if (std::find(seg2Merge.begin(), seg2Merge.end(), sourceSegments[i].segment->GetSegmentId()) !=
            seg2Merge.end()) {
            auto newSourceSegment = sourceSegments[i];
            newSourceSegment.baseDocid = newBaseDocId;
            std::vector<int32_t> localAttrValues1(attrValues1.begin() + oldBaseDocId,
                                                  attrValues1.begin() + oldBaseDocId + docCount);
            newSourceSegment.segment->AddIndexer(index::ATTRIBUTE_INDEX_TYPE_STR, ATTRIBUTE_NAME1,
                                                 std::make_shared<FakeSingleAttributeDiskIndexer>(localAttrValues1));

            std::vector<int32_t> localAttrValues2(attrValues2.begin() + oldBaseDocId,
                                                  attrValues2.begin() + oldBaseDocId + docCount);
            newSourceSegment.segment->AddIndexer(index::ATTRIBUTE_INDEX_TYPE_STR, ATTRIBUTE_NAME2,
                                                 std::make_shared<FakeSingleAttributeDiskIndexer>(localAttrValues2));
            segMergeInfos.srcSegments.push_back(newSourceSegment);
            newBaseDocId += docCount;
        }
        oldBaseDocId += docCount;
    }

    auto maxSegId = sourceSegments.back().segment->GetSegmentId();
    auto lastSegmentInfo = sourceSegments.back().segment->GetSegmentInfo();
    ReclaimMapUtil::CreateTargetSegment(_rootDir, ++maxSegId, lastSegmentInfo, segMergeInfos);
    if (multiTargetSegment) {
        ReclaimMapUtil::CreateTargetSegment(_rootDir, ++maxSegId, lastSegmentInfo, segMergeInfos);
    }
}

TEST_F(SortReclaimMapTest, TestCaseForSortMergePart)
{
    TestSortMergePartWithoutDelete(false, false);
    TestSortMergePartWithoutDelete(true, false);
    TestSortMergePartWithDelete(false, false);
    TestSortMergePartWithDelete(true, false);

    TestSortMergePartWithoutDelete(false, true);
    TestSortMergePartWithoutDelete(true, true);
    TestSortMergePartWithDelete(false, true);
    TestSortMergePartWithDelete(true, true);
}

TEST_F(SortReclaimMapTest, TestMergeAll)
{
    TestSortMergeAllWithoutDelete(false, false);
    TestSortMergeAllWithoutDelete(true, false);
    TestSortMergeAllWithDelete(false, false);
    TestSortMergeAllWithDelete(true, false);

    TestSortMergeAllWithoutDelete(false, true);
    TestSortMergeAllWithoutDelete(true, true);
    TestSortMergeAllWithDelete(false, true);
    TestSortMergeAllWithDelete(true, true);
}
} // namespace indexlibv2::table
