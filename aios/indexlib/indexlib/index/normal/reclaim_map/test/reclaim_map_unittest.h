#include "indexlib/common_define.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class FakeAttributeSegmentReaderRM : public AttributeSegmentReader
{
public:
    FakeAttributeSegmentReaderRM(const std::vector<int32_t>& attrValues)
        : mAttrValues(attrValues)
    {
    }
    ~FakeAttributeSegmentReaderRM() {}

public:
    bool IsInMemory() const { return true; }
    uint32_t GetDataLength(docid_t docId) const  { return 0; }
    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen)
    {
        int32_t* int32Buf = (int32_t*)buf;
        *int32Buf = mAttrValues[docId];
        return true;
    }

    uint64_t GetOffset(docid_t docId) const
    {
        return docId * sizeof(int32_t);
    }
private:
    std::vector<int32_t> mAttrValues;
};

class FakeInMemorySegmentReader : public InMemorySegmentReader
{
public:
    FakeInMemorySegmentReader(const std::vector<AttributeSegmentReaderPtr>& attrSegReades)
        : mIter(0)
    {
        mAttrSegReaders = attrSegReades;
    }
public:
    AttributeSegmentReaderPtr GetAttributeSegmentReader(const std::string& name)
    {
        return mAttrSegReaders[mIter++];
    }
private:
    std::vector<AttributeSegmentReaderPtr> mAttrSegReaders;
    uint32_t  mIter;
};

class ReclaimMapTest : public INDEXLIB_TESTBASE 
{
public:
    DECLARE_CLASS_NAME(ReclaimMapTest);

    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestCaseForMergePart();
    void TestCaseForSortMergePart();
    void TestCaseForMergeAll();
    void TestCaseForSortMergeAll();
    void TestCaseForMergeSegmentApart();
    void TestCaseForStoreAndLoad();
    void TestCaseForSubReclaimMapInit();
    void TestLoadDocIdArrayWithOldVersion();

private:
    void TestMergePartWithoutDelete(bool needReverseMapping, bool multiTargetSegment = false);
    void TestMergePartWithDelete(bool needReverseMapping, bool multiTargetSegment = false);
    void TestSortMergePartWithoutDelete(bool needReverseMapping, bool multiTargetSegment = false);
    void TestSortMergePartWithDelete(bool needReverseMapping, bool multiTargetSegment = false);
    void TestMergePart(IndexTestUtil::ToDelete toDel, 
                       bool sortByWeight, 
                       const std::string& expectedDocIdStr,
                       bool needReverseMapping = false,
                       const std::string& expectedOldDocIdStr = "",
                       const std::string& expectedSegIdStr = "",
                       bool splitSegment = false);

    void TestMergeAllWithoutDelete(bool needReverseMapping, bool multiTargetSegment = false);
    void TestMergeAllWithDelete(bool needReverseMapping, bool multiTargetSegment = false);
    void TestSortMergeAllWithoutDelete(bool needReverseMapping, bool multiTargetSegment = false);
    void TestSortMergeAllWithDelete(bool needReverseMapping, bool multiTargetSegment = false);
    void TestMergeAll(IndexTestUtil::ToDelete toDel,
                      bool sortByWeight,
                      const std::string& expectedDocIdStr,
                      bool needReverseMapping = false,
                      const std::string& expectedOldDocIdStr = "",
                      const std::string& expectedSegIdStr = "",
                      bool multiTargetSegment = false);

    void TestMergeSegmentApartWithoutDelete(bool multiTargetSegment = false);
    void TestMergeSegmentApartWithDelete(bool multiTargetSegment = false);
    void TestMergeSegmentApart(IndexTestUtil::ToDelete toDel, 
                               const std::string& expectedDocIdStr,
                               bool needReverseMapping = false,
                               bool multiTargetSegment = false);

    void Prepare(const std::string& docCountsStr,
                 const std::string& segIdStr,
                 const std::string& seg2MergeStr,
                 IndexTestUtil::ToDelete toDel,
                 IndexTestUtil::ToDelete subDocToDel = IndexTestUtil::NoDelete,
                 bool multiTargetSegment = false);
                 

    void InnerTestMerge(const std::string& docCountsStr,     // input doc counts
                        const std::string& segIdStr,         // input segment ids
                        const std::string& seg2MergeStr,     // to merge segids
                        bool sortByWeight,
                        bool needReverseMapping,
                        IndexTestUtil::ToDelete toDel,
                        const std::string& expectedDocIdStr, // reclaimed docids
                        const std::string& expectedReverseDocIdStr, // old docids
                        const std::string& expectedSegIdStr, // old segids
                        bool multiTargetSegment = false);

    void CheckJoinValue(const ReclaimMap& reclaimMap, 
                        const std::string& expectedJoinValueStr);
    void CheckAnswer(const ReclaimMap& reclaimMap, 
                     const std::string& expectedDocIdStr,
                     bool needReverseMapping,
                     const std::string& expectedOldDocIdStr,
                     const std::string& expectedSegIdStr);

    void CreateSegmentMergeInfos(
            const std::vector<uint32_t>& docCounts, 
            const std::vector<segmentid_t>& segmentIds,
            const DeletionMapReaderPtr& deletionMapReader, 
            const merger::MergePlan& plan,
            index_base::SegmentMergeInfos& segMergeInfos,
            bool isSubDoc = false);

    void CreateSegmentMergeInfosForSort(
            const std::vector<uint32_t>& docCounts, 
            const std::vector<segmentid_t>& segmentIds,
            const DeletionMapReaderPtr& deletionMapReader, 
            const merger::MergePlan& plan,
            index_base::SegmentMergeInfos& segMergeInfos,
            const std::vector<int32_t>& highAttrValues,
            const std::vector<int32_t>& lowAttrValues);

    void InnerSubReclaimMapInit(
        IndexTestUtil::ToDelete mainToDel, IndexTestUtil::ToDelete subToDel, 
        bool sortByWeight, bool needReverseMapping, const std::string& expectedDocIdStr, 
        const std::string& expectedOldDocIdStr, const std::string& expectedSegIdStr,
        const std::string& expectedMainJoinValueStr, const std::string& expectedSubJoinValueStr);


private:
    std::string mRootDir;
    std::string mDocCountStr;
    std::vector<int32_t> mHighAttrValue;
    std::vector<int32_t> mLowAttrValue;
    std::vector<uint32_t> mDocCounts;
    std::vector<segmentid_t> mSegIds;
    
    merger::SegmentDirectoryPtr mSegDir;
    DeletionMapReaderPtr mDelMapReader;
    merger::MergePlan mMergePlan;
    std::vector<config::AttributeConfigPtr> mAttrConfigs;
    std::vector<SortPattern> mPatterns;

    uint32_t mSubDocCount;
    std::vector<uint32_t> mSubDocCounts;
    config::AttributeConfigPtr mMainJoinAttrConfig;
    std::vector<docid_t> mMainJoinAttrValue;
    merger::SegmentDirectoryPtr mSubSegDir;
    DeletionMapReaderPtr mSubDelMapReader;
};

INDEXLIB_UNIT_TEST_CASE(ReclaimMapTest, TestCaseForMergePart);
INDEXLIB_UNIT_TEST_CASE(ReclaimMapTest, TestCaseForSortMergePart);
INDEXLIB_UNIT_TEST_CASE(ReclaimMapTest, TestCaseForMergeAll);
INDEXLIB_UNIT_TEST_CASE(ReclaimMapTest, TestCaseForSortMergeAll);
INDEXLIB_UNIT_TEST_CASE(ReclaimMapTest, TestCaseForMergeSegmentApart);
INDEXLIB_UNIT_TEST_CASE(ReclaimMapTest, TestCaseForStoreAndLoad);
INDEXLIB_UNIT_TEST_CASE(ReclaimMapTest, TestCaseForSubReclaimMapInit);
INDEXLIB_UNIT_TEST_CASE(ReclaimMapTest, TestLoadDocIdArrayWithOldVersion);

IE_NAMESPACE_END(index);
