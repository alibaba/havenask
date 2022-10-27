#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/document_merge_info_heap.h"
#include "indexlib/index/test/index_test_util.h"

using namespace std;

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

class DocumentMergeInfoHeapTest : public INDEXLIB_TESTBASE
{
public:
    DocumentMergeInfoHeapTest()
    {
    }

    DECLARE_CLASS_NAME(DocumentMergeInfoHeapTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForOneSegment()
    {
        vector<uint32_t> docCounts;
        docCounts.push_back(26);

        vector<set<docid_t> > emptyDelDocIdSets;        
        emptyDelDocIdSets.push_back(set<docid_t>());
        ReclaimMapPtr reclaimMap =
            IndexTestUtil::CreateReclaimMap(docCounts, emptyDelDocIdSets);
        InnerTest(docCounts, emptyDelDocIdSets, reclaimMap);

        set<docid_t> delIdSet;
        delIdSet.insert(0);
        delIdSet.insert(10);

        vector<set<docid_t> > delDocIdSets;
        delDocIdSets.push_back(delIdSet);
        reclaimMap = IndexTestUtil::CreateReclaimMap(docCounts, delDocIdSets);
        InnerTest(docCounts, delDocIdSets, reclaimMap);
    }

    void TestCaseForMultiSegments()
    {
        vector<uint32_t> docCounts;
        docCounts.push_back(10);
        docCounts.push_back(100);
        docCounts.push_back(30);

        vector<set<docid_t> > emptyDelDocIdSets;
        emptyDelDocIdSets.push_back(set<docid_t>());
        emptyDelDocIdSets.push_back(set<docid_t>());
        emptyDelDocIdSets.push_back(set<docid_t>());
        ReclaimMapPtr reclaimMap =
            IndexTestUtil::CreateReclaimMap(docCounts, emptyDelDocIdSets, false, {0, 4});
        InnerTest(docCounts, emptyDelDocIdSets, reclaimMap);

        set<docid_t> delDocIdSet1;
        delDocIdSet1.insert(0);
        delDocIdSet1.insert(5);
        set<docid_t> delDocIdSet2;
        delDocIdSet2.insert(3);

        vector<set<docid_t> > delDocIdSets;
        delDocIdSets.push_back(delDocIdSet1);
        delDocIdSets.push_back(delDocIdSet2);
        delDocIdSets.push_back(set<docid_t>());

        reclaimMap = IndexTestUtil::CreateReclaimMap(docCounts, delDocIdSets);
        InnerTest(docCounts, delDocIdSets, reclaimMap);

        reclaimMap = IndexTestUtil::CreateSortMergingReclaimMap(docCounts, delDocIdSets);
        InnerTest(docCounts, delDocIdSets, reclaimMap);
    }


private:
    void InnerTest(const vector<uint32_t>& docCounts,             
                   vector<set<docid_t> >& delDocIdSets,
                   const ReclaimMapPtr& reclaimMap)
    {
        IndexTestUtil::ResetDir(mRootDir);
             
        merger::SegmentDirectoryPtr segDir = IndexTestUtil::CreateSegmentDirectory(mRootDir, docCounts.size());
        DeletionMapReaderPtr deletionMapReader = 
            IndexTestUtil::CreateDeletionMap(segDir, docCounts, delDocIdSets);
        
        SegmentMergeInfos segMergeInfos;
        CreateSegmentMergeInfos(docCounts, deletionMapReader, segMergeInfos);
        
        Check(segMergeInfos, reclaimMap);   
    }

    void Check(const SegmentMergeInfos& segMergeInfos,
                   const ReclaimMapPtr& reclaimMap)
    {
        DocumentMergeInfoHeap heap;
        heap.Init(segMergeInfos, reclaimMap);

        DocumentMergeInfo info;
        docid_t lastNewDocId = docid_t(-1);
        uint32_t totalNewDocIds = 0;
        while(heap.GetNext(info))
        {
            totalNewDocIds++;
            segmentid_t segId = info.segmentIndex;
            docid_t oldDocId = info.oldDocId;
            docid_t newDocId = info.newDocId;
            
            INDEXLIB_TEST_TRUE(newDocId != INVALID_DOCID);
            INDEXLIB_TEST_EQUAL(lastNewDocId + 1, newDocId);
            INDEXLIB_TEST_EQUAL(
                    reclaimMap->GetNewId(oldDocId + segMergeInfos[segId].baseDocId),
                    newDocId);
            lastNewDocId = newDocId;
        }

        INDEXLIB_TEST_EQUAL(reclaimMap->GetTotalDocCount() - 
                            reclaimMap->GetDeletedDocCount(),
                            totalNewDocIds);
    }

    void CreateSegmentMergeInfos(vector<uint32_t> docCounts,
                                 const DeletionMapReaderPtr& deletionMapReader,
                                 SegmentMergeInfos& segMergeInfos)
    {
        docid_t baseDocId = 0;
        for (size_t i = 0; i < docCounts.size(); ++i)
        {
            SegmentInfo segInfo;
            segInfo.docCount = docCounts[i];
            uint32_t deletedDocCount = deletionMapReader->GetDeletedDocCount(i);
            SegmentMergeInfo segMergeInfo(i, segInfo, deletedDocCount, baseDocId);
            segMergeInfos.push_back(segMergeInfo);
            baseDocId += docCounts[i];
        }
    }

private:
    string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(DocumentMergeInfoHeapTest, TestCaseForOneSegment);
INDEXLIB_UNIT_TEST_CASE(DocumentMergeInfoHeapTest, TestCaseForMultiSegments);

IE_NAMESPACE_END(index);
