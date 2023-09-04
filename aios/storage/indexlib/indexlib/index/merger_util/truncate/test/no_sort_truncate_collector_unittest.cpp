#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/no_sort_truncate_collector.h"
#include "indexlib/index/merger_util/truncate/test/fake_truncate_attribute_reader.h"
#include "indexlib/index/test/fake_index_reader.h"
#include "indexlib/test/unittest.h"
using namespace std;

namespace indexlib::index::legacy {

class NoSortTruncateCollectorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(NoSortTruncateCollectorTest);
    void CaseSetUp() override { mBucketVecAllocator.reset(new BucketVectorAllocator); }

    void CaseTearDown() override {}

    void TestCaseForNoDistinct()
    {
        static uint32_t EXPECTED_DOC_COUNT = 3;
        DocCollectorPtr docCollector(new NoSortTruncateCollector(
            EXPECTED_DOC_COUNT, EXPECTED_DOC_COUNT, DocFilterProcessorPtr(), DocDistinctorPtr(), mBucketVecAllocator));

        // add docs(docid is 0, 1, ..., 9) and truncate them
        const uint32_t DOC_COUNT_TO_ADD = 10;
        vector<docid_t> addDocIds;
        for (size_t i = 0; i < DOC_COUNT_TO_ADD; ++i) {
            addDocIds.push_back(i);
        }
        std::shared_ptr<PostingIterator> postingIt(new FakePostingIterator(addDocIds));
        docCollector->CollectDocIds(index::DictKeyInfo(0), postingIt);

        // check truncated result(expected truncated docid is 0, 1, 2)
        const DocCollector::DocIdVector& docIds = *docCollector->GetTruncateDocIds();
        INDEXLIB_TEST_EQUAL(EXPECTED_DOC_COUNT, docIds.size());
        for (size_t i = 0; i < EXPECTED_DOC_COUNT; ++i) {
            INDEXLIB_TEST_EQUAL((docid_t)i, docIds[i]);
        }
        const docid_t EXPECTED_LAST_DOCID = 2;
        INDEXLIB_TEST_EQUAL(EXPECTED_LAST_DOCID, docCollector->GetMinValueDocId());
    }

    void TestCaseForDistinctSmallerThanDocLimit()
    {
        string attrValue = "1;2;2;2;3;3;4;4;5;5";
        DocDistinctorPtr docDistict = CreateDocDistinctor(attrValue, 2);
        InnerTestForDistinct(5, 3, 2, 3, 4, docDistict);
    }

    void TestCaseForDistinctEqualToDocLimit()
    {
        string attrValue = "1;2;2;2;3;3;4;4;5;5";
        DocDistinctorPtr docDistict = CreateDocDistinctor(attrValue, 3);
        InnerTestForDistinct(10, 5, 4, 5, 7, docDistict);
    }

    void TestCaseForDistinctLargerThanDocLimit()
    {
        string attrValue = "1;2;2;2;3;3;4;4;5;5";
        DocDistinctorPtr docDistict = CreateDocDistinctor(attrValue, 3);
        InnerTestForDistinct(8, 5, 4, 3, 6, docDistict);
    }

    void TestCaseForDistinctEqualToMaxDocLimit()
    {
        string attrValue = "1;2;2;2;3;3;4;4;5;5";
        DocDistinctorPtr docDistict = CreateDocDistinctor(attrValue, 4);
        InnerTestForDistinct(8, 5, 4, 4, 5, docDistict);
    }

private:
    void InnerTestForDistinct(size_t docCountToAdd, size_t expectedDocCount, docid_t expectedLastDocId,
                              uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                              const DocDistinctorPtr& docDistinctor)
    {
        DocCollectorPtr docCollector(new NoSortTruncateCollector(
            minDocCountToReserve, maxDocCountToReserve, DocFilterProcessorPtr(), docDistinctor, mBucketVecAllocator));
        TruncateAndCheck(docCountToAdd, expectedDocCount, expectedLastDocId, docCollector);
    }

    void TruncateAndCheck(size_t docCountToAdd, size_t expectedDocCount, docid_t expectedLastDocId,
                          const DocCollectorPtr& docCollector)
    {
        vector<docid_t> addDocIds;

        for (size_t i = 0; i < docCountToAdd; ++i) {
            addDocIds.push_back((docid_t)i);
        }
        std::shared_ptr<PostingIterator> postingIt(new FakePostingIterator(addDocIds));

        docCollector->CollectDocIds(index::DictKeyInfo(0), postingIt);
        const DocCollector::DocIdVector& docIds = *docCollector->GetTruncateDocIds();
        INDEXLIB_TEST_EQUAL(expectedDocCount, docIds.size());
        for (size_t i = 0; i < expectedDocCount; ++i) {
            INDEXLIB_TEST_EQUAL((docid_t)i, docIds[i]);
        }
        INDEXLIB_TEST_EQUAL(expectedLastDocId, docCollector->GetMinValueDocId());
    }

    // attrValue separate by ';', example: "3;4;5"
    DocDistinctorPtr CreateDocDistinctor(string attrValue, uint64_t minDistinctValueCountToReserve)
    {
        FakeTruncateAttributeReader<int32_t>* pAttrReader = new FakeTruncateAttributeReader<int32_t>();
        pAttrReader->SetAttrValue(attrValue);
        TruncateAttributeReaderPtr attrReader(pAttrReader);
        return DocDistinctorPtr(new DocDistinctorTyped<int32_t>(attrReader, minDistinctValueCountToReserve));
    }

private:
    BucketVectorAllocatorPtr mBucketVecAllocator;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, NoSortTruncateCollectorTest);

INDEXLIB_UNIT_TEST_CASE(NoSortTruncateCollectorTest, TestCaseForNoDistinct);
INDEXLIB_UNIT_TEST_CASE(NoSortTruncateCollectorTest, TestCaseForDistinctSmallerThanDocLimit);
INDEXLIB_UNIT_TEST_CASE(NoSortTruncateCollectorTest, TestCaseForDistinctEqualToDocLimit);
INDEXLIB_UNIT_TEST_CASE(NoSortTruncateCollectorTest, TestCaseForDistinctLargerThanDocLimit);
INDEXLIB_UNIT_TEST_CASE(NoSortTruncateCollectorTest, TestCaseForDistinctEqualToMaxDocLimit);
} // namespace indexlib::index::legacy
