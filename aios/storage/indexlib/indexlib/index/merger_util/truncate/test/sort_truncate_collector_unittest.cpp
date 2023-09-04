#include "indexlib/common_define.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/index/merger_util/truncate/bucket_map.h"
#include "indexlib/index/merger_util/truncate/doc_collector.h"
#include "indexlib/index/merger_util/truncate/doc_distinctor.h"
#include "indexlib/index/merger_util/truncate/sort_truncate_collector.h"
#include "indexlib/index/merger_util/truncate/test/fake_truncate_attribute_reader.h"
#include "indexlib/index/test/fake_index_reader.h"
#include "indexlib/test/unittest.h"
using namespace std;

namespace indexlib::index::legacy {

#define CheckDocIdVector(expectDocsVec, truncatedDocIds)                                                               \
    INDEXLIB_TEST_EQUAL(expectDocsVec.size(), truncatedDocIds.size());                                                 \
    for (size_t i = 0; i < expectDocsVec.size(); ++i) {                                                                \
        INDEXLIB_TEST_EQUAL((docid_t)expectDocsVec[i], truncatedDocIds[i]);                                            \
    }

class SortTruncateCollectorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SortTruncateCollectorTest);
    void CaseSetUp() override
    {
        mBucketVecAllocator.reset(new BucketVectorAllocator);
        mTruncateProfilePtr.reset(new config::TruncateProfile());
        config::SortParam sortParam;
        sortParam.mSortField = DOC_PAYLOAD_FIELD_NAME;
        mTruncateProfilePtr->mSortParams.emplace_back(sortParam);
    }

    void CaseTearDown() override {}

    void TestCaseForSimple()
    {
        // prepare data
        string sortedDocs = "2;1;0";
        BucketMapPtr bucketMap = CreateBucketMap(sortedDocs);
        DocCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(2, 2, DocFilterProcessorPtr(), DocDistinctorPtr(), mBucketVecAllocator, bucketMap,
                                      100, config::TruncateProfilePtr(new config::TruncateProfile()), nullptr));

        string docsToAdd = "0;1;2";
        vector<docid_t> docsToAddVec;
        autil::StringUtil::fromString(docsToAdd, docsToAddVec, ";");

        // truncate posting
        PostingIteratorPtr postingIt(new FakePostingIterator(docsToAddVec));
        sortTruncateCollector->CollectDocIds(DictKeyInfo(0), postingIt);

        // check truncated posting result

        string expectedDocs = "1;2";
        vector<int> expectedDocsVec;
        autil::StringUtil::fromString(expectedDocs, expectedDocsVec, ";");
        const DocCollector::DocIdVector& docIds = *sortTruncateCollector->GetTruncateDocIds();
        CheckDocIdVector(expectedDocsVec, docIds);
    }

    void TestCaseForDocPayload()
    {
        // prepare data
        DocCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(2, 2, DocFilterProcessorPtr(), DocDistinctorPtr(), mBucketVecAllocator,
                                      BucketMapPtr(), 100, mTruncateProfilePtr, nullptr));

        TruncateAndCheck("0;1;2", "1;2", sortTruncateCollector);
    }

    void TestCaseForMinDocCountToReserveLargerThanPostingDocCount()
    {
        BucketMapPtr bucketMap = CreateBucketMap("3;1;2;0");
        DocCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(5, 5, DocFilterProcessorPtr(), DocDistinctorPtr(), mBucketVecAllocator, bucketMap,
                                      100, config::TruncateProfilePtr(new config::TruncateProfile()), nullptr));
        TruncateAndCheck("0;1;2;3", "0;1;2;3", sortTruncateCollector);
    }

    void TestCaseForMinDocCountToReserveLargerThanPostingDocCount_SortByDocPayload()
    {
        // prepare data
        DocCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(5, 5, DocFilterProcessorPtr(), DocDistinctorPtr(), mBucketVecAllocator,
                                      BucketMapPtr(), 100, mTruncateProfilePtr, nullptr));
        TruncateAndCheck("0;1;2;3", "0;1;2;3", sortTruncateCollector);
    }

    void TestCaseForReset()
    {
        BucketMapPtr bucketMap = CreateBucketMap("0;1;2;3;4;5;6;7;8;9");
        DocCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(4, 4, DocFilterProcessorPtr(), DocDistinctorPtr(), mBucketVecAllocator, bucketMap,
                                      100, config::TruncateProfilePtr(new config::TruncateProfile()), nullptr));
        TruncateAndCheck("0;1;2", "0;1;2", sortTruncateCollector);
        sortTruncateCollector->Reset();

        // after reset, sortPostingTruncate can be reused.
        TruncateAndCheck("3;4;6;7;8", "3;4;6;7", sortTruncateCollector);
    }

    void TestCaseForReset_SortByDocPayload()
    {
        DocCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(4, 4, DocFilterProcessorPtr(), DocDistinctorPtr(), mBucketVecAllocator,
                                      BucketMapPtr(), 100, mTruncateProfilePtr, nullptr));
        TruncateAndCheck("0;1;2", "0;1;2", sortTruncateCollector);
        sortTruncateCollector->Reset();

        // after reset, sortPostingTruncate can be reused.
        TruncateAndCheck("3;4;6;7;8", "4;6;7;8", sortTruncateCollector);
    }

    void TestCaseForSortBucket()
    {
        BucketMapPtr bucketMap = CreateBucketMap("5;4;3;2;1;0");
        std::vector<docid_t> docIds;
        docIds.push_back(1);
        docIds.push_back(4);
        docIds.push_back(3);
        SortTruncateCollector::SortBucket(docIds, bucketMap);
        INDEXLIB_TEST_EQUAL((docid_t)4, docIds[0]);
        INDEXLIB_TEST_EQUAL((docid_t)3, docIds[1]);
        INDEXLIB_TEST_EQUAL((docid_t)1, docIds[2]);
    }

    void TestCaseForSortWithDocPayload()
    {
        SortTruncateCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(6, 6, DocFilterProcessorPtr(), DocDistinctorPtr(), mBucketVecAllocator,
                                      BucketMapPtr(), 100, mTruncateProfilePtr, nullptr));
        vector<SortTruncateCollector::Doc> docInfos;
        vector<int> payloads {7, 9, 8, 6, 5, 4, 2, 3, 1, 0};
        for (int i = 0; i < payloads.size(); i++) {
            SortTruncateCollector::Doc doc;
            doc.mDocId = payloads[i];
            doc.mPayload = payloads[i];
            docInfos.push_back(doc);
        }
        sortTruncateCollector->SortWithDocPayload(docInfos.begin(), docInfos.end(), 0, 6);

        // check min value docid
        INDEXLIB_TEST_EQUAL((docid_t)4, docInfos[5].mDocId);
        auto cmp = [](const SortTruncateCollector::Doc& doc1, const SortTruncateCollector::Doc& doc2) {
            return doc1.mDocId > doc2.mDocId;
        };
        sort(docInfos.begin(), docInfos.begin() + 6, cmp);
        for (int i = 9; i >= 4; i--) {
            INDEXLIB_TEST_EQUAL((docid_t)i, docInfos[9 - i].mDocId);
        }
    }

    void TestCaseForGetMinValueDocId()
    {
        BucketMapPtr bucketMap = CreateBucketMap("0;1;2;3;4;5;6;7;8;9");
        DocCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(4, 4, DocFilterProcessorPtr(), DocDistinctorPtr(), mBucketVecAllocator, bucketMap,
                                      100, config::TruncateProfilePtr(new config::TruncateProfile()), nullptr));
        TruncateAndCheck("0;1;3;5;9", "0;1;3;5", sortTruncateCollector);
        INDEXLIB_TEST_EQUAL((docid_t)5, sortTruncateCollector->GetMinValueDocId());
    }

    void TestCaseForGetMinValueDocId_SortByDocPayload()
    {
        DocCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(4, 4, DocFilterProcessorPtr(), DocDistinctorPtr(), mBucketVecAllocator,
                                      BucketMapPtr(), 100, mTruncateProfilePtr, nullptr));
        TruncateAndCheck("0;1;3;5;9", "1;3;5;9", sortTruncateCollector);
        INDEXLIB_TEST_EQUAL((docid_t)1, sortTruncateCollector->GetMinValueDocId());
    }

    void TestCaseForResultInOneBucket()
    {
        string sortedDocs;
        JoinDocIdsWithSemicolon(0, 100, sortedDocs);

        BucketMapPtr bucketMap = CreateBucketMap(sortedDocs);
        DocCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(3, 3, DocFilterProcessorPtr(), DocDistinctorPtr(), mBucketVecAllocator, bucketMap,
                                      100, config::TruncateProfilePtr(new config::TruncateProfile()), nullptr));
        TruncateAndCheck("0;1;2;3;7", "0;1;2", sortTruncateCollector);
    }

#define TEST_DISTINCT_CASE(docCountToAdd, expectedDocCount, minDistinctValueCountToReserve, minDocCountToReserve,      \
                           maxDocCountToReserve, bucketSortValue, attrValue, sortByDocPayload)                         \
                                                                                                                       \
    InnerCaseWithDistinct<int8_t>(docCountToAdd, expectedDocCount, minDistinctValueCountToReserve,                     \
                                  minDocCountToReserve, maxDocCountToReserve, bucketSortValue, attrValue,              \
                                  sortByDocPayload);                                                                   \
    InnerCaseWithDistinct<uint8_t>(docCountToAdd, expectedDocCount, minDistinctValueCountToReserve,                    \
                                   minDocCountToReserve, maxDocCountToReserve, bucketSortValue, attrValue,             \
                                   sortByDocPayload);                                                                  \
    InnerCaseWithDistinct<int16_t>(docCountToAdd, expectedDocCount, minDistinctValueCountToReserve,                    \
                                   minDocCountToReserve, maxDocCountToReserve, bucketSortValue, attrValue,             \
                                   sortByDocPayload);                                                                  \
    InnerCaseWithDistinct<uint16_t>(docCountToAdd, expectedDocCount, minDistinctValueCountToReserve,                   \
                                    minDocCountToReserve, maxDocCountToReserve, bucketSortValue, attrValue,            \
                                    sortByDocPayload);                                                                 \
    InnerCaseWithDistinct<int32_t>(docCountToAdd, expectedDocCount, minDistinctValueCountToReserve,                    \
                                   minDocCountToReserve, maxDocCountToReserve, bucketSortValue, attrValue,             \
                                   sortByDocPayload);                                                                  \
    InnerCaseWithDistinct<uint32_t>(docCountToAdd, expectedDocCount, minDistinctValueCountToReserve,                   \
                                    minDocCountToReserve, maxDocCountToReserve, bucketSortValue, attrValue,            \
                                    sortByDocPayload);                                                                 \
    InnerCaseWithDistinct<int64_t>(docCountToAdd, expectedDocCount, minDistinctValueCountToReserve,                    \
                                   minDocCountToReserve, maxDocCountToReserve, bucketSortValue, attrValue,             \
                                   sortByDocPayload);                                                                  \
    InnerCaseWithDistinct<uint64_t>(docCountToAdd, expectedDocCount, minDistinctValueCountToReserve,                   \
                                    minDocCountToReserve, maxDocCountToReserve, bucketSortValue, attrValue,            \
                                    sortByDocPayload);

    void TestCaseForEnoughAttributeValueInMinDocCountToReserve()
    {
        // minDocCountToReserve is 3, distinct value count is 2, and minDistinctValueCountToReserve is 2
        TEST_DISTINCT_CASE(5, 3, 2, 3, 5, "0;1;2;3;4;5", "1;1;2;2;3;3;", false);
    }

    void TestCaseForEnoughAttributeValueInMinDocCountToReserve_SortByDocPayload()
    {
        // minDocCountToReserve is 3, distinct value count is 2, and minDistinctValueCountToReserve is 2
        mTruncateProfilePtr->mSortParams[0].SetSortPattern(indexlibv2::config::sp_asc);
        TEST_DISTINCT_CASE(5, 3, 2, 3, 5, "", "1;1;2;2;3;3;", true);
    }

    void TestCaseForMinDocCountToReserveEqualToDistinctCountToReserve()
    {
        TEST_DISTINCT_CASE(7, 5, 3, 5, 6, "0;1;2;3;4;5;6;7;8;9", "1;1;2;2;3;3;4;4;5;5", false);
    }

    void TestCaseForMinDocCountToReserveEqualToDistinctCountToReserve_SortByDocPayload()
    {
        mTruncateProfilePtr->mSortParams[0].SetSortPattern(indexlibv2::config::sp_asc);
        TEST_DISTINCT_CASE(7, 5, 3, 5, 6, "", "1;1;2;2;3;3;4;4;5;5", true);
    }

    void TestCaseForMaxDocCountToReserveEqualToDistinctCountToReserve()
    {
        TEST_DISTINCT_CASE(8, 6, 4, 5, 6, "0;1;2;3;4;5;6;7;8;9", "1;1;2;2;3;3;4;4;5;5", false);
    }

    void TestCaseForMaxDocCountToReserveEqualToDistinctCountToReserve_SortByDocPayload()
    {
        mTruncateProfilePtr->mSortParams[0].SetSortPattern(indexlibv2::config::sp_asc);
        TEST_DISTINCT_CASE(8, 7, 4, 5, 7, "", "1;1;2;2;3;3;4;4;5;5", true);
    }

    void TestCaseForMaxDocCountToReserveNotLessThanDistinctCountToReserve()
    {
        TEST_DISTINCT_CASE(8, 5, 3, 4, 6, "0;1;2;3;4;5;6;7;8;9", "1;1;2;2;3;3;4;4;5;5", false);
    }

    void TestCaseForMaxDocCountToReserveNotLessThanDistinctCountToReserve_SortByDocPayload()
    {
        mTruncateProfilePtr->mSortParams[0].SetSortPattern(indexlibv2::config::sp_asc);
        TEST_DISTINCT_CASE(8, 5, 3, 4, 6, "", "1;1;2;2;3;3;4;4;5;5", true);
    }

    void TestCaseForDistinctInBucketBorderlineEnd()
    {
        BucketMapPtr bucketMap = CreateBucketMap("0;1;2;3;6;4;7;5;8;10;9;11");
        DocDistinctorPtr docDistinctor = CreateDocDistinctor<int32_t>("1;1;1;1;1;1;4;4;4;5;5;5", 2);
        DocCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(5, 8, DocFilterProcessorPtr(), docDistinctor, mBucketVecAllocator, bucketMap, 100,
                                      config::TruncateProfilePtr(new config::TruncateProfile()), nullptr));

        TruncateAndCheck("0;1;2;3;4;5;6;7;8;9", "0;1;2;3;6", sortTruncateCollector);
    }

    void TestCaseForDistinctInBucketBorderlineBegin()
    {
        BucketMapPtr bucketMap = CreateBucketMap("0;1;2;3;6;4;7;5;8;10;9;11");
        DocDistinctorPtr docDistinctor = CreateDocDistinctor<int32_t>("1;1;1;1;1;1;1;4;4;5;5;5", 2);
        DocCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(5, 8, DocFilterProcessorPtr(), docDistinctor, mBucketVecAllocator, bucketMap, 100,
                                      config::TruncateProfilePtr(new config::TruncateProfile()), nullptr));
        TruncateAndCheck("0;1;2;3;4;5;6;7;8;9", "0;1;2;3;4;6;7", sortTruncateCollector);
    }

    void TestCaseForDistinctInLastBucket()
    {
        BucketMapPtr bucketMap = CreateBucketMap("0;1;2;3;6;4;7;5;8;10;9;11");
        DocDistinctorPtr docDistinctor = CreateDocDistinctor<int32_t>("1;1;1;1;1;1;1;4;4;5;6;7", 5);
        DocCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(5, 14, DocFilterProcessorPtr(), docDistinctor, mBucketVecAllocator, bucketMap,
                                      100, config::TruncateProfilePtr(new config::TruncateProfile()), nullptr));

        TruncateAndCheck("0;1;2;3;4;5;6;7;8;9;10;11", "0;1;2;3;4;5;6;7;8;9;10;11", sortTruncateCollector);
    }

    void TestCaseForDistinctInFirstBucket()
    {
        BucketMapPtr bucketMap = CreateBucketMap("0;1;2;3;6;4;7;5;8;10;9;11");
        DocDistinctorPtr docDistinctor = CreateDocDistinctor<int32_t>("1;1;2;1;1;1;1;4;4;5;6;7", 1);
        DocCollectorPtr sortTruncateCollector(
            new SortTruncateCollector(2, 4, DocFilterProcessorPtr(), docDistinctor, mBucketVecAllocator, bucketMap, 100,
                                      config::TruncateProfilePtr(new config::TruncateProfile()), nullptr));
        TruncateAndCheck("0;1;2;3;4;5;6;7;8;9;10;11", "0;1", sortTruncateCollector);
    }

private:
    template <typename T>
    void InnerCaseWithDistinct(size_t docCountToAdd, size_t expectedDocCount, uint64_t minDistinctValueCountToReserve,
                               uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                               const string& bucketSortValue, const string& attrValue, bool sortByDocPayload)
    {
        DocCollectorPtr sortTruncateCollector;
        DocDistinctorPtr docDistinctor = CreateDocDistinctor<T>(attrValue, minDistinctValueCountToReserve);
        if (sortByDocPayload) {
            sortTruncateCollector.reset(new SortTruncateCollector(
                minDocCountToReserve, maxDocCountToReserve, DocFilterProcessorPtr(), docDistinctor, mBucketVecAllocator,
                BucketMapPtr(), 100, mTruncateProfilePtr, nullptr));
        } else {
            BucketMapPtr bucketMap = CreateBucketMap(bucketSortValue);
            sortTruncateCollector.reset(new SortTruncateCollector(
                minDocCountToReserve, maxDocCountToReserve, DocFilterProcessorPtr(), docDistinctor, mBucketVecAllocator,
                bucketMap, 100, config::TruncateProfilePtr(new config::TruncateProfile()), nullptr));
        }
        TruncateAndCheck(docCountToAdd, expectedDocCount, sortTruncateCollector);
    }

    void TruncateAndCheck(size_t docCountToAdd, size_t expectedDocCount, const DocCollectorPtr& docCollector)
    {
        string docsToAdd;
        JoinDocIdsWithSemicolon(0, docCountToAdd, docsToAdd);
        string expectDocs;
        JoinDocIdsWithSemicolon(0, expectedDocCount, expectDocs);
        TruncateAndCheck(docsToAdd, expectDocs, docCollector);
    }

    void TruncateAndCheck(const string& docsToAdd, const string& expectDocs, const DocCollectorPtr& docCollector)
    {
        vector<int> docsToAddVec;
        autil::StringUtil::fromString(docsToAdd, docsToAddVec, ";");

        vector<int> expectDocsVec;
        autil::StringUtil::fromString(expectDocs, expectDocsVec, ";");

        PostingIteratorPtr postingIt(new FakePostingIterator(docsToAddVec));
        docCollector->CollectDocIds(DictKeyInfo(0), postingIt);
        const DocCollector::DocIdVector& docIds = *docCollector->GetTruncateDocIds();
        INDEXLIB_TEST_EQUAL(expectDocsVec.size(), docIds.size());
        for (size_t i = 0; i < expectDocsVec.size(); ++i) {
            INDEXLIB_TEST_EQUAL((docid_t)expectDocsVec[i], docIds[i]);
        }
    }

    // attrValue separate by ';', example: "3;4;5"
    template <typename T>
    DocDistinctorPtr CreateDocDistinctor(const string& attrValue, uint64_t minDistinctValueCountToReserve)
    {
        FakeTruncateAttributeReader<T>* fakeAttrReader = new FakeTruncateAttributeReader<T>();
        fakeAttrReader->SetAttrValue(attrValue);
        TruncateAttributeReaderPtr attrReader(fakeAttrReader);
        return DocDistinctorPtr(new DocDistinctorTyped<T>(attrReader, minDistinctValueCountToReserve));
    }

    // sortValue separate by ';', example: "3;4;5"
    BucketMapPtr CreateBucketMap(const string& sortedDocs)
    {
        BucketMapPtr bucketMap(new BucketMap());
        vector<int> sortDocsVec;
        autil::StringUtil::fromString(sortedDocs, sortDocsVec, ";");
        bucketMap->Init(sortDocsVec.size());
        for (size_t i = 0; i < sortDocsVec.size(); ++i) {
            bucketMap->SetSortValue(sortDocsVec[i], i);
        }
        return bucketMap;
    }

    void JoinDocIdsWithSemicolon(uint32_t startDocId, uint32_t endDocId, string& sortDocids)
    {
        char buf[20];
        for (uint32_t i = startDocId; i < endDocId; ++i) {
            sprintf(buf, "%d;", i);
            sortDocids.append(buf);
        }
    }

private:
    BucketVectorAllocatorPtr mBucketVecAllocator;
    config::TruncateProfilePtr mTruncateProfilePtr;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, SortTruncateCollectorTest);

INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForSimple);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForDocPayload);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForMinDocCountToReserveLargerThanPostingDocCount);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest,
                        TestCaseForMinDocCountToReserveLargerThanPostingDocCount_SortByDocPayload);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForReset);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForReset_SortByDocPayload);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForSortBucket);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForSortWithDocPayload);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForGetMinValueDocId);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForGetMinValueDocId_SortByDocPayload);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForResultInOneBucket);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForEnoughAttributeValueInMinDocCountToReserve);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest,
                        TestCaseForEnoughAttributeValueInMinDocCountToReserve_SortByDocPayload);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForMinDocCountToReserveEqualToDistinctCountToReserve);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest,
                        TestCaseForMinDocCountToReserveEqualToDistinctCountToReserve_SortByDocPayload);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForMaxDocCountToReserveEqualToDistinctCountToReserve);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest,
                        TestCaseForMaxDocCountToReserveEqualToDistinctCountToReserve_SortByDocPayload);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForMaxDocCountToReserveNotLessThanDistinctCountToReserve);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest,
                        TestCaseForMaxDocCountToReserveNotLessThanDistinctCountToReserve_SortByDocPayload);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForDistinctInBucketBorderlineEnd);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForDistinctInBucketBorderlineBegin);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForDistinctInLastBucket);
INDEXLIB_UNIT_TEST_CASE(SortTruncateCollectorTest, TestCaseForDistinctInFirstBucket);
} // namespace indexlib::index::legacy
