#include "indexlib/index/inverted_index/truncate/SortTruncateCollector.h"

#include "indexlib/index/inverted_index/truncate/BucketMap.h"
#include "indexlib/index/inverted_index/truncate/DocCollector.h"
#include "indexlib/index/inverted_index/truncate/DocDistinctor.h"
#include "indexlib/index/inverted_index/truncate/test/FakePostingIterator.h"
#include "indexlib/index/inverted_index/truncate/test/FakeTruncateAttributeReader.h"
#include "unittest/unittest.h"
namespace indexlib::index {

class SortTruncateCollectorTest : public TESTBASE
{
public:
    void setUp() override
    {
        _bucketVecAllocator.reset(new BucketVectorAllocator);
        _truncateProfile.reset(new indexlibv2::config::TruncateProfile());
        config::SortParam sortParam;
        sortParam.mSortField = DOC_PAYLOAD_FIELD_NAME;
        _truncateProfile->sortParams.emplace_back(sortParam);
    }
    void tearDown() override {}

private:
    template <typename T>
    void InnerCaseWithDistinct(size_t docCountToAdd, size_t expectedDocCount, uint64_t minDistinctValueCountToReserve,
                               uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                               const std::string& bucketSortValue, const std::string& attrValue, bool sortByDocPayload);

    void TruncateAndCheck(size_t docCountToAdd, size_t expectedDocCount,
                          const std::shared_ptr<DocCollector>& docCollector);

    void TruncateAndCheck(const std::string& docsToAdd, const std::string& expectDocs,
                          const std::shared_ptr<DocCollector>& docCollector);

    // attrValue separate by ';', example: "3;4;5"
    template <typename T>
    std::shared_ptr<DocDistinctor> CreateDocDistinctor(const std::string& attrValue,
                                                       uint64_t minDistinctValueCountToReserve);

    // sortValue separate by ';', example: "3;4;5"
    std::shared_ptr<BucketMap> CreateBucketMap(const std::string& sortedDocs);

    void JoinDocIdsWithSemicolon(uint32_t startDocId, uint32_t endDocId, std::string& sortDocids);
    void CheckDocIdVector(const std::vector<docid_t>& expectDocsVec, const std::vector<docid_t>& truncatedDocIds) const;

private:
    std::shared_ptr<BucketVectorAllocator> _bucketVecAllocator;
    std::shared_ptr<indexlibv2::config::TruncateProfile> _truncateProfile;
};

void SortTruncateCollectorTest::CheckDocIdVector(const std::vector<docid_t>& expectDocsVec,
                                                 const std::vector<docid_t>& truncatedDocIds) const
{
    ASSERT_EQ(expectDocsVec.size(), truncatedDocIds.size());
    for (size_t i = 0; i < expectDocsVec.size(); ++i) {
        ASSERT_EQ((docid_t)expectDocsVec[i], truncatedDocIds[i]);
    }
}
template <typename T>
void SortTruncateCollectorTest::InnerCaseWithDistinct(size_t docCountToAdd, size_t expectedDocCount,
                                                      uint64_t minDistinctValueCountToReserve,
                                                      uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                                                      const std::string& bucketSortValue, const std::string& attrValue,
                                                      bool sortByDocPayload)
{
    std::shared_ptr<DocCollector> sortTruncateCollector;
    std::shared_ptr<DocDistinctor> docDistinctor = CreateDocDistinctor<T>(attrValue, minDistinctValueCountToReserve);
    if (sortByDocPayload) {
        sortTruncateCollector.reset(new SortTruncateCollector(minDocCountToReserve, maxDocCountToReserve, nullptr,
                                                              docDistinctor, _bucketVecAllocator, nullptr, 100,
                                                              _truncateProfile, nullptr));
    } else {
        std::shared_ptr<BucketMap> bucketMap = CreateBucketMap(bucketSortValue);
        auto profile = std::make_shared<indexlibv2::config::TruncateProfile>();
        sortTruncateCollector.reset(new SortTruncateCollector(minDocCountToReserve, maxDocCountToReserve,
                                                              /*docFilterProcessor*/ nullptr, docDistinctor,
                                                              _bucketVecAllocator, bucketMap, 100, profile, nullptr));
    }
    TruncateAndCheck(docCountToAdd, expectedDocCount, sortTruncateCollector);
}

void SortTruncateCollectorTest::TruncateAndCheck(size_t docCountToAdd, size_t expectedDocCount,
                                                 const std::shared_ptr<DocCollector>& docCollector)
{
    std::string docsToAdd;
    JoinDocIdsWithSemicolon(0, docCountToAdd, docsToAdd);
    std::string expectDocs;
    JoinDocIdsWithSemicolon(0, expectedDocCount, expectDocs);
    TruncateAndCheck(docsToAdd, expectDocs, docCollector);
}

void SortTruncateCollectorTest::TruncateAndCheck(const std::string& docsToAdd, const std::string& expectDocs,
                                                 const std::shared_ptr<DocCollector>& docCollector)
{
    std::vector<int> docsToAddVec;
    autil::StringUtil::fromString(docsToAdd, docsToAddVec, ";");

    std::vector<int> expectDocsVec;
    autil::StringUtil::fromString(expectDocs, expectDocsVec, ";");

    std::shared_ptr<PostingIterator> postingIt(new FakePostingIterator(docsToAddVec));
    docCollector->CollectDocIds(index::DictKeyInfo(0), postingIt, /*docFreq*/ -1);
    const DocCollector::DocIdVector& docIds = *docCollector->GetTruncateDocIds();
    ASSERT_EQ(expectDocsVec.size(), docIds.size());
    for (size_t i = 0; i < expectDocsVec.size(); ++i) {
        ASSERT_EQ((docid_t)expectDocsVec[i], docIds[i]);
    }
}

// attrValue separate by ';', example: "3;4;5"
template <typename T>
std::shared_ptr<DocDistinctor> SortTruncateCollectorTest::CreateDocDistinctor(const std::string& attrValue,
                                                                              uint64_t minDistinctValueCountToReserve)
{
    FakeTruncateAttributeReader<T>* fakeAttrReader =
        new FakeTruncateAttributeReader<T>(/*docMapper*/ nullptr, /*attrConfig*/ nullptr);
    fakeAttrReader->SetAttrValue(attrValue);
    std::shared_ptr<TruncateAttributeReader> attrReader(fakeAttrReader);
    return std::make_shared<DocDistinctorTyped<T>>(attrReader, minDistinctValueCountToReserve);
}

// sortValue separate by ';', example: "3;4;5"
std::shared_ptr<BucketMap> SortTruncateCollectorTest::CreateBucketMap(const std::string& sortedDocs)
{
    std::shared_ptr<BucketMap> bucketMap(new BucketMap("ut_name", "ut_type"));
    std::vector<int> sortDocsVec;
    autil::StringUtil::fromString(sortedDocs, sortDocsVec, ";");
    bucketMap->Init(sortDocsVec.size());
    for (size_t i = 0; i < sortDocsVec.size(); ++i) {
        bucketMap->SetSortValue(sortDocsVec[i], i);
    }
    return bucketMap;
}

void SortTruncateCollectorTest::JoinDocIdsWithSemicolon(uint32_t startDocId, uint32_t endDocId, std::string& sortDocids)
{
    char buf[20];
    for (uint32_t i = startDocId; i < endDocId; ++i) {
        sprintf(buf, "%d;", i);
        sortDocids.append(buf);
    }
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

TEST_F(SortTruncateCollectorTest, TestCaseForSimple)
{
    // prepare data
    std::string sortedDocs = "2;1;0";
    std::shared_ptr<BucketMap> bucketMap = CreateBucketMap(sortedDocs);
    auto profile = std::make_shared<indexlibv2::config::TruncateProfile>();
    std::shared_ptr<DocCollector> sortTruncateCollector(
        new SortTruncateCollector(2, 2, /*docFilterProcessor*/ nullptr, /*docDistinctor*/ nullptr, _bucketVecAllocator,
                                  bucketMap, 100, profile, nullptr));

    std::string docsToAdd = "0;1;2";
    std::vector<docid_t> docsToAddVec;
    autil::StringUtil::fromString(docsToAdd, docsToAddVec, ";");

    // truncate posting
    std::shared_ptr<PostingIterator> postingIt(new FakePostingIterator(docsToAddVec));
    sortTruncateCollector->CollectDocIds(index::DictKeyInfo(0), postingIt, /*docFreq*/ -1);

    // check truncated posting result
    std::string expectedDocs = "1;2";
    std::vector<int> expectedDocsVec;
    autil::StringUtil::fromString(expectedDocs, expectedDocsVec, ";");
    const DocCollector::DocIdVector& docIds = *sortTruncateCollector->GetTruncateDocIds();
    CheckDocIdVector(expectedDocsVec, docIds);
}

TEST_F(SortTruncateCollectorTest, TestCaseForDocPayload)
{
    // prepare data
    std::shared_ptr<DocCollector> sortTruncateCollector(new SortTruncateCollector(
        2, 2, nullptr, nullptr, _bucketVecAllocator, nullptr, 100, _truncateProfile, nullptr));
    TruncateAndCheck("0;1;2", "1;2", sortTruncateCollector);
}

TEST_F(SortTruncateCollectorTest, TestCaseForMinDocCountToReserveLargerThanPostingDocCount)
{
    std::shared_ptr<BucketMap> bucketMap = CreateBucketMap("3;1;2;0");
    auto profile = std::make_shared<indexlibv2::config::TruncateProfile>();
    std::shared_ptr<DocCollector> sortTruncateCollector(
        new SortTruncateCollector(5, 5, /*docFilterProcessor*/ nullptr, /*docDistinctor*/ nullptr, _bucketVecAllocator,
                                  bucketMap, 100, profile, nullptr));
    TruncateAndCheck("0;1;2;3", "0;1;2;3", sortTruncateCollector);
}

TEST_F(SortTruncateCollectorTest, TestCaseForMinDocCountToReserveLargerThanPostingDocCount_SortByDocPayload)
{
    // prepare data
    std::shared_ptr<DocCollector> sortTruncateCollector(new SortTruncateCollector(
        5, 5, nullptr, nullptr, _bucketVecAllocator, nullptr, 100, _truncateProfile, nullptr));
    TruncateAndCheck("0;1;2;3", "0;1;2;3", sortTruncateCollector);
}

TEST_F(SortTruncateCollectorTest, TestCaseForReset)
{
    std::shared_ptr<BucketMap> bucketMap = CreateBucketMap("0;1;2;3;4;5;6;7;8;9");
    auto profile = std::make_shared<indexlibv2::config::TruncateProfile>();
    std::shared_ptr<DocCollector> sortTruncateCollector(
        new SortTruncateCollector(4, 4, /*docFilterProcessor*/ nullptr, /*docDistinctor*/ nullptr, _bucketVecAllocator,
                                  bucketMap, 100, profile, nullptr));
    TruncateAndCheck("0;1;2", "0;1;2", sortTruncateCollector);
    sortTruncateCollector->Reset();

    // after reset, sortPostingTruncate can be reused.
    TruncateAndCheck("3;4;6;7;8", "3;4;6;7", sortTruncateCollector);
}

TEST_F(SortTruncateCollectorTest, TestCaseForReset_SortByDocPayload)
{
    std::shared_ptr<DocCollector> sortTruncateCollector(new SortTruncateCollector(
        4, 4, nullptr, nullptr, _bucketVecAllocator, nullptr, 100, _truncateProfile, nullptr));
    TruncateAndCheck("0;1;2", "0;1;2", sortTruncateCollector);
    sortTruncateCollector->Reset();

    // after reset, sortPostingTruncate can be reused.
    TruncateAndCheck("3;4;6;7;8", "4;6;7;8", sortTruncateCollector);
}

TEST_F(SortTruncateCollectorTest, TestCaseForSortBucket)
{
    std::shared_ptr<BucketMap> bucketMap = CreateBucketMap("5;4;3;2;1;0");
    std::vector<docid_t> docIds;
    docIds.push_back(1);
    docIds.push_back(4);
    docIds.push_back(3);
    SortTruncateCollector::SortBucket(docIds, bucketMap);
    ASSERT_EQ((docid_t)4, docIds[0]);
    ASSERT_EQ((docid_t)3, docIds[1]);
    ASSERT_EQ((docid_t)1, docIds[2]);
}

TEST_F(SortTruncateCollectorTest, TestCaseForSortWithDocPayload)
{
    std::shared_ptr<SortTruncateCollector> sortTruncateCollector(new SortTruncateCollector(
        6, 6, nullptr, nullptr, _bucketVecAllocator, nullptr, 100, _truncateProfile, nullptr));
    std::vector<SortTruncateCollector::Doc> docInfos;
    std::vector<int> payloads {7, 9, 8, 6, 5, 4, 2, 3, 1, 0};
    for (int i = 0; i < payloads.size(); i++) {
        SortTruncateCollector::Doc doc;
        doc.docId = payloads[i];
        doc.payload = payloads[i];
        docInfos.push_back(doc);
    }
    sortTruncateCollector->SortWithDocPayload(docInfos.begin(), docInfos.end(), 0, 6);

    // check min value docid
    ASSERT_EQ((docid_t)4, docInfos[5].docId);
    auto cmp = [](const SortTruncateCollector::Doc& doc1, const SortTruncateCollector::Doc& doc2) {
        return doc1.docId > doc2.docId;
    };
    sort(docInfos.begin(), docInfos.begin() + 6, cmp);
    for (int i = 9; i >= 4; i--) {
        ASSERT_EQ((docid_t)i, docInfos[9 - i].docId);
    }
}

TEST_F(SortTruncateCollectorTest, TestCaseForGetMinValueDocId)
{
    std::shared_ptr<BucketMap> bucketMap = CreateBucketMap("0;1;2;3;4;5;6;7;8;9");
    auto profile = std::make_shared<indexlibv2::config::TruncateProfile>();
    std::shared_ptr<DocCollector> sortTruncateCollector(
        new SortTruncateCollector(4, 4, /*docFilterProcessor*/ nullptr, /*docDistinctor*/ nullptr, _bucketVecAllocator,
                                  bucketMap, 100, profile, nullptr));
    TruncateAndCheck("0;1;3;5;9", "0;1;3;5", sortTruncateCollector);
    ASSERT_EQ((docid_t)5, sortTruncateCollector->GetMinValueDocId());
}

TEST_F(SortTruncateCollectorTest, TestCaseForGetMinValueDocId_SortByDocPayload)
{
    std::shared_ptr<DocCollector> sortTruncateCollector(new SortTruncateCollector(
        4, 4, nullptr, nullptr, _bucketVecAllocator, nullptr, 100, _truncateProfile, nullptr));
    TruncateAndCheck("0;1;3;5;9", "1;3;5;9", sortTruncateCollector);
    ASSERT_EQ((docid_t)1, sortTruncateCollector->GetMinValueDocId());
}

TEST_F(SortTruncateCollectorTest, TestCaseForResultInOneBucket)
{
    std::string sortedDocs;
    JoinDocIdsWithSemicolon(0, 100, sortedDocs);

    std::shared_ptr<BucketMap> bucketMap = CreateBucketMap(sortedDocs);
    auto profile = std::make_shared<indexlibv2::config::TruncateProfile>();
    std::shared_ptr<DocCollector> sortTruncateCollector(
        new SortTruncateCollector(3, 3, /*docFilterProcessor*/ nullptr, /*docDistinctor*/ nullptr, _bucketVecAllocator,
                                  bucketMap, 100, profile, nullptr));
    TruncateAndCheck("0;1;2;3;7", "0;1;2", sortTruncateCollector);
}
TEST_F(SortTruncateCollectorTest, TestCaseForEnoughAttributeValueInMinDocCountToReserve)
{
    // minDocCountToReserve is 3, distinct value count is 2, and minDistinctValueCountToReserve is 2
    TEST_DISTINCT_CASE(5, 3, 2, 3, 5, "0;1;2;3;4;5", "1;1;2;2;3;3;", false);
}
TEST_F(SortTruncateCollectorTest, TestCaseForEnoughAttributeValueInMinDocCountToReserve_SortByDocPayload)
{
    // minDocCountToReserve is 3, distinct value count is 2, and minDistinctValueCountToReserve is 2
    _truncateProfile->sortParams[0].SetSortPattern(indexlibv2::config::sp_asc);
    TEST_DISTINCT_CASE(5, 3, 2, 3, 5, "", "1;1;2;2;3;3;", true);
}
TEST_F(SortTruncateCollectorTest, TestCaseForMinDocCountToReserveEqualToDistinctCountToReserve)
{
    TEST_DISTINCT_CASE(7, 5, 3, 5, 6, "0;1;2;3;4;5;6;7;8;9", "1;1;2;2;3;3;4;4;5;5", false);
}
TEST_F(SortTruncateCollectorTest, TestCaseForMinDocCountToReserveEqualToDistinctCountToReserve_SortByDocPayload)
{
    _truncateProfile->sortParams[0].SetSortPattern(indexlibv2::config::sp_asc);
    TEST_DISTINCT_CASE(7, 5, 3, 5, 6, "", "1;1;2;2;3;3;4;4;5;5", true);
}
TEST_F(SortTruncateCollectorTest, TestCaseForMaxDocCountToReserveEqualToDistinctCountToReserve)
{
    TEST_DISTINCT_CASE(8, 6, 4, 5, 6, "0;1;2;3;4;5;6;7;8;9", "1;1;2;2;3;3;4;4;5;5", false);
}
TEST_F(SortTruncateCollectorTest, TestCaseForMaxDocCountToReserveEqualToDistinctCountToReserve_SortByDocPayload)
{
    _truncateProfile->sortParams[0].SetSortPattern(indexlibv2::config::sp_asc);
    TEST_DISTINCT_CASE(8, 7, 4, 5, 7, "", "1;1;2;2;3;3;4;4;5;5", true);
}
TEST_F(SortTruncateCollectorTest, TestCaseForMaxDocCountToReserveNotLessThanDistinctCountToReserve)
{
    TEST_DISTINCT_CASE(8, 5, 3, 4, 6, "0;1;2;3;4;5;6;7;8;9", "1;1;2;2;3;3;4;4;5;5", false);
}
TEST_F(SortTruncateCollectorTest, TestCaseForMaxDocCountToReserveNotLessThanDistinctCountToReserve_SortByDocPayload)
{
    _truncateProfile->sortParams[0].SetSortPattern(indexlibv2::config::sp_asc);
    TEST_DISTINCT_CASE(8, 5, 3, 4, 6, "", "1;1;2;2;3;3;4;4;5;5", true);
}
TEST_F(SortTruncateCollectorTest, TestCaseForDistinctInBucketBorderlineEnd)
{
    std::shared_ptr<BucketMap> bucketMap = CreateBucketMap("0;1;2;3;6;4;7;5;8;10;9;11");
    std::shared_ptr<DocDistinctor> docDistinctor = CreateDocDistinctor<int32_t>("1;1;1;1;1;1;4;4;4;5;5;5", 2);
    auto profile = std::make_shared<indexlibv2::config::TruncateProfile>();
    std::shared_ptr<DocCollector> sortTruncateCollector(new SortTruncateCollector(
        5, 8, /*docFilterProcessor*/ nullptr, docDistinctor, _bucketVecAllocator, bucketMap, 100, profile, nullptr));

    TruncateAndCheck("0;1;2;3;4;5;6;7;8;9", "0;1;2;3;6", sortTruncateCollector);
}
TEST_F(SortTruncateCollectorTest, TestCaseForDistinctInBucketBorderlineBegin)
{
    std::shared_ptr<BucketMap> bucketMap = CreateBucketMap("0;1;2;3;6;4;7;5;8;10;9;11");
    std::shared_ptr<DocDistinctor> docDistinctor = CreateDocDistinctor<int32_t>("1;1;1;1;1;1;1;4;4;5;5;5", 2);
    auto profile = std::make_shared<indexlibv2::config::TruncateProfile>();
    std::shared_ptr<DocCollector> sortTruncateCollector(new SortTruncateCollector(
        5, 8, /*docFilterProcessor*/ nullptr, docDistinctor, _bucketVecAllocator, bucketMap, 100, profile, nullptr));
    TruncateAndCheck("0;1;2;3;4;5;6;7;8;9", "0;1;2;3;4;6;7", sortTruncateCollector);
}
TEST_F(SortTruncateCollectorTest, TestCaseForDistinctInLastBucket)
{
    std::shared_ptr<BucketMap> bucketMap = CreateBucketMap("0;1;2;3;6;4;7;5;8;10;9;11");
    std::shared_ptr<DocDistinctor> docDistinctor = CreateDocDistinctor<int32_t>("1;1;1;1;1;1;1;4;4;5;6;7", 5);
    auto profile = std::make_shared<indexlibv2::config::TruncateProfile>();
    std::shared_ptr<DocCollector> sortTruncateCollector(new SortTruncateCollector(
        5, 14, /*docFilterProcessor*/ nullptr, docDistinctor, _bucketVecAllocator, bucketMap, 100, profile, nullptr));

    TruncateAndCheck("0;1;2;3;4;5;6;7;8;9;10;11", "0;1;2;3;4;5;6;7;8;9;10;11", sortTruncateCollector);
}
TEST_F(SortTruncateCollectorTest, TestCaseForDistinctInFirstBucket)
{
    auto profile = std::make_shared<indexlibv2::config::TruncateProfile>();
    std::shared_ptr<BucketMap> bucketMap = CreateBucketMap("0;1;2;3;6;4;7;5;8;10;9;11");
    std::shared_ptr<DocDistinctor> docDistinctor = CreateDocDistinctor<int32_t>("1;1;2;1;1;1;1;4;4;5;6;7", 1);
    std::shared_ptr<DocCollector> sortTruncateCollector(new SortTruncateCollector(
        2, 4, /*docFilterProcessor*/ nullptr, docDistinctor, _bucketVecAllocator, bucketMap, 100, profile, nullptr));
    TruncateAndCheck("0;1;2;3;4;5;6;7;8;9;10;11", "0;1", sortTruncateCollector);
}
} // namespace indexlib::index
