#include "indexlib/index/inverted_index/truncate/NoSortTruncateCollector.h"

#include "indexlib/index/inverted_index/truncate/test/FakePostingIterator.h"
#include "indexlib/index/inverted_index/truncate/test/FakeTruncateAttributeReader.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class NoSortTruncateCollectorTest : public TESTBASE
{
public:
    void setUp() override { _bucketVecAllocator.reset(new BucketVectorAllocator); }
    void tearDown() override {}

private:
    void InnerTestForDistinct(size_t docCountToAdd, size_t expectedDocCount, docid_t expectedLastDocId,
                              uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                              const std::shared_ptr<DocDistinctor>& docDistinctor);

    void TruncateAndCheck(size_t docCountToAdd, size_t expectedDocCount, docid_t expectedLastDocId,
                          const std::shared_ptr<DocCollector>& docCollector);

    std::shared_ptr<DocDistinctor> CreateDocDistinctor(std::string attrValue, uint64_t minDistinctValueCountToReserve);

private:
    std::shared_ptr<BucketVectorAllocator> _bucketVecAllocator;
};

std::shared_ptr<DocDistinctor> NoSortTruncateCollectorTest::CreateDocDistinctor(std::string attrValue,
                                                                                uint64_t minDistinctValueCountToReserve)
{
    std::shared_ptr<indexlibv2::index::DocMapper> docMapper;
    auto attrConfig = std::make_shared<indexlibv2::index::AttributeConfig>();
    auto pAttrReader = std::make_shared<FakeTruncateAttributeReader<int32_t>>(docMapper, attrConfig);
    pAttrReader->SetAttrValue(attrValue);
    return std::make_shared<DocDistinctorTyped<int32_t>>(pAttrReader, minDistinctValueCountToReserve);
}

void NoSortTruncateCollectorTest::InnerTestForDistinct(size_t docCountToAdd, size_t expectedDocCount,
                                                       docid_t expectedLastDocId, uint64_t minDocCountToReserve,
                                                       uint64_t maxDocCountToReserve,
                                                       const std::shared_ptr<DocDistinctor>& docDistinctor)
{
    std::shared_ptr<IDocFilterProcessor> docFilterProcessor;
    auto docCollector = std::make_shared<NoSortTruncateCollector>(
        minDocCountToReserve, maxDocCountToReserve, docFilterProcessor, docDistinctor, _bucketVecAllocator);
    TruncateAndCheck(docCountToAdd, expectedDocCount, expectedLastDocId, docCollector);
}

void NoSortTruncateCollectorTest::TruncateAndCheck(size_t docCountToAdd, size_t expectedDocCount,
                                                   docid_t expectedLastDocId,
                                                   const std::shared_ptr<DocCollector>& docCollector)
{
    std::vector<docid_t> addDocIds;
    for (size_t i = 0; i < docCountToAdd; ++i) {
        addDocIds.push_back((docid_t)i);
    }

    auto postingIt = std::make_shared<FakePostingIterator>(addDocIds);
    docCollector->CollectDocIds(index::DictKeyInfo(0), postingIt, -1);
    auto docIds = *(docCollector->GetTruncateDocIds());
    ASSERT_EQ(expectedDocCount, docIds.size());
    for (size_t i = 0; i < expectedDocCount; ++i) {
        ASSERT_EQ((docid_t)i, docIds[i]);
    }
    ASSERT_EQ(expectedLastDocId, docCollector->GetMinValueDocId());
}

TEST_F(NoSortTruncateCollectorTest, TestCaseForNoDistinct)
{
    static uint32_t EXPECTED_DOC_COUNT = 3;
    std::shared_ptr<IDocFilterProcessor> docFilterProcessor;
    std::shared_ptr<DocDistinctor> docDistinctor;
    auto docCollector = std::make_shared<NoSortTruncateCollector>(
        EXPECTED_DOC_COUNT, EXPECTED_DOC_COUNT, docFilterProcessor, docDistinctor, _bucketVecAllocator);

    // add docs(docid is 0, 1, ..., 9) and truncate
    std::vector<docid_t> addDocIds;
    for (size_t i = 0; i < 10; ++i) {
        addDocIds.push_back(i);
    }
    auto postingIt = std::make_shared<FakePostingIterator>(addDocIds);
    docCollector->CollectDocIds(index::DictKeyInfo(0), postingIt, -1);

    // check truncated result(expected truncated docid is 0, 1, 2)
    auto docIds = *(docCollector->GetTruncateDocIds());
    ASSERT_EQ(EXPECTED_DOC_COUNT, docIds.size());
    for (size_t i = 0; i < EXPECTED_DOC_COUNT; ++i) {
        ASSERT_EQ((docid_t)i, docIds[i]);
    }
    ASSERT_EQ(2, docCollector->GetMinValueDocId());
}

TEST_F(NoSortTruncateCollectorTest, TestCaseForDistinctSmallerThanDocLimit)
{
    // select first 5 docs, truncate 3~4 docs
    // truncate 3 docs to satisfy 2 distinct value
    std::string attrValue = "1;2;2;2;3;3;4;4;5;5";
    auto docDistict = CreateDocDistinctor(attrValue, 2);
    InnerTestForDistinct(5, 3, 2, 3, 4, docDistict);
}

TEST_F(NoSortTruncateCollectorTest, TestCaseForDistinctEqualToDocLimit)
{
    // select first 10 docs, truncate 5~7 docs
    // truncate 5 docs to satisfy 3 distinct value
    std::string attrValue = "1;2;2;2;3;3;4;4;5;5";
    auto docDistict = CreateDocDistinctor(attrValue, 3);
    InnerTestForDistinct(10, 5, 4, 5, 7, docDistict);
}

TEST_F(NoSortTruncateCollectorTest, TestCaseForDistinctLargerThanDocLimit)
{
    // select first 8 docs, truncate 3~6 docs
    // truncate 5 docs to satisfy 3 distinct value
    std::string attrValue = "1;2;2;2;3;3;4;4;5;5";
    auto docDistict = CreateDocDistinctor(attrValue, 3);
    InnerTestForDistinct(8, 5, 4, 3, 6, docDistict);
}

TEST_F(NoSortTruncateCollectorTest, TestCaseForDistinctEqualToMaxDocLimit)
{
    // select first 8 docs, truncate 4~5 docs
    // truncate 5 docs to satisfy 4 distinct value
    std::string attrValue = "1;2;2;2;3;3;4;4;5;5";
    auto docDistict = CreateDocDistinctor(attrValue, 4);
    InnerTestForDistinct(8, 5, 4, 4, 5, docDistict);
}

} // namespace indexlib::index
