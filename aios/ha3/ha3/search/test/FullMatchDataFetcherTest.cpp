#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/FullMatchDataFetcher.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/test/SearcherTestHelper.h>

using namespace std;

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

class FullMatchDataFetcherTest : public TESTBASE {
public:
    FullMatchDataFetcherTest();
    ~FullMatchDataFetcherTest();
public:
    void setUp();
    void tearDown();
protected:
    autil::mem_pool::Pool _pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, FullMatchDataFetcherTest);


FullMatchDataFetcherTest::FullMatchDataFetcherTest() {
}

FullMatchDataFetcherTest::~FullMatchDataFetcherTest() {
}

void FullMatchDataFetcherTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void FullMatchDataFetcherTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(FullMatchDataFetcherTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    uint32_t termCount = 3;
    common::Ha3MatchDocAllocator allocator(&_pool);
    FullMatchDataFetcher fetcher;
    ASSERT_TRUE(fetcher.require(&allocator, MATCH_DATA_REF, termCount) != NULL);
    docid_t docId = 0;
    matchdoc::MatchDoc matchDoc = allocator.allocate(0);
    auto fieldGroupPair = allocator.findGroup(common::HA3_MATCHDATA_GROUP);
    ASSERT_TRUE(fieldGroupPair.first != NULL);
    ASSERT_TRUE(fieldGroupPair.second);
    auto matchDataRef = fieldGroupPair.first->findReferenceWithoutType(
            MATCH_DATA_REF);
    ASSERT_TRUE(matchDataRef != NULL);

    set<docid_t> matchDocs;
    matchDocs.insert(0);
    matchDocs.insert(5);
    matchDocs.insert(7);
    matchDocs.insert(9);

    index::FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "a:0[1,2,3]\n"
                                   "b:1[1]\n"
                                   "c:0[1]\n";
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        IE_NAMESPACE(index)::FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    indexPartReaderPtr->setTopK(1);
    MatchDataManager manager;
    QueryExecutorCreator creator(&manager, indexPartReaderPtr.get(), &_pool);

    string queryStr = "a OR b OR c";
    unique_ptr<Query> query(SearcherTestHelper::createQuery(queryStr));

    query->accept(&creator);
    MatchData &data = fetcher._ref->getReference(matchDoc);
    data.setMaxNumTerms(termCount);

    const std::vector<QueryExecutor*> &termExecutors = manager.getExecutors(0);
    for (size_t i = 0; i < termExecutors.size(); ++i) {
        termExecutors[i]->legacySeek(docId);
    }

    fetcher.fillMatchData(termExecutors, matchDoc, matchdoc::MatchDoc());
    const TermMatchData& tmd1 = data.getTermMatchData(0);
    ASSERT_TRUE(tmd1.isMatched());
    ASSERT_EQ(tf_t(3), tmd1.getTermFreq());

    const TermMatchData& tmd2 = data.getTermMatchData(1);
    ASSERT_TRUE(!tmd2.isMatched());

    const TermMatchData& tmd3 = data.getTermMatchData(2);
    ASSERT_TRUE(tmd3.isMatched());
    ASSERT_EQ(tf_t(1), tmd3.getTermFreq());

    allocator.deallocate(matchDoc);
}

TEST_F(FullMatchDataFetcherTest, testMultiQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    // query count 3 layer 4
    uint32_t termCount = 5;
    common::Ha3MatchDocAllocator allocator(&_pool);
    FullMatchDataFetcher fetcher;
    ASSERT_TRUE(fetcher.require(&allocator, MATCH_DATA_REF, termCount) != NULL);
    matchdoc::MatchDoc matchDoc = allocator.allocate(0);
    matchdoc::MatchDoc matchDoc1 = allocator.allocate(1);
    matchdoc::MatchDoc matchDoc2 = allocator.allocate(2);
    matchdoc::MatchDoc matchDoc3 = allocator.allocate(3);

    index::FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "a:0[1,2,3];3[1,2,3]\n"
                                   "b:1[1];3[1]\n"
                                   "c:0[1];2[1]\n";
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        IE_NAMESPACE(index)::FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    indexPartReaderPtr->setTopK(1);
    MatchDataManager manager;
    manager.setQueryCount(3);
    manager._fetcher = &fetcher;
    QueryExecutor *queryExecutor1, *queryExecutor2, *queryExecutor3, *queryExecutor4;
    {
        QueryExecutorCreator creator(&manager, indexPartReaderPtr.get(), &_pool);
        string queryStr = "a OR b OR c";
        unique_ptr<Query> query(SearcherTestHelper::createQuery(queryStr));
        query->accept(&creator);
        queryExecutor1 = creator.stealQuery();
    }
    {
        QueryExecutorCreator creator(&manager, indexPartReaderPtr.get(), &_pool);
        string queryStr = "b";
        unique_ptr<Query> query(SearcherTestHelper::createQuery(queryStr));
        query->accept(&creator);
        queryExecutor2 = creator.stealQuery();
    }
    {
        QueryExecutorCreator creator(&manager, indexPartReaderPtr.get(), &_pool);
        string queryStr = "c";
        unique_ptr<Query> query(SearcherTestHelper::createQuery(queryStr));
        query->accept(&creator);
        queryExecutor3 = creator.stealQuery();
    }
    {
        QueryExecutorCreator creator(&manager, indexPartReaderPtr.get(), &_pool);
        string queryStr = "a OR b OR c";
        unique_ptr<Query> query(SearcherTestHelper::createQuery(queryStr));
        query->accept(&creator);
        queryExecutor4 = creator.stealQuery();
    }

    MatchData &data = fetcher._ref->getReference(matchDoc);
    MatchData &data1 = fetcher._ref->getReference(matchDoc1);
    MatchData &data2 = fetcher._ref->getReference(matchDoc2);
    MatchData &data3 = fetcher._ref->getReference(matchDoc3);
    // layer0
    auto &termExecutors = manager.getExecutors(0);
    for (size_t i = 0; i < termExecutors.size(); ++i) {
        termExecutors[i]->legacySeek(0);
    }
    fetcher.fillMatchData(termExecutors, matchDoc, matchdoc::MatchDoc());
    // layer1
    manager.moveToLayer(1);
    auto &termExecutors1 = manager.getExecutors(1);
    for (size_t i = 0; i < termExecutors1.size(); ++i) {
        termExecutors1[i]->legacySeek(1);
    }
    fetcher.fillMatchData(termExecutors1, matchDoc1, matchdoc::MatchDoc());
    // layer2
    manager.moveToLayer(2);
    auto &termExecutors2 = manager.getExecutors(2);
    for (size_t i = 0; i < termExecutors2.size(); ++i) {
        termExecutors2[i]->legacySeek(2);
    }
    fetcher.fillMatchData(termExecutors2, matchDoc2, matchdoc::MatchDoc());
    // layer3
    manager.moveToLayer(3);
    auto &termExecutors3 = manager.getExecutors(3);
    for (size_t i = 0; i < termExecutors3.size(); ++i) {
        termExecutors3[i]->legacySeek(3);
    }
    fetcher.fillMatchData(termExecutors3, matchDoc3, matchdoc::MatchDoc());

    // check 0
    {
        const TermMatchData& tmd1 = data.getTermMatchData(0);
        ASSERT_TRUE(tmd1.isMatched());
        ASSERT_EQ(tf_t(3), tmd1.getTermFreq());

        const TermMatchData& tmd2 = data.getTermMatchData(1);
        ASSERT_TRUE(!tmd2.isMatched());

        const TermMatchData& tmd3 = data.getTermMatchData(2);
        ASSERT_TRUE(tmd3.isMatched());
        ASSERT_EQ(tf_t(1), tmd3.getTermFreq());

        const TermMatchData& tmd4 = data.getTermMatchData(3);
        ASSERT_TRUE(!tmd4.isMatched());

        const TermMatchData& tmd5 = data.getTermMatchData(4);
        ASSERT_TRUE(!tmd5.isMatched());
    }

    // check 1
    {
        const TermMatchData& tmd1 = data1.getTermMatchData(0);
        ASSERT_TRUE(!tmd1.isMatched());

        const TermMatchData& tmd2 = data1.getTermMatchData(1);
        ASSERT_TRUE(!tmd2.isMatched());

        const TermMatchData& tmd3 = data1.getTermMatchData(2);
        ASSERT_TRUE(!tmd3.isMatched());

        const TermMatchData& tmd4 = data1.getTermMatchData(3);
        ASSERT_TRUE(tmd4.isMatched());
        ASSERT_EQ(tf_t(1), tmd4.getTermFreq());

        const TermMatchData& tmd5 = data1.getTermMatchData(4);
        ASSERT_TRUE(!tmd5.isMatched());
    }

    // check 2
    {
        const TermMatchData& tmd1 = data2.getTermMatchData(0);
        ASSERT_TRUE(!tmd1.isMatched());

        const TermMatchData& tmd2 = data2.getTermMatchData(1);
        ASSERT_TRUE(!tmd2.isMatched());

        const TermMatchData& tmd3 = data2.getTermMatchData(2);
        ASSERT_TRUE(!tmd3.isMatched());

        const TermMatchData& tmd4 = data2.getTermMatchData(3);
        ASSERT_TRUE(!tmd4.isMatched());

        const TermMatchData& tmd5 = data2.getTermMatchData(4);
        ASSERT_TRUE(tmd5.isMatched());
        ASSERT_EQ(tf_t(1), tmd5.getTermFreq());
    }

    // check 3
    {
        const TermMatchData& tmd1 = data3.getTermMatchData(0);
        ASSERT_TRUE(tmd1.isMatched());
        ASSERT_EQ(tf_t(3), tmd1.getTermFreq());

        const TermMatchData& tmd2 = data3.getTermMatchData(1);
        ASSERT_TRUE(tmd2.isMatched());
        ASSERT_EQ(tf_t(1), tmd2.getTermFreq());

        const TermMatchData& tmd3 = data3.getTermMatchData(2);
        ASSERT_TRUE(!tmd3.isMatched());

        const TermMatchData& tmd4 = data3.getTermMatchData(3);
        ASSERT_TRUE(!tmd4.isMatched());

        const TermMatchData& tmd5 = data3.getTermMatchData(4);
        ASSERT_TRUE(!tmd5.isMatched());
    }

    allocator.deallocate(matchDoc);
    allocator.deallocate(matchDoc1);
    allocator.deallocate(matchDoc2);
    allocator.deallocate(matchDoc3);
    POOL_DELETE_CLASS(queryExecutor1);
    POOL_DELETE_CLASS(queryExecutor2);
    POOL_DELETE_CLASS(queryExecutor3);
    POOL_DELETE_CLASS(queryExecutor4);
}

END_HA3_NAMESPACE(search);
