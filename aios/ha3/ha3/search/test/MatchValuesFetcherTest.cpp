#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MatchValuesFetcher.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/test/SearcherTestHelper.h>

using namespace std;

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

class MatchValuesFetcherTest : public TESTBASE {
public:
    MatchValuesFetcherTest();
    ~MatchValuesFetcherTest();
public:
    void setUp();
    void tearDown();
protected:
    autil::mem_pool::Pool _pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, MatchValuesFetcherTest);


MatchValuesFetcherTest::MatchValuesFetcherTest() {
}

MatchValuesFetcherTest::~MatchValuesFetcherTest() {
}

void MatchValuesFetcherTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void MatchValuesFetcherTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(MatchValuesFetcherTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    uint32_t termCount = 3;
    common::Ha3MatchDocAllocator allocator(&_pool);
    MatchValuesFetcher matchValuesFetcher;
    ASSERT_TRUE(matchValuesFetcher.require(&allocator, MATCH_VALUE_REF, termCount) != NULL);
    allocator.allocate();
    auto fieldGroupPair = allocator.findGroup(common::HA3_MATCHVALUE_GROUP);
    ASSERT_TRUE(fieldGroupPair.first != NULL);
    ASSERT_TRUE(fieldGroupPair.second);
    auto matchValuesRef = fieldGroupPair.first->findReferenceWithoutType(
            MATCH_VALUE_REF);
    ASSERT_TRUE(matchValuesRef != NULL);

    index::FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "a:0^100[1,2,3]\n"
                                   "b:1^200[1]\n"
                                   "c:0^300[1]\n";
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        IE_NAMESPACE(index)::FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    indexPartReaderPtr->setTopK(1);
    MatchDataManager manager;
    QueryExecutorCreator creator(&manager, indexPartReaderPtr.get(), &_pool);

    string queryStr = "a OR b OR c";
    unique_ptr<Query> query(SearcherTestHelper::createQuery(queryStr));

    query->accept(&creator);

    const std::vector<QueryExecutor*> &termExecutors = manager.getExecutors(0);
    {
        docid_t docId = 0;
        matchdoc::MatchDoc matchDoc = allocator.allocate(docId);
        Ha3MatchValues &matchValues = matchValuesFetcher._ref->getReference(matchDoc);
        matchValues.setMaxNumTerms(termCount);
        for (size_t i = 0; i < termExecutors.size(); ++i) {
            termExecutors[i]->legacySeek(docId);
        }

        matchValuesFetcher.fillMatchValues(termExecutors, matchDoc);
        matchvalue_t& tmv0 = matchValues.getTermMatchValue(0);
        ASSERT_EQ((uint16_t)100, tmv0.GetUint16());

        matchvalue_t& tmv1 = matchValues.getTermMatchValue(1);
        ASSERT_EQ((uint16_t)0, tmv1.GetUint16());

        matchvalue_t& tmv2 = matchValues.getTermMatchValue(2);
        ASSERT_EQ((uint16_t)300, tmv2.GetUint16());

        allocator.deallocate(matchDoc);
    }

    {
        docid_t docId = 1;
        matchdoc::MatchDoc matchDoc = allocator.allocate(docId);
        Ha3MatchValues &matchValues = matchValuesFetcher._ref->getReference(matchDoc);
        matchValues.setMaxNumTerms(termCount);
        for (size_t i = 0; i < termExecutors.size(); ++i) {
            termExecutors[i]->legacySeek(docId);
        }

        matchValuesFetcher.fillMatchValues(termExecutors, matchDoc);
        matchvalue_t& tmv0 = matchValues.getTermMatchValue(0);
        ASSERT_EQ((uint16_t)0, tmv0.GetUint16());

        matchvalue_t& tmv1 = matchValues.getTermMatchValue(1);
        ASSERT_EQ((uint16_t)200, tmv1.GetUint16());

        matchvalue_t& tmv2 = matchValues.getTermMatchValue(2);
        ASSERT_EQ((uint16_t)0, tmv2.GetUint16());

        allocator.deallocate(matchDoc);
    }
}

TEST_F(MatchValuesFetcherTest, testMultiQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    // query count 3 layer 4
    uint32_t termCount = 5;
    common::Ha3MatchDocAllocator allocator(&_pool);
    MatchValuesFetcher matchValuesFetcher;
    ASSERT_TRUE(matchValuesFetcher.require(&allocator, MATCH_VALUE_REF, termCount) != NULL);
    matchdoc::MatchDoc matchDoc = allocator.allocate(0);
    matchdoc::MatchDoc matchDoc1 = allocator.allocate(1);
    matchdoc::MatchDoc matchDoc2 = allocator.allocate(2);
    matchdoc::MatchDoc matchDoc3 = allocator.allocate(3);

    index::FakeIndex fakeIndex;
    fakeIndex.indexes["default"] = "a:0^100[1,2,3];3^103[1,2,3]\n"
                                   "b:1^201[1];3^203[1]\n"
                                   "c:0^300[1];2^302[1]\n";
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        IE_NAMESPACE(index)::FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    indexPartReaderPtr->setTopK(1);
    MatchDataManager manager;
    manager.setQueryCount(3);
    manager._matchValuesFetcher = &matchValuesFetcher;
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

    // layer0
    {
        auto &termExecutors = manager.getExecutors(0);
        for (size_t i = 0; i < termExecutors.size(); ++i) {
            termExecutors[i]->legacySeek(0);
        }
        Ha3MatchValues &matchValues = matchValuesFetcher._ref->getReference(matchDoc);
        matchValuesFetcher.fillMatchValues(termExecutors, matchDoc);
        matchvalue_t& tmv0 = matchValues.getTermMatchValue(0);
        ASSERT_EQ((uint16_t)100, tmv0.GetUint16());

        matchvalue_t& tmv1 = matchValues.getTermMatchValue(1);
        ASSERT_EQ((uint16_t)0, tmv1.GetUint16());

        matchvalue_t& tmv2 = matchValues.getTermMatchValue(2);
        ASSERT_EQ((uint16_t)300, tmv2.GetUint16());

        matchvalue_t& tmv3 = matchValues.getTermMatchValue(3);
        ASSERT_EQ((uint16_t)0, tmv3.GetUint16());

        matchvalue_t& tmv4 = matchValues.getTermMatchValue(4);
        ASSERT_EQ((uint16_t)0, tmv4.GetUint16());
    }
    
    // layer1
    {
        manager.moveToLayer(1);
        auto &termExecutors1 = manager.getExecutors(1);
        for (size_t i = 0; i < termExecutors1.size(); ++i) {
            termExecutors1[i]->legacySeek(1);
        }
        Ha3MatchValues &matchValues = matchValuesFetcher._ref->getReference(matchDoc1);
        matchValuesFetcher.fillMatchValues(termExecutors1, matchDoc1);
        matchvalue_t& tmv0 = matchValues.getTermMatchValue(0);
        ASSERT_EQ((uint16_t)0, tmv0.GetUint16());

        matchvalue_t& tmv1 = matchValues.getTermMatchValue(1);
        ASSERT_EQ((uint16_t)0, tmv1.GetUint16());

        matchvalue_t& tmv2 = matchValues.getTermMatchValue(2);
        ASSERT_EQ((uint16_t)0, tmv2.GetUint16());

        matchvalue_t& tmv3 = matchValues.getTermMatchValue(3);
        ASSERT_EQ((uint16_t)201, tmv3.GetUint16());

        matchvalue_t& tmv4 = matchValues.getTermMatchValue(4);
        ASSERT_EQ((uint16_t)0, tmv4.GetUint16());
    }
    
    // layer2
    { 
        manager.moveToLayer(2);
        auto &termExecutors2 = manager.getExecutors(2);
        for (size_t i = 0; i < termExecutors2.size(); ++i) {
            termExecutors2[i]->legacySeek(2);
        }
        Ha3MatchValues &matchValues = matchValuesFetcher._ref->getReference(matchDoc2);
        matchValuesFetcher.fillMatchValues(termExecutors2, matchDoc2);
        matchvalue_t& tmv0 = matchValues.getTermMatchValue(0);
        ASSERT_EQ((uint16_t)0, tmv0.GetUint16());

        matchvalue_t& tmv1 = matchValues.getTermMatchValue(1);
        ASSERT_EQ((uint16_t)0, tmv1.GetUint16());

        matchvalue_t& tmv2 = matchValues.getTermMatchValue(2);
        ASSERT_EQ((uint16_t)0, tmv2.GetUint16());

        matchvalue_t& tmv3 = matchValues.getTermMatchValue(3);
        ASSERT_EQ((uint16_t)0, tmv3.GetUint16());

        matchvalue_t& tmv4 = matchValues.getTermMatchValue(4);
        ASSERT_EQ((uint16_t)302, tmv4.GetUint16());
    }
    
    // layer3
    {
        manager.moveToLayer(3);
        auto &termExecutors3 = manager.getExecutors(3);
        for (size_t i = 0; i < termExecutors3.size(); ++i) {
            termExecutors3[i]->legacySeek(3);
        }
        Ha3MatchValues &matchValues = matchValuesFetcher._ref->getReference(matchDoc3);
        matchValuesFetcher.fillMatchValues(termExecutors3, matchDoc3);
        matchvalue_t& tmv0 = matchValues.getTermMatchValue(0);
        ASSERT_EQ((uint16_t)103, tmv0.GetUint16());

        matchvalue_t& tmv1 = matchValues.getTermMatchValue(1);
        ASSERT_EQ((uint16_t)203, tmv1.GetUint16());

        matchvalue_t& tmv2 = matchValues.getTermMatchValue(2);
        ASSERT_EQ((uint16_t)0, tmv2.GetUint16());

        matchvalue_t& tmv3 = matchValues.getTermMatchValue(3);
        ASSERT_EQ((uint16_t)0, tmv3.GetUint16());

        matchvalue_t& tmv4 = matchValues.getTermMatchValue(4);
        ASSERT_EQ((uint16_t)0, tmv4.GetUint16());
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
