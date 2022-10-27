#include <unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/rank/MatchData.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/test/test.h>
#include <iostream>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3/search/test/QueryExecutorConstructor.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/common/Query.h>
#include <ha3/common/Term.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/AndQuery.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3/search/WeakAndQueryExecutor.h>
#include <ha3/search/test/SearcherTestHelper.h>
#include <ha3/search/test/QueryExecutorTestHelper.h>


using namespace std;
using namespace testing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);

BEGIN_HA3_NAMESPACE(search);

class WeakAndQueryExecutorTest : public TESTBASE {
public:
    WeakAndQueryExecutorTest();
    ~WeakAndQueryExecutorTest();
public:
    void setUp();
    void tearDown();
protected:
    void createIndexPartitionReaderWrapper(
            const std::string &indexStr);
    std::vector<QueryExecutorEntry> initializeHeap(
            std::vector<docid_t> &docids);
    inline bool heapEq(
            std::vector<QueryExecutorEntry> &heap,
            std::vector<docid_t> &expect);

protected:
    rank::MatchData *_matchData;
    QueryExecutor *_queryExecutor;
    autil::mem_pool::Pool *_pool;
    IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, WeakAndQueryExecutorTest);

WeakAndQueryExecutorTest::WeakAndQueryExecutorTest() { 
    _queryExecutor = NULL;
    _matchData = NULL;
    _pool = new autil::mem_pool::Pool(1024);
}

WeakAndQueryExecutorTest::~WeakAndQueryExecutorTest() {
    delete _pool;
}

void WeakAndQueryExecutorTest::setUp() { 
    _matchData = NULL;
}

void WeakAndQueryExecutorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    if (NULL != _matchData) {
        _matchData->~MatchData();//release the 'TermMatchData'
        free(_matchData);
    }
    _matchData = NULL;
    if (_queryExecutor) {
        POOL_DELETE_CLASS(_queryExecutor);
    }
}

TEST_F(WeakAndQueryExecutorTest, testOrQueryTwoTermQuery1) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];4[1];\n";
    string two = "two:6[1];8[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareOrQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());
    
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)4, _queryExecutor->legacySeek(3));
    ASSERT_EQ((docid_t)4, _queryExecutor->legacySeek(4));
    ASSERT_EQ((docid_t)6, _queryExecutor->legacySeek(5));
    ASSERT_EQ((docid_t)8, _queryExecutor->legacySeek(8));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(9));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(9));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(END_DOCID));
}

TEST_F(WeakAndQueryExecutorTest, testOrQueryTwoTermQuery2) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];3[1];5[1];\n";
    string two = "two:2[1];4[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareWeakAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    ASSERT_EQ((docid_t)4, _queryExecutor->legacySeek(4));
    ASSERT_EQ((docid_t)5, _queryExecutor->legacySeek(5));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(6));
}

TEST_F(WeakAndQueryExecutorTest, testOrQueryTwoTermQuery3) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];3[1];\n";
    string two = "two:2[1];3[1];4[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareWeakAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(1));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    ASSERT_EQ((docid_t)4, _queryExecutor->legacySeek(4));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(5));
}

TEST_F(WeakAndQueryExecutorTest, testOrQueryTwoTermQuery4) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];9[1];\n";
    string two = "two:2[1];8[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareWeakAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)8, _queryExecutor->legacySeek(7));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(10));
    ASSERT_EQ(END_DOCID, _queryExecutor->getDocId());
}

TEST_F(WeakAndQueryExecutorTest, testOrQueryTwoTermQueryWithOneEmptyQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];3[1];\n";
    string two = "two:2[1];3[1];4[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareWeakAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "no_such_term", NULL);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(5));
}


TEST_F(WeakAndQueryExecutorTest, testOrQueryTwoTermQueryWithTwoEmptyQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];3[1];\n";
    string two = "two:2[1];3[1];4[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareWeakAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", 
            "no_such_term_1", "no_such_term_2", NULL);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());
    ASSERT_TRUE(_queryExecutor->isEmpty());
}

TEST_F(WeakAndQueryExecutorTest, testOrQueryThreeTermQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];3[1];6[1];7[1];\n";
    string two = "two:2[1];3[1];5[1];\n";
    string three = "three:2[1];3[1];6[1];8[1];\n";
    createIndexPartitionReaderWrapper(one + two + three);
    _queryExecutor = QueryExecutorConstructor::prepareWeakAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", "three");
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    ASSERT_EQ((docid_t)5, _queryExecutor->legacySeek(4));
    ASSERT_EQ((docid_t)6, _queryExecutor->legacySeek(6));
    ASSERT_EQ((docid_t)7, _queryExecutor->legacySeek(7));
    ASSERT_EQ((docid_t)7, _queryExecutor->legacySeek(7));
    ASSERT_EQ((docid_t)8, _queryExecutor->legacySeek(8));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(9));
}

TEST_F(WeakAndQueryExecutorTest, testSubTermQuery) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term:0;1;11;13;17;\n";
    fakeIndex.indexes["index_name"] = "term:1;3;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,10,15,20");
    IndexPartitionReaderWrapperPtr indexPartReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    
    QueryPtr queryPtr(SearcherTestHelper::createQuery(
                    "sub_index_name:(term||term2)^1 OR index_name:term||term3"));

    QueryExecutor *queryExecutor = QueryExecutorConstructor::createQueryExecutor(
                    _pool, indexPartReaderWrapper.get(), queryPtr.get());
    ASSERT_TRUE(queryExecutor->hasSubDocExecutor());
    ASSERT_EQ(docid_t(0), queryExecutor->legacySeek(0));
    ASSERT_TRUE(!queryExecutor->isMainDocHit(0));
    ASSERT_EQ(docid_t(0), queryExecutor->legacySeekSub(0, 0, 3));
    ASSERT_EQ(docid_t(1), queryExecutor->legacySeekSub(0, 1, 3));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeekSub(0, 2, 3));
    ASSERT_EQ(docid_t(1), queryExecutor->legacySeek(1));
    ASSERT_TRUE(queryExecutor->isMainDocHit(1));
    ASSERT_EQ(docid_t(3), queryExecutor->legacySeekSub(1, 3, 5));
    ASSERT_EQ(docid_t(4), queryExecutor->legacySeekSub(1, 4, 5));
    ASSERT_EQ(docid_t(3), queryExecutor->legacySeek(2));
    ASSERT_TRUE(queryExecutor->isMainDocHit(3));
    ASSERT_EQ(docid_t(10), queryExecutor->legacySeekSub(3, 10, 15));
    ASSERT_EQ(docid_t(11), queryExecutor->legacySeekSub(3, 11, 15));
    ASSERT_EQ(docid_t(12), queryExecutor->legacySeekSub(3, 12, 15));
    ASSERT_EQ(docid_t(13), queryExecutor->legacySeekSub(3, 13, 15));
    ASSERT_EQ(docid_t(14), queryExecutor->legacySeekSub(3, 14, 15));
    ASSERT_EQ(docid_t(4), queryExecutor->legacySeek(4));
    ASSERT_TRUE(!queryExecutor->isMainDocHit(4));
    ASSERT_EQ(docid_t(17), queryExecutor->legacySeekSub(4, 15, 20));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeekSub(4, 18, 20));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeek(5));
    POOL_DELETE_CLASS(queryExecutor);
}

TEST_F(WeakAndQueryExecutorTest, testSubTermQueryWhenNotSetMainJoin) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term:0;1;2;3;\n";
    fakeIndex.indexes["index_name"] = "term:0;1\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "2,-1", "0,0,1,1");
    IndexPartitionReaderWrapperPtr indexPartReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    
    QueryPtr queryPtr(SearcherTestHelper::createQuery(
                    "sub_index_name:(term||term2)^1 OR index_name:term||term3"));

    QueryExecutor *queryExecutor = QueryExecutorConstructor::createQueryExecutor(
                    _pool, indexPartReaderWrapper.get(), queryPtr.get());
    ASSERT_TRUE(queryExecutor->hasSubDocExecutor());

    ASSERT_EQ(docid_t(0), queryExecutor->legacySeek(0));
    ASSERT_EQ(docid_t(1), queryExecutor->legacySeek(1));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeek(2));
    POOL_DELETE_CLASS(queryExecutor);
}

TEST_F(WeakAndQueryExecutorTest, testReset) {
    FakeIndex fakeIndex;
    fakeIndex.indexes["index_name"] = "term1:1;3;5;\n"
                                      "term2:2;4;6;\n";
    IndexPartitionReaderWrapperPtr indexPartReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    QueryPtr queryPtr(SearcherTestHelper::createQuery(
                    "index_name:(term1 || term2)^1"));
    QueryExecutor *queryExecutor = QueryExecutorConstructor::createQueryExecutor(
            _pool, indexPartReaderWrapper.get(), queryPtr.get());
    ASSERT_EQ(docid_t(1), queryExecutor->legacySeek(0));
    ASSERT_EQ(docid_t(2), queryExecutor->legacySeek(2));
    ASSERT_EQ(docid_t(3), queryExecutor->legacySeek(3));
    ASSERT_EQ(docid_t(4), queryExecutor->legacySeek(4));
    queryExecutor->reset();
    ASSERT_EQ(docid_t(1), queryExecutor->legacySeek(0));
    ASSERT_EQ(docid_t(2), queryExecutor->legacySeek(2));
    ASSERT_EQ(docid_t(3), queryExecutor->legacySeek(3));


    POOL_DELETE_CLASS(queryExecutor);    
}

TEST_F(WeakAndQueryExecutorTest, testAndQueryTwoTermQuery1) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];3[1];5[1];\n";
    string two = "two:2[1];4[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareWeakAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL, 100, 2);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(0));
}

TEST_F(WeakAndQueryExecutorTest, testAndQueryTwoTermQuery2) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];3[1];\n";
    string two = "two:2[1];3[1];4[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareWeakAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL, 100, 2);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(4));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(END_DOCID));
}

TEST_F(WeakAndQueryExecutorTest, testAndQueryTwoTermQuery3) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];8[1];\n";
    string two = "two:1[1];8[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareWeakAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL, 100, 2);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());
    ASSERT_EQ((docid_t)8, _queryExecutor->legacySeek(7));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(10));
}

TEST_F(WeakAndQueryExecutorTest, testAndQueryThreeTermQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];3[1];6[1];7[1];8[1];\n";
    string two = "two:2[1];3[1];5[1];8[1];\n";
    string three = "three:2[1];3[1];6[1];8[1];\n";
    createIndexPartitionReaderWrapper(one + two + three);
    _queryExecutor = QueryExecutorConstructor::prepareWeakAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", "three", 100, 3);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(0));
    ASSERT_EQ(8, _queryExecutor->legacySeek(4));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(10));
}

TEST_F(WeakAndQueryExecutorTest, testSubDocSeekAllSubExecutor) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term1:0;1;2;4;7;9;13;14;\n"
                                          "term2:0;2;3;4;6;8;9;10;11;13;\n"
                                          "term3:0;2;4;6;8;11;13;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,10,15");
    IndexPartitionReaderWrapperPtr indexPartReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    
    QueryPtr queryPtr(SearcherTestHelper::createQuery(
                    "sub_index_name:(term1||term2||term3)^3"));

    QueryExecutor *queryExecutor = QueryExecutorConstructor::createQueryExecutor(
                    _pool, indexPartReaderWrapper.get(), queryPtr.get());
    ASSERT_TRUE(queryExecutor->hasSubDocExecutor());
    ASSERT_EQ(docid_t(0), queryExecutor->legacySeek(0));
    ASSERT_TRUE(!queryExecutor->isMainDocHit(0));
    ASSERT_EQ(docid_t(0), queryExecutor->legacySeekSub(0, 0, 3));
    ASSERT_EQ(docid_t(2), queryExecutor->legacySeekSub(0, 1, 3));
    ASSERT_EQ(docid_t(1), queryExecutor->legacySeek(1));
    ASSERT_TRUE(!queryExecutor->isMainDocHit(1));
    ASSERT_EQ(docid_t(4), queryExecutor->legacySeekSub(1, 3, 5));
    ASSERT_EQ(docid_t(2), queryExecutor->legacySeek(2));
    ASSERT_TRUE(!queryExecutor->isMainDocHit(2));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeekSub(2, 5, 10));
    ASSERT_EQ(docid_t(3), queryExecutor->legacySeek(3));
    ASSERT_TRUE(!queryExecutor->isMainDocHit(3));
    ASSERT_EQ(docid_t(13), queryExecutor->legacySeekSub(3, 10, 15));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeekSub(2, 14, 15));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeek(4));
    POOL_DELETE_CLASS(queryExecutor);
}

TEST_F(WeakAndQueryExecutorTest, testSubDocSeekMultiExecutor) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term1:0;1;4;7;9;10;13;14;15;20;23;31;33;37;40;44;55;\n"
                                          "term2:0;1;4;7;9;11;13;14;15;20;22;24;33;37;40;44;55;\n";
    fakeIndex.indexes["index_name"] = "term1:1;3;5;7;9;11;13;15;\n"
                                      "term2:0;2;3;4;5;6;7;9;13;\n"
                                      "term3:1;3;4;5;6;7;8;9;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex,
            "3,5,10,15,17,17,20,25,30,35,40,45,50,55,60");
    IndexPartitionReaderWrapperPtr indexPartReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    
    QueryPtr queryPtr(SearcherTestHelper::createQuery(
                    "sub_index_name:(term1 || term2)^2 AND "
                    "index_name:(term1 || term2 || term3)^3"));
    
    QueryExecutor *queryExecutor = QueryExecutorConstructor::createQueryExecutor(
                    _pool, indexPartReaderWrapper.get(), queryPtr.get());
    ASSERT_TRUE(queryExecutor->hasSubDocExecutor());
    ASSERT_EQ(docid_t(3), queryExecutor->legacySeek(0));
    ASSERT_TRUE(!queryExecutor->isMainDocHit(3));
    ASSERT_EQ(docid_t(13), queryExecutor->legacySeekSub(3, 10, 15));
    ASSERT_EQ(docid_t(14), queryExecutor->legacySeekSub(3, 14, 15));
    ASSERT_EQ(docid_t(7), queryExecutor->legacySeek(4));
    ASSERT_TRUE(!queryExecutor->isMainDocHit(7));
    ASSERT_EQ(docid_t(20), queryExecutor->legacySeekSub(7, 20, 25));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeekSub(7, 21, 25));
    ASSERT_EQ(docid_t(9), queryExecutor->legacySeek(8));
    ASSERT_TRUE(!queryExecutor->isMainDocHit(9));
    ASSERT_EQ(docid_t(33), queryExecutor->legacySeekSub(9, 30, 35));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeekSub(9, 34, 35));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeek(10));
    POOL_DELETE_CLASS(queryExecutor);
}


TEST_F(WeakAndQueryExecutorTest, testDoNthElement) {
    {
        WeakAndQueryExecutor qe(3);
        std::vector<QueryExecutorEntry> heap;
        std::vector<docid_t> docids = {3,1,1,6,5};
        qe._count = 5;
        heap = initializeHeap(docids);
        qe.doNthElement(heap);
        std::vector<docid_t> expect = {1,1,3,6,5};
        ASSERT_TRUE(heapEq(heap, expect));
    }
    {
        WeakAndQueryExecutor qe(3);
        std::vector<QueryExecutorEntry> heap;
        std::vector<docid_t> docids = {6,1,1,3,5};
        qe._count = 5;
        heap = initializeHeap(docids);
        qe.doNthElement(heap);
        std::vector<docid_t> expect = {1,1,3,6,5};
        ASSERT_TRUE(heapEq(heap, expect));
    }
    {
        WeakAndQueryExecutor qe(3);
        std::vector<QueryExecutorEntry> heap;
        std::vector<docid_t> docids = {6,1,1};
        qe._count = 3;
        heap = initializeHeap(docids);
        qe.doNthElement(heap);
        std::vector<docid_t> expect = {1,1,6};
        ASSERT_TRUE(heapEq(heap, expect));
    }
    {
        WeakAndQueryExecutor qe(1);
        std::vector<QueryExecutorEntry> heap;
        std::vector<docid_t> docids = {3,1,4,2};
        qe._count = 4;
        heap = initializeHeap(docids);
        qe.doNthElement(heap);
        std::vector<docid_t> expect = {1,2,4,3};
        ASSERT_TRUE(heapEq(heap, expect));
    }
    {
        WeakAndQueryExecutor qe(1);
        std::vector<QueryExecutorEntry> heap;
        std::vector<docid_t> docids = {3,1,2,4};
        qe._count = 4;
        heap = initializeHeap(docids);
        qe.doNthElement(heap);
        std::vector<docid_t> expect = {1,3,2,4};
        ASSERT_TRUE(heapEq(heap, expect));
    }
    {
        WeakAndQueryExecutor qe(2);
        std::vector<QueryExecutorEntry> heap;
        std::vector<docid_t> docids = {2,1,2,1,3,2,1};
        qe._count = 7;
        heap = initializeHeap(docids);
        qe.doNthElement(heap);
        std::vector<docid_t> expect = {1,1,2,1,3,2,2};
        ASSERT_TRUE(heapEq(heap, expect));
    }
}

std::vector<QueryExecutorEntry> WeakAndQueryExecutorTest::initializeHeap(
        std::vector<docid_t> &docids)
{
    std::vector<QueryExecutorEntry> ret;
    QueryExecutorEntry dummy;
    ret.push_back(dummy);
    for (uint32_t i = 0; i < docids.size(); ++i) {
        QueryExecutorEntry entry;
        entry.docId = docids[i];
        entry.entryId = i;
        ret.push_back(entry);
    }
    return ret;
}

bool WeakAndQueryExecutorTest::heapEq(
        std::vector<QueryExecutorEntry> &heap,
        std::vector<docid_t> &expect)
{
    for (uint32_t i = 1; i < heap.size(); ++i) {
        if (heap[i].docId != expect[i-1]) {
            return false;
        }
    }
    return true;
}

void WeakAndQueryExecutorTest::createIndexPartitionReaderWrapper(
        const string &indexStr)
{
    FakeIndex fakeIndex;
    fakeIndex.indexes["indexname"] = indexStr;
    _indexPartitionReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexPartitionReaderWrapper->setTopK(1000);
}

END_HA3_NAMESPACE();

