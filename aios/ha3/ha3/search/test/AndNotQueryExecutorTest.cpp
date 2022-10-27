#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/search/AndNotQueryExecutor.h>
#include <ha3/test/test.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <iostream>
#include <ha3/search/test/QueryExecutorConstructor.h>
#include <ha3/search/test/SearcherTestHelper.h>
#include <ha3/search/test/QueryExecutorTestHelper.h>
using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(search);

class AndNotQueryExecutorTest : public TESTBASE {
public:
    AndNotQueryExecutorTest();
    ~AndNotQueryExecutorTest();
public:
    void setUp();
    void tearDown();
protected:
    void createIndexPartitionReaderWrapper(const std::string &indexStr);
protected:
    QueryExecutor *_queryExecutor;
    autil::mem_pool::Pool *_pool;
    IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, AndNotQueryExecutorTest);


AndNotQueryExecutorTest::AndNotQueryExecutorTest() {
    _queryExecutor = NULL;
    _pool = new autil::mem_pool::Pool(1024);
}

AndNotQueryExecutorTest::~AndNotQueryExecutorTest() {
    delete _pool;
}

void AndNotQueryExecutorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void AndNotQueryExecutorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    if (_queryExecutor) {
        POOL_DELETE_CLASS(_queryExecutor);
    }
}

TEST_F(AndNotQueryExecutorTest, testAndNotQueryTwoTermQuery1) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];4[1];\n";
    string two = "two:6[1];8[1];9[1];10[1]\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareAndNotQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)4, _queryExecutor->legacySeek(4));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(5));
}

TEST_F(AndNotQueryExecutorTest, testAndNotQueryTwoTermQuery2) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];5[1];\n";
    string two = "two:2[1];4[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareAndNotQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)5, _queryExecutor->legacySeek(5));
    ASSERT_EQ((docid_t)5, _queryExecutor->legacySeek(5));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(6));
}

TEST_F(AndNotQueryExecutorTest, testAndNotQueryTwoTermQuery3) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];3[1];\n";
    string two = "two:1[1];2[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareAndNotQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);

    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(4));
}

TEST_F(AndNotQueryExecutorTest, testAndNotQueryTwoTermQuery4) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];3[1];\n";
    string two = "two:1[1];2[1];3[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareAndNotQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);

    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(0));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(1));
}

TEST_F(AndNotQueryExecutorTest, testAndNotQueryTwoTermQuery5) {
    HA3_LOG(DEBUG, "Begin Test!");

    string one = "one:1[1];3[1];5[1];\n";
    string two = "two:2[1];4[1];6[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareAndNotQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);

    ASSERT_EQ((docid_t)5, _queryExecutor->legacySeek(4));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(6));
}

TEST_F(AndNotQueryExecutorTest, testAndNotQueryTwoTermQueryWithOneEmpty) {
    HA3_LOG(DEBUG, "Begin Test!");

    string one = "one:1[1];2[1];3[1];\n";
    string two = "two:1[1];2[1];3[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareAndNotQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "no_such_term", NULL);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(4));
}

TEST_F(AndNotQueryExecutorTest, testAndNotQueryTwoTermQueryWithFirstEmpty) {
    HA3_LOG(DEBUG, "Begin Test!");

    string one = "one:1[1];2[1];3[1];\n";
    string two = "two:1[1];2[1];3[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareAndNotQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "no_such_term", "one", NULL);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(_queryExecutor->isEmpty());
}

TEST_F(AndNotQueryExecutorTest, testAndNotQueryTwoTermQueryWithTwoEmpty) {
    HA3_LOG(DEBUG, "Begin Test!");

    string one = "one:1[1];2[1];3[1];\n";
    string two = "two:1[1];2[1];3[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareAndNotQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "no_such_term",
            "no_such_term", NULL);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());

    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(_queryExecutor->isEmpty());
}

TEST_F(AndNotQueryExecutorTest, testAndNotQueryThreeTermQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];3[1];6[1];7[1];\n";
    string two = "two:2[1];3[1];5[1];\n";
    string three = "three:2[1];3[1];6[1];8[1];\n";
    createIndexPartitionReaderWrapper(one + two + three);
    _queryExecutor = QueryExecutorConstructor::prepareAndNotQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", "three");
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)7, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)7, _queryExecutor->legacySeek(7));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(8));
}

TEST_F(AndNotQueryExecutorTest, testReset) {
    string one = "one:1[1];2[1];4[1];\n";
    string two = "two:6[1];8[1];9[1];10[1]\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareAndNotQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    _queryExecutor->reset();
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));    
}

TEST_F(AndNotQueryExecutorTest, testSubDocSeekMultiExecutor) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term1:0;1; 7;9; 10;13;14; 15; 20;23; 31;33; 37; 40;44; 55;\n";
    fakeIndex.indexes["index_name"] = "term1:1;3;5;7;9;11;13;15;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex,
            "3,5,10,15,17,17,20,25,30,35,40,45,50,55,60");
    IndexPartitionReaderWrapperPtr indexPartReaderWrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);

    QueryPtr queryPtr(SearcherTestHelper::createQuery(
                    "index_name:term1 ANDNOT sub_index_name:term1"));

    QueryExecutor *queryExecutor = QueryExecutorConstructor::createQueryExecutor(
                    _pool, indexPartReaderWrapper.get(), queryPtr.get());
    ASSERT_TRUE(queryExecutor->hasSubDocExecutor());
    ASSERT_EQ(docid_t(1), queryExecutor->legacySeek(0));
    ASSERT_FALSE(queryExecutor->isMainDocHit(1));
    ASSERT_EQ(docid_t(3), queryExecutor->legacySeekSub(1, 3, 5));
    ASSERT_EQ(docid_t(4), queryExecutor->legacySeekSub(1, 4, 5));
    ASSERT_EQ(docid_t(5), queryExecutor->legacySeek(4));
    ASSERT_FALSE(queryExecutor->isMainDocHit(5));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeekSub(5, 17, 17));
    ASSERT_EQ(docid_t(13), queryExecutor->legacySeek(12));
    ASSERT_FALSE(queryExecutor->isMainDocHit(13));
    ASSERT_EQ(docid_t(50), queryExecutor->legacySeekSub(13, 50, 55));
    ASSERT_EQ(docid_t(51), queryExecutor->legacySeekSub(13, 51, 55));
    ASSERT_EQ(docid_t(52), queryExecutor->legacySeekSub(13, 52, 55));
    ASSERT_EQ(docid_t(53), queryExecutor->legacySeekSub(13, 53, 55));
    ASSERT_EQ(docid_t(54), queryExecutor->legacySeekSub(13, 54, 55));
    ASSERT_EQ(docid_t(15), queryExecutor->legacySeek(14));
    ASSERT_FALSE(queryExecutor->isMainDocHit(15));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeek(16));

    POOL_DELETE_CLASS(queryExecutor);
}

TEST_F(AndNotQueryExecutorTest, testSubDocSeekWhenNotSetMainJoin) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term:0;1;2;3;\n";
    fakeIndex.indexes["index_name"] = "term:1;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex,"2,-1", "0,0,1,1");
    IndexPartitionReaderWrapperPtr indexPartReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    
    QueryPtr queryPtr(SearcherTestHelper::createQuery(
                    "sub_index_name:term ANDNOT index_name:term"));

    QueryExecutor *queryExecutor = QueryExecutorConstructor::createQueryExecutor(
                    _pool, indexPartReaderWrapper.get(), queryPtr.get());
    ASSERT_TRUE(queryExecutor->hasSubDocExecutor());

    ASSERT_EQ(docid_t(0), queryExecutor->legacySeek(0));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeek(1));
    POOL_DELETE_CLASS(queryExecutor);
}

void AndNotQueryExecutorTest::createIndexPartitionReaderWrapper(
        const string &indexStr)
{
    FakeIndex fakeIndex;
    fakeIndex.indexes["indexname"] = indexStr;
    _indexPartitionReaderWrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexPartitionReaderWrapper->setTopK(1000);
}

END_HA3_NAMESPACE(search);

