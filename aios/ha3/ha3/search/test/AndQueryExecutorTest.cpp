#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/rank/MatchData.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h> 
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/test/test.h>
#include <ha3/search/AndQueryExecutor.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3/search/test/QueryExecutorConstructor.h>
#include <ha3/search/test/QueryExecutorTestHelper.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/test/SearcherTestHelper.h>

using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(search);

class AndQueryExecutorTest : public TESTBASE {
public:
    AndQueryExecutorTest();
    ~AndQueryExecutorTest();
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

HA3_LOG_SETUP(search, AndQueryExecutorTest);


AndQueryExecutorTest::AndQueryExecutorTest() { 
    _queryExecutor = NULL;
    _pool = new autil::mem_pool::Pool(1024);
}

AndQueryExecutorTest::~AndQueryExecutorTest() {
    delete _pool;
}

void AndQueryExecutorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void AndQueryExecutorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    if (_queryExecutor) {
        POOL_DELETE_CLASS(_queryExecutor);
    }
}

TEST_F(AndQueryExecutorTest, testAndQueryTwoTermQuery1) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];3[1];5[1];\n";
    string two = "two:2[1];4[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(0));
}

TEST_F(AndQueryExecutorTest, testAndQueryTwoTermQuery2) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];3[1];\n";
    string two = "two:2[1];3[1];4[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(4));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(END_DOCID));
}

TEST_F(AndQueryExecutorTest, testAndQueryTwoTermQuery3) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];8[1];\n";
    string two = "two:1[1];8[1];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::prepareAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());
    ASSERT_EQ((docid_t)8, _queryExecutor->legacySeek(7));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(10));
}

TEST_F(AndQueryExecutorTest, testAndQueryThreeTermQuery) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];3[1];6[1];7[1];8[1];\n";
    string two = "two:2[1];3[1];5[1];8[1];\n";
    string three = "three:2[1];3[1];6[1];8[1];\n";
    createIndexPartitionReaderWrapper(one + two + three);
    _queryExecutor = QueryExecutorConstructor::prepareAndQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", "three");
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(0));
    ASSERT_EQ(8, _queryExecutor->legacySeek(4));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(10));
}

TEST_F(AndQueryExecutorTest, testSubDocSeekTwoExecutor) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term:0;1; 4; 11;13; 17;\n";
    fakeIndex.indexes["index_name"] = "term:1;2;3;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,10,15");
    IndexPartitionReaderWrapperPtr indexPartReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    
    QueryPtr queryPtr(SearcherTestHelper::createQuery(
                    "sub_index_name:term AND index_name:term"));

    QueryExecutor *queryExecutor = QueryExecutorConstructor::createQueryExecutor(
                    _pool, indexPartReaderWrapper.get(), queryPtr.get());
    ASSERT_TRUE(queryExecutor->hasSubDocExecutor());

    ASSERT_EQ(docid_t(1), queryExecutor->legacySeek(0));
    ASSERT_TRUE(!queryExecutor->isMainDocHit(1));
    ASSERT_EQ(docid_t(4), queryExecutor->legacySeekSub(1, 3, 5));
    ASSERT_EQ(docid_t(3), queryExecutor->legacySeek(2));
    ASSERT_TRUE(!queryExecutor->isMainDocHit(3));
    ASSERT_EQ(docid_t(11), queryExecutor->legacySeekSub(3, 10, 15));
    ASSERT_EQ(docid_t(13), queryExecutor->legacySeekSub(3, 12, 15));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeek(4));
    POOL_DELETE_CLASS(queryExecutor);
}

TEST_F(AndQueryExecutorTest, testSubDocSeekAllSubExecutor) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term1:0;1;2;4;7;9;13;14;\n"
                                          "term2:0;2;3;4;6;8;9;10;11;13;\n"
                                          "term3:0;2;4;6;8;11;13;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,10,15");
    IndexPartitionReaderWrapperPtr indexPartReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    
    QueryPtr queryPtr(SearcherTestHelper::createQuery(
                    "sub_index_name:term1 AND sub_index_name:term2 AND sub_index_name:term3"));

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

TEST_F(AndQueryExecutorTest, testSubDocSeekMultiExecutor) {
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
                    "sub_index_name:term1 AND sub_index_name:term2 AND "
                    "index_name:term1 AND index_name:term2 AND index_name:term3"));
    
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

TEST_F(AndQueryExecutorTest, testSubDocSeekTwoExecutorWhenNotSetMainJoin) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["sub_index_name"] = "term:0;1;2;3;\n";
    fakeIndex.indexes["index_name"] = "term:0;1;\n";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex,"2,-1", "0,0,1,1");
    IndexPartitionReaderWrapperPtr indexPartReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    
    QueryPtr queryPtr(SearcherTestHelper::createQuery(
                    "sub_index_name:term AND index_name:term"));

    QueryExecutor *queryExecutor = QueryExecutorConstructor::createQueryExecutor(
                    _pool, indexPartReaderWrapper.get(), queryPtr.get());
    ASSERT_TRUE(queryExecutor->hasSubDocExecutor());

    ASSERT_EQ(docid_t(0), queryExecutor->legacySeek(0));
    ASSERT_EQ(docid_t(1), queryExecutor->legacySeek(1));
    ASSERT_EQ(docid_t(END_DOCID), queryExecutor->legacySeek(2));
    POOL_DELETE_CLASS(queryExecutor);
}

void AndQueryExecutorTest::createIndexPartitionReaderWrapper(
        const string &indexStr)
{
    FakeIndex fakeIndex;
    fakeIndex.indexes["indexname"] = indexStr;
    _indexPartitionReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexPartitionReaderWrapper->setTopK(1000);
}

END_HA3_NAMESPACE(search);

