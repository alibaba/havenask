#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MultiTermOrQueryExecutor.h>
#include <ha3/search/test/QueryExecutorConstructor.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>

using namespace std;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);

BEGIN_HA3_NAMESPACE(search);

class MultiTermOrQueryExecutorTest : public TESTBASE {
public:
    MultiTermOrQueryExecutorTest();
    ~MultiTermOrQueryExecutorTest();
public:
    void setUp();
    void tearDown();

private:
    QueryExecutor *_queryExecutor;
    autil::mem_pool::Pool *_pool;
    IndexPartitionReaderWrapperPtr _indexReaderWrapper;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, MultiTermOrQueryExecutorTest);

MultiTermOrQueryExecutorTest::MultiTermOrQueryExecutorTest() {
    _queryExecutor = NULL;
   _pool = new autil::mem_pool::Pool(1024);
}

MultiTermOrQueryExecutorTest::~MultiTermOrQueryExecutorTest() {
    delete _pool;
}

void MultiTermOrQueryExecutorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void MultiTermOrQueryExecutorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    if (_queryExecutor) {
        POOL_DELETE_CLASS(_queryExecutor);
    }
}

TEST_F(MultiTermOrQueryExecutorTest, TwoTermQuery1) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];4[1];\n";
    string two = "two:6[2];8[1];\n";
    FakeIndex fakeIndex;
    fakeIndex.indexes["indexname"] = one + two;
    _indexReaderWrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexReaderWrapper->setTopK(1000);

    _queryExecutor = QueryExecutorConstructor::prepareMultiTermOrQueryExecutor(
            _pool, _indexReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)4, _queryExecutor->legacySeek(3));
    ASSERT_EQ((docid_t)4, _queryExecutor->legacySeek(4));
    ASSERT_EQ((docid_t)6, _queryExecutor->legacySeek(5));
    ASSERT_EQ((docid_t)8, _queryExecutor->legacySeek(8));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(9));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(9));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(END_DOCID));
}

TEST_F(MultiTermOrQueryExecutorTest, TwoTermQuery2) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];3[1];5[1];\n";
    string two = "two:2[1];4[1];\n";

    FakeIndex fakeIndex;
    fakeIndex.indexes["indexname"] = one + two;
    _indexReaderWrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexReaderWrapper->setTopK(1000);

    _queryExecutor = QueryExecutorConstructor::prepareMultiTermOrQueryExecutor(
            _pool, _indexReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    ASSERT_EQ((docid_t)4, _queryExecutor->legacySeek(4));
    ASSERT_EQ((docid_t)5, _queryExecutor->legacySeek(5));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(6));
}

TEST_F(MultiTermOrQueryExecutorTest, TwoTermQuery3) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];3[1];\n";
    string two = "two:2[1];3[1];4[1];\n";

    FakeIndex fakeIndex;
    fakeIndex.indexes["indexname"] = one + two;
    _indexReaderWrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexReaderWrapper->setTopK(1000);

    _queryExecutor = QueryExecutorConstructor::prepareMultiTermOrQueryExecutor(
            _pool, _indexReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(1));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    ASSERT_EQ((docid_t)4, _queryExecutor->legacySeek(4));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(5));
}

TEST_F(MultiTermOrQueryExecutorTest, TwoTermQuery4) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];9[1];\n";
    string two = "two:2[1];8[1];\n";

    FakeIndex fakeIndex;
    fakeIndex.indexes["indexname"] = one + two;
    _indexReaderWrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexReaderWrapper->setTopK(1000);

    _queryExecutor = QueryExecutorConstructor::prepareMultiTermOrQueryExecutor(
            _pool, _indexReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)8, _queryExecutor->legacySeek(7));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(10));
    ASSERT_EQ(END_DOCID, _queryExecutor->getDocId());
}

TEST_F(MultiTermOrQueryExecutorTest, TwoTermQueryWithOneEmptyQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];3[1];\n";
    string two = "two:2[1];3[1];4[1];\n";
    FakeIndex fakeIndex;
    fakeIndex.indexes["indexname"] = one + two;
    _indexReaderWrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexReaderWrapper->setTopK(1000);

    _queryExecutor = QueryExecutorConstructor::prepareMultiTermOrQueryExecutor(
            _pool, _indexReaderWrapper.get(), "indexname", "one", "no_such_term", NULL);
    ASSERT_TRUE(_queryExecutor);

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(5));
}


TEST_F(MultiTermOrQueryExecutorTest, TwoTermQueryWithTwoEmptyQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];2[1];3[1];\n";
    string two = "two:2[1];3[1];4[1];\n";
    FakeIndex fakeIndex;
    fakeIndex.indexes["indexname"] = one + two;
    _indexReaderWrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexReaderWrapper->setTopK(1000);

    _queryExecutor = QueryExecutorConstructor::prepareMultiTermOrQueryExecutor(
            _pool, _indexReaderWrapper.get(), "indexname", "no_such_term_1", "no_such_term_2", NULL);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(0));
}

TEST_F(MultiTermOrQueryExecutorTest, ThreeTermQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    string one = "one:1[1];3[1];6[1];7[1];\n";
    string two = "two:2[1];3[1];5[1];\n";
    string three = "three:2[1];3[1];6[1];8[1];\n";
    FakeIndex fakeIndex;
    fakeIndex.indexes["indexname"] = one + two + three;
    _indexReaderWrapper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexReaderWrapper->setTopK(1000);

    _queryExecutor = QueryExecutorConstructor::prepareMultiTermOrQueryExecutor(
            _pool, _indexReaderWrapper.get(), "indexname", "one", "two", "three");
    ASSERT_TRUE(_queryExecutor);

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

END_HA3_NAMESPACE(search);
