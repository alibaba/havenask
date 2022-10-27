#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/PKQueryExecutor.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3_sdk/testlib/index/FakePostingMaker.h>
#include <ha3/search/test/QueryExecutorConstructor.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>

using namespace autil;
using namespace std;

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(search);

class PKQueryExecutorTest : public TESTBASE {
public:
    PKQueryExecutorTest();
    ~PKQueryExecutorTest();
public:
    void setUp();
    void tearDown();
protected:
    void SeekFail(docid_t docId);
protected:
    IndexPartitionReaderWrapperPtr _indexReaderWrapper;
    autil::mem_pool::Pool *_pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, PKQueryExecutorTest);


PKQueryExecutorTest::PKQueryExecutorTest() {
    _pool = new autil::mem_pool::Pool(1024);
}

PKQueryExecutorTest::~PKQueryExecutorTest() {
    delete _pool;
}

void PKQueryExecutorTest::setUp() { 
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "ALIBABA:0[20];1[10,20,30];\n";
    _indexReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexReaderWrapper->setTopK(1000);
}

void PKQueryExecutorTest::tearDown() { 
}

TEST_F(PKQueryExecutorTest, testSeekCorrect) { 
    HA3_LOG(DEBUG, "Begin Test!");
    QueryExecutor *queryExecutor = 
        QueryExecutorConstructor::prepareTermQueryExecutor(
                _pool, "ALIBABA", "phrase", _indexReaderWrapper.get());
    PKQueryExecutor *pkQueryExecutor = new PKQueryExecutor(queryExecutor, 1);
    ASSERT_TRUE(pkQueryExecutor);
    ASSERT_TRUE(!pkQueryExecutor->hasSubDocExecutor());
    ASSERT_EQ(docid_t (1), pkQueryExecutor->legacySeek(0));
    ASSERT_TRUE(pkQueryExecutor->isMainDocHit(1));
    ASSERT_EQ(docid_t (END_DOCID), pkQueryExecutor->legacySeek(2));
    delete pkQueryExecutor;
}

TEST_F(PKQueryExecutorTest, testSeekFailEndDoc) { 
    SeekFail(END_DOCID);
}

TEST_F(PKQueryExecutorTest, testSeekFail) { 
    SeekFail(2);
}

void PKQueryExecutorTest::SeekFail(docid_t docId) { 
    HA3_LOG(DEBUG, "Begin Test!");
    QueryExecutor *queryExecutor = 
        QueryExecutorConstructor::prepareTermQueryExecutor(_pool,
                "ALIBABA", "phrase", _indexReaderWrapper.get());
    PKQueryExecutor *pkQueryExecutor1 = new PKQueryExecutor(queryExecutor, docId);
    ASSERT_EQ(docid_t (END_DOCID), pkQueryExecutor1->legacySeek(0));
    delete pkQueryExecutor1;
}

END_HA3_NAMESPACE(search);

