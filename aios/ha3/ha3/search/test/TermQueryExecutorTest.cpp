#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/TermQueryExecutor.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/test/test.h>
#include <ha3/common/TermQuery.h>
#include <ha3/search/TermQueryExecutor.h>
#include <ha3/search/test/QueryExecutorConstructor.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/rank/MetaInfo.h>
#include <memory>
#include <assert.h>

using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(search);

class TermQueryExecutorTest : public TESTBASE {
public:
    TermQueryExecutorTest();
    ~TermQueryExecutorTest();
public:
    void setUp();
    void tearDown();
protected:
    IndexPartitionReaderWrapperPtr _indexReaderWrapper;
    TermQueryExecutor *_queryExecutor;
    rank::MatchData *_matchData;
    autil::mem_pool::Pool *_pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, TermQueryExecutorTest);


TermQueryExecutorTest::TermQueryExecutorTest() { 
   _queryExecutor = NULL;
   _matchData = NULL;
   _pool = new autil::mem_pool::Pool(1024);
}

TermQueryExecutorTest::~TermQueryExecutorTest() {
    delete _pool;
}

void TermQueryExecutorTest::setUp() {
    _matchData = new rank::MatchData;
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "ALIBABA:0[20];1[10,20,30];\n";
    _indexReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexReaderWrapper->setTopK(1000);
}

void TermQueryExecutorTest::tearDown() {
    DELETE_AND_SET_NULL(_matchData);

    if (_queryExecutor) {    
        POOL_DELETE_CLASS(_queryExecutor);
    }
}

TEST_F(TermQueryExecutorTest, testTermQuery) { 
    HA3_LOG(DEBUG, "Begin testQuery");
    _queryExecutor = 
        QueryExecutorConstructor::prepareTermQueryExecutor(_pool,
                "ALIBABA", "phrase", _indexReaderWrapper.get());

    ASSERT_TRUE(_queryExecutor != NULL);
    ASSERT_TRUE(!_queryExecutor->hasSubDocExecutor());

    ASSERT_EQ((docid_t)0, _queryExecutor->legacySeek(0));
    ASSERT_TRUE(_queryExecutor->isMainDocHit(0));
    ASSERT_EQ((docid_t)0, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(2));
}
TEST_F(TermQueryExecutorTest, testTermQueryWithReset) { 
    HA3_LOG(DEBUG, "Begin testQuery");
    _queryExecutor = 
        QueryExecutorConstructor::prepareTermQueryExecutor(_pool,
                "ALIBABA", "phrase", _indexReaderWrapper.get());

    ASSERT_TRUE(_queryExecutor != NULL);
    ASSERT_EQ((docid_t)0, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)0, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(2));
    _queryExecutor->reset();
    ASSERT_EQ((docid_t)0, _queryExecutor->legacySeek(0));
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(2));
}

TEST_F(TermQueryExecutorTest, testTermQueryWithResetEmpytIter) { 
    HA3_LOG(DEBUG, "Begin testQuery");
    _queryExecutor = 
        QueryExecutorConstructor::prepareTermQueryExecutor(_pool,
                "not_exist_term", "phrase", _indexReaderWrapper.get());

    ASSERT_TRUE(_queryExecutor != NULL);
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(0));
    _queryExecutor->reset();
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(0));
}


TEST_F(TermQueryExecutorTest, testTermQueryNoResult2) { 
    HA3_LOG(DEBUG, "Begin testQuery");
    _queryExecutor = 
        QueryExecutorConstructor::prepareTermQueryExecutor(_pool,
                "alibaba", "phrase", _indexReaderWrapper.get());

    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(_queryExecutor->isEmpty());
}

TEST_F(TermQueryExecutorTest, testUnpack) { 
    HA3_LOG(DEBUG, "Begin Test!");

    _queryExecutor = 
        QueryExecutorConstructor::prepareTermQueryExecutor(_pool,
                "ALIBABA", "phrase", _indexReaderWrapper.get());
    _matchData->setMaxNumTerms(1);
    ASSERT_EQ((docid_t)0, _queryExecutor->legacySeek(0));
    rank::TermMatchData &tmd = _matchData->nextFreeMatchData();
    _queryExecutor->unpackMatchData(tmd);

    ASSERT_EQ((uint32_t)1, _matchData->getNumTerms());
    ASSERT_EQ(1, tmd.getTermFreq());
    InDocPositionIteratorPtr inDocPosIter = tmd.getInDocPositionIterator();
    ASSERT_EQ((pos_t)20, inDocPosIter->SeekPosition(0));
    ASSERT_EQ(INVALID_POSITION, inDocPosIter->SeekPosition(21));
}

TEST_F(TermQueryExecutorTest, testUnpackSecondDoc) {
    HA3_LOG(DEBUG, "Begin Test!");

    _queryExecutor = QueryExecutorConstructor::prepareTermQueryExecutor(
            _pool, "ALIBABA", "phrase", _indexReaderWrapper.get());

    _matchData->setMaxNumTerms(1);
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    rank::TermMatchData &tmd = _matchData->nextFreeMatchData();
    _queryExecutor->unpackMatchData(tmd);
    ASSERT_EQ((uint32_t)1, _matchData->getNumTerms());


    ASSERT_EQ(3, tmd.getTermFreq());

    InDocPositionIteratorPtr inDocPosIter = tmd.getInDocPositionIterator();
    ASSERT_EQ((pos_t)10, inDocPosIter->SeekPosition(0));
    ASSERT_EQ((pos_t)20, inDocPosIter->SeekPosition(11));
    ASSERT_EQ((pos_t)30, inDocPosIter->SeekPosition(21));
    ASSERT_EQ(INVALID_POSITION, inDocPosIter->SeekPosition(31));
}

TEST_F(TermQueryExecutorTest, testGetMetaInfo) { 
    HA3_LOG(DEBUG, "Begin Test!");

    _queryExecutor = QueryExecutorConstructor::prepareTermQueryExecutor(
            _pool, "ALIBABA", "phrase", _indexReaderWrapper.get(), 111);

    rank::MetaInfo metaInfo;
    _queryExecutor->getMetaInfo(&metaInfo);
    ASSERT_EQ(1u, metaInfo.size());
    ASSERT_EQ(2, metaInfo[0].getDocFreq());
    ASSERT_EQ(4, metaInfo[0].getTotalTermFreq());
    ASSERT_EQ(string("phrase"), metaInfo[0].getIndexName());
    ASSERT_EQ(string("ALIBABA"), metaInfo[0].getTermText());
    ASSERT_EQ(111, metaInfo[0].getTermBoost());
}

END_HA3_NAMESPACE(search);

