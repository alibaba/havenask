#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/rank/MatchData.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/common/Term.h>
#include <ha3/common/PhraseQuery.h>
#include <ha3/test/test.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3_sdk/testlib/index/FakeIndexReader.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3/search/test/QueryExecutorConstructor.h>
#include <ha3/search/test/PhraseQueryExecutorTest.h> 
using namespace std;
using namespace autil;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(search);

HA3_LOG_SETUP(search, PhraseQueryExecutorTest);

PhraseQueryExecutorTest::PhraseQueryExecutorTest() { 
    _queryExecutor = NULL;
    _pool = new autil::mem_pool::Pool(1024);
}

PhraseQueryExecutorTest::~PhraseQueryExecutorTest() {
    delete _pool;
}

void PhraseQueryExecutorTest::setUp() { 
    HA3_LOG(DEBUG, "Begin setUp");
}

void PhraseQueryExecutorTest::tearDown() {
    if (_queryExecutor) {
        POOL_DELETE_CLASS(_queryExecutor);
    }
}

TEST_F(PhraseQueryExecutorTest, testPhraseOneTerm) {
    string one = "one:3[1];";
    createIndexPartitionReaderWrapper(one);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", NULL, NULL);

    ASSERT_TRUE(_queryExecutor);
    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(0));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(4));
}

TEST_F(PhraseQueryExecutorTest, testPhraseTwoTerm) {
    string one = "one:2[1];5[3,7];\n";
    string two = "two:1[3];5[8];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::preparePhraseQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);

    ASSERT_TRUE(_queryExecutor);
    ASSERT_EQ((docid_t)5, _queryExecutor->legacySeek(0));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(6));
}

TEST_F(PhraseQueryExecutorTest, testPhraseQueryNoResult1) {
    string one = "one:2[1];5[3,7];\n";
    string two = "two:3[3];7[8];\n";
    createIndexPartitionReaderWrapper(one + two);
    _queryExecutor = QueryExecutorConstructor::preparePhraseQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);

    ASSERT_TRUE(_queryExecutor);
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(0));
}

TEST_F(PhraseQueryExecutorTest, testPhraseQueryNoResult2) {
    string one = "one:2[1];5[3,7];\n";
    createIndexPartitionReaderWrapper(one);
    _queryExecutor = QueryExecutorConstructor::preparePhraseQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "one", "two", NULL);
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(_queryExecutor->isEmpty());
}

TEST_F(PhraseQueryExecutorTest, testPhraseQueryNoResult) { 
    HA3_LOG(DEBUG, "Begin");
    createIndexPartitionReaderWrapper("");
    _queryExecutor = QueryExecutorConstructor::preparePhraseQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "this", "is", "a");
    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(_queryExecutor->isEmpty());
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuery) { 
    HA3_LOG(DEBUG, "Begin");
    string one = "THIS:0[1];5[3,7];\n";
    string two = "IS:0[2];6[3,7];\n";
    string three = "A:0[3];7[3,7];\n";

    createIndexPartitionReaderWrapper(one + two + three);
    _queryExecutor = QueryExecutorConstructor::preparePhraseQueryExecutor(
            _pool, _indexPartitionReaderWrapper.get(), "indexname", "THIS", "IS", "A");

    ASSERT_TRUE(_queryExecutor);
    ASSERT_EQ((docid_t)0, _queryExecutor->legacySeek(0));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(1));
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeek) {
    HA3_LOG(DEBUG, "Begin testPhraseQuerySeek");

    FakeTextIndexReader::Map map;
    string abc = "abc:0[0];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[1];2[1,3,5,23];19[5,27,59];28[10,24,52];29[12];\n";
    string hij = "hij:0[2];1[1,3,5,23];19[41,557];28[1,25,66];29[3,5,13];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");

    ASSERT_TRUE(_queryExecutor);
    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid);
    ASSERT_EQ((docid_t)0, docid);

    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)28, docid);
    docid = _queryExecutor->legacySeek(docid);
    ASSERT_EQ((docid_t)28, docid);

    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid - 2);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid);
    ASSERT_EQ((docid_t)29, docid);

    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeek2) {
    HA3_LOG(DEBUG, "Begin %s", __func__);
    string abc = "abc:0[0];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[5,27,59];28[10,26,52];29[12];\n";
    string hij = "hij:0[2];1[1,3,5,23];19[41,557];28[1,25,66];29[3,5,10];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");
    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeek3) {
    HA3_LOG(DEBUG, "Begin %s", __func__);
    string abc = "abc:0[0];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[5,27,59];28[10,24,52];29[12];\n";
    string hij = "hij:0[2];1[1,3,5,23];19[41,557];28[1,25,66];29[3,5,10];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");
    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)28, docid);
    docid = _queryExecutor->legacySeek(docid);
    ASSERT_EQ((docid_t)28, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}
    
TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeek4) {
    HA3_LOG(DEBUG, "Begin %s", __func__);
    string abc = "abc:0[0];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[5,27,59];28[10,24,52];29[12];\n";
    string hij = "hij:0[2];1[1,3,5,23];19[41,557];28[1,25,66];29[3,5,13];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");
    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)28, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeek5) {
    HA3_LOG(DEBUG, "Begin %s", __func__);
    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[5,27,59];28[10,24,52];29[15];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[41,557];28[1,26,66];29[3,5,13];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");
    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeek6) {
    HA3_LOG(DEBUG, "Begin %s", __func__);
    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,14];\n";
    string def = "def:0[5];2[1,3,5,23];19[5,27,59];28[10,24,52];29[15];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[41,557];28[1,26,66];29[3,5,16];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");
    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeek7) {
    HA3_LOG(DEBUG, "Begin %s", __func__);
    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[5,24,59];28[10,24,52];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[1,25];28[1,26,66];29[3,5,13];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");
    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeek8) {
    HA3_LOG(DEBUG, "Begin %s", __func__);
    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,6,23];19[5,24,59];28[10,24,52];29[16];\n";
    string hij = "hij:0[3];1[1,3,7,23];19[1,25];28[1,26,66];29[3,5,13];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeek9) {
    HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[5,28,59];28[10,24,52];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[1,25];28[1,26,66];29[3,5,13];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}
TEST_F(PhraseQueryExecutorTest, testGetDocId) {
    HA3_LOG(DEBUG, "Begin Test!");

    HA3_LOG(DEBUG, "Begin %s", __func__);
    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[5,24,59];29[12];\n";
    createIndexPartitionReaderWrapper(abc + def);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", NULL);
    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    ASSERT_EQ((docid_t)0, _queryExecutor->getDocId());
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)19, docid);
    ASSERT_EQ((docid_t)19, _queryExecutor->getDocId());
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    ASSERT_EQ((docid_t)29, _queryExecutor->getDocId());
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
    ASSERT_EQ((docid_t)END_DOCID, _queryExecutor->getDocId());
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithStopWord1) {
    HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[5,28,59];28[10,24,52];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,25];28[1,26,66];29[3,5,13];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij",
                DEFAULT_BOOST_VALUE, true);

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithStopWord2) {
    HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[28,59];28[10,24,52];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,24];28[1,26,66];29[3,5,15];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij",
                DEFAULT_BOOST_VALUE,false,true);

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)1, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithStopWord3) {
    HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[5,28,59];28[10,24,52];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,25];28[1,26,66];29[3,5,13];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij",
                DEFAULT_BOOST_VALUE, false,false,true);

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)28, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithStopWord4) {
    HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[5,28,59];28[10,24,52];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,25];28[1,26,66];29[3,5,13];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij",
                DEFAULT_BOOST_VALUE, true,true,false);

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)1, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)28, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithStopWord5) {
    HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[5,28,59];28[10,24,52];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,25];28[1,26,66];29[3,5,13];\n";
    createIndexPartitionReaderWrapper(abc + def + hij);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij",
                DEFAULT_BOOST_VALUE, true,true,true);

    ASSERT_TRUE(_queryExecutor);
    ASSERT_TRUE(_queryExecutor->isEmpty());
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithoutPosting1) {
    HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[5,28,59];28[10,24,65];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,25];28[1,26,66];29[3,5,13];\n";

    FakeTextIndexReader::PositionMap posMap;
    posMap.insert(make_pair(string("abc"), false));
    createIndexPartitionReaderWrapper(abc + def + hij, posMap);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)28, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}
TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithoutPosting2) {
    HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];1[1,3,5,23];2[1,3,5,23];19[5,28,59];28[10,24,65];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,25];28[1,26,66];29[3,5,13];\n";
    FakeTextIndexReader::PositionMap posMap;
    posMap.insert(make_pair(string("def"), false));
    createIndexPartitionReaderWrapper(abc + def + hij, posMap);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)1, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithoutPosting3) {
    HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[2];2[1,3,5,23];19[5,28,59];28[10,24,65];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[10,25];28[1,26,66];29[3,5,13];\n";
    FakeTextIndexReader::PositionMap posMap;
    posMap.insert(make_pair(string("hij"), false));
    createIndexPartitionReaderWrapper(abc + def + hij, posMap);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)28, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}
TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithoutPosting4) {
    HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[10];1[1,3,5,23];19[7,28,59];28[10,24,65];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,25];28[1,26,66];29[3,5,13];\n";
    FakeTextIndexReader::PositionMap posMap;
    posMap.insert(make_pair(string("abc"), false));
    posMap.insert(make_pair(string("def"), false));
    posMap.insert(make_pair(string("hij"), false));
    createIndexPartitionReaderWrapper(abc + def + hij, posMap);
    
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)1, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)28, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}
TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithoutPosting5) {
    HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[10];1[1,3,5,23];19[7,28,59];28[10,24,65];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,25];28[1,26,66];29[3,5,13];\n";
    FakeTextIndexReader::PositionMap posMap;
    posMap.insert(make_pair(string("def"), false));
    posMap.insert(make_pair(string("hij"), false));
    createIndexPartitionReaderWrapper(abc + def + hij, posMap);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)1, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)28, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}
TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithoutPosting6) {
    HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[10];1[1,3,5,23];19[7,28,59];28[10,24,65];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,25];28[1,26,66];29[3,5,13];\n";

    FakeTextIndexReader::PositionMap posMap;
    posMap.insert(make_pair(string("abc"), false));
    posMap.insert(make_pair(string("hij"), false));

    createIndexPartitionReaderWrapper(abc + def + hij, posMap);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij");

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)1, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)28, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}
TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithStopWordAndMissingPosting1){
      HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[10];1[1,3,5,23];19[7,28,59];28[10,24,65];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,25];28[1,26,66];29[3,5,13];\n";
    FakeTextIndexReader::PositionMap posMap;
    posMap.insert(make_pair(string("abc"), false));    
    createIndexPartitionReaderWrapper(abc + def + hij, posMap);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij",
                DEFAULT_BOOST_VALUE, false, true, false);

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)1, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)28, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}
TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithStopWordAndMissingPosting2){
      HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[10];1[1,3,5,23];19[7,28,59];28[10,24,65];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,25];28[1,26,66];29[3,5,13];\n";
    FakeTextIndexReader::PositionMap posMap;
    posMap.insert(make_pair(string("abc"), false));
    createIndexPartitionReaderWrapper(abc + def + hij, posMap);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", "abc", "def", "hij",
                DEFAULT_BOOST_VALUE, false, false, true);

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)1, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)19, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)28, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}
TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithStopWordAndMissingPosting3){
      HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[10];1[1,2,5,23];19[7,28,59];28[10,24,65];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,25];28[1,26,66];29[3,5,13];\n";
    string klm = "klm:0[4];1[1,4,5,23];19[10,28];28[3,9,15];29[6,8,18];\n";

    FakeTextIndexReader::PositionMap posMap;
    posMap.insert(make_pair(string("abc"), false));    
    createIndexPartitionReaderWrapper(abc + def + hij + klm, posMap);

    vector<string>strVec;
    vector<bool>posVec;

    strVec.push_back(string("abc"));
    strVec.push_back(string("def"));
    strVec.push_back(string("hij"));
    strVec.push_back(string("klm"));

    posVec.push_back(false);
    posVec.push_back(true);
    posVec.push_back(false);
    posVec.push_back(false);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", strVec, posVec);

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)0, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)1, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)29, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

TEST_F(PhraseQueryExecutorTest, testPhraseQuerySeekWithStopWordAndMissingPosting4){
    HA3_LOG(DEBUG, "Begin %s", __func__);

    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];28[8,23,59];29[10,11];\n";
    string def = "def:0[10];1[1,2,5,23];19[7,28,59];28[10,24,65];29[12];\n";
    string hij = "hij:0[3];1[1,3,5,23];19[6,25];28[1,26,66];29[3,5,13];\n";
    string klm = "klm:0[4];1[1,4,5,23];19[10,28];28[3,9,15];29[6,8,18];\n";
    FakeTextIndexReader::PositionMap posMap;
    posMap.insert(make_pair(string("abc"), false));    

    createIndexPartitionReaderWrapper(abc + def + hij + klm, posMap);
  
    vector<string>strVec;
    vector<bool>posVec;

    strVec.push_back(string("abc"));
    strVec.push_back(string("def"));
    strVec.push_back(string("hij"));
    strVec.push_back(string("klm"));

    posVec.push_back(false);
    posVec.push_back(false);
    posVec.push_back(true);
    posVec.push_back(false);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", strVec, posVec);

    docid_t docid = _queryExecutor->legacySeek(docid_t(0));
    ASSERT_EQ((docid_t)1, docid);
    docid = _queryExecutor->legacySeek(docid + 1);
    ASSERT_EQ((docid_t)END_DOCID, docid);
}

void PhraseQueryExecutorTest::createIndexPartitionReaderWrapper(
        const string &indexStr, const FakeTextIndexReader::PositionMap &posMap)
{
    FakeIndex fakeIndex;
    fakeIndex.indexes["indexname"] = indexStr;
    _indexPartitionReaderWrapper = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexPartitionReaderWrapper->setTopK(1000);
    IndexReaderPtr indexReader = 
        _indexPartitionReaderWrapper->getIndexReader("indexname");
    FakeIndexReader *fakeIndexReader = 
        dynamic_cast<FakeIndexReader *>(indexReader.get());
    ASSERT_TRUE(fakeIndexReader != NULL);
    indexReader = fakeIndexReader->getIndexReader("indexname");
    ASSERT_TRUE(indexReader);
    FakeTextIndexReader *fakeTextIndexReader = 
        dynamic_cast<FakeTextIndexReader *>(indexReader.get());
    ASSERT_TRUE(fakeTextIndexReader != NULL);
    fakeTextIndexReader->setPostitionMap(posMap);
}

END_HA3_NAMESPACE(search);

