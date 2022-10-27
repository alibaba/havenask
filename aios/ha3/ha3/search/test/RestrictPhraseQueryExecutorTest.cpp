#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/RestrictPhraseQueryExecutor.h>
#include <ha3/search/test/PhraseQueryExecutorTest.h>
#include <ha3/search/test/QueryExecutorConstructor.h>
#include <ha3/search/test/FakeTimeoutTerminator.h>

using namespace std;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(index);

BEGIN_HA3_NAMESPACE(search);

class RestrictPhraseQueryExecutorTest : public PhraseQueryExecutorTest {

public:
    RestrictPhraseQueryExecutorTest();
    ~RestrictPhraseQueryExecutorTest();
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, RestrictPhraseQueryExecutorTest);


RestrictPhraseQueryExecutorTest::RestrictPhraseQueryExecutorTest() { 
}

RestrictPhraseQueryExecutorTest::~RestrictPhraseQueryExecutorTest() { 
}

void RestrictPhraseQueryExecutorTest::setUp() { 
    PhraseQueryExecutorTest::setUp();
}

void RestrictPhraseQueryExecutorTest::tearDown() { 
    PhraseQueryExecutorTest::tearDown();
}

TEST_F(RestrictPhraseQueryExecutorTest, testTimeoutCheck) { 
    string abc = "abc:0[1];1[1];2[1];3[1];4[1];5[1];6[1];7[1];8[1];9[1];10[1];11[1];12[1];13[1];14[1];15[1];16[1];\n";
    string def = "def:0[2];1[2];2[2];3[2];4[2];5[2];6[2];7[2];8[2];9[2];10[2];11[2];12[2];13[2];14[2];15[2];16[2];\n";
    createIndexPartitionReaderWrapper(abc + def);
  
    vector<string>strVec;
    vector<bool>posVec;

    strVec.push_back(string("abc"));
    strVec.push_back(string("def"));

    posVec.push_back(false);
    posVec.push_back(false);

    FakeTimeoutTerminator timeoutTerminator(2);
    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", strVec, posVec, DEFAULT_BOOST_VALUE, &timeoutTerminator, NULL);

    int32_t checkStep = common::TimeoutTerminator::RESTRICTOR_CHECK_MASK + 1;
    for (int32_t i = 0; i < checkStep; ++i) {
        ASSERT_EQ(docid_t(i), _queryExecutor->legacySeek(docid_t(i)));
    }
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(docid_t(16)));
}

TEST_F(RestrictPhraseQueryExecutorTest, testLayerMeta) { 
    string abc = "abc:0[1];1[1,3,5,23];19[4,23,55];25[8,23,59];29[10,11];\n";
    string def = "def:0[10];1[1,2,5,23];19[5,28,59];25[10,24,65];29[12];\n";
    createIndexPartitionReaderWrapper(abc + def);
  
    vector<string>strVec;
    vector<bool>posVec;

    strVec.push_back(string("abc"));
    strVec.push_back(string("def"));

    posVec.push_back(false);
    posVec.push_back(false);
    autil::mem_pool::Pool pool(1024);
    LayerMeta layerMeta(&pool);
    layerMeta.push_back(DocIdRangeMeta(0, 10));
    layerMeta.push_back(DocIdRangeMeta(25, 28));

    _queryExecutor = 
        QueryExecutorConstructor::preparePhraseQueryExecutor(
                _pool, _indexPartitionReaderWrapper.get(), "indexname", strVec, posVec, DEFAULT_BOOST_VALUE, NULL, &layerMeta);

    ASSERT_EQ(docid_t(1), _queryExecutor->legacySeek(docid_t(0)));
    ASSERT_EQ(docid_t(1), _queryExecutor->legacySeek(docid_t(1)));
    ASSERT_EQ(docid_t(25), _queryExecutor->legacySeek(docid_t(2)));
    ASSERT_EQ(END_DOCID, _queryExecutor->legacySeek(docid_t(26)));
}

END_HA3_NAMESPACE(search);

