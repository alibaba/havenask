#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/index/TableReader.h>
#include <ha3/index/ReaderFacade.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/rank/MatchData.h>
#include <ha3/test/test.h>
#include <suez/turing/expression/util/IndexInfos.h>
#include <ha3/index/TableReader.h>
#include <ha3/rank/MatchData.h>
#include <ha3/common/TermQuery.h>
#include <ha3/search/TermQueryExecutor.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <memory>

using namespace std;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(rank);
BEGIN_HA3_NAMESPACE(search);

class UnpackMatchDataTest : public TESTBASE {
public:
    UnpackMatchDataTest();
    ~UnpackMatchDataTest();
public:
    void setUp();
    void tearDown();
protected:
    void prepareQueryExecutor(const char *str);
protected:
    index::TableReader *_tableReader;
    index::ReaderFacade *_readerFacade;
    QueryExecutor *_queryExecutor;
    rank::MatchData *_matchData;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, UnpackMatchDataTest);


UnpackMatchDataTest::UnpackMatchDataTest() { 
    _tableReader = NULL;
    _readerFacade = NULL;
    _queryExecutor = NULL;
    _matchData = NULL;
}

UnpackMatchDataTest::~UnpackMatchDataTest() { 
}

void UnpackMatchDataTest::setUp() { 
    std::string basePath = TEST_DATA_PATH;
    std::string rootPath = basePath + "/multiroot";

    TableInfo tableInfo(rootPath.c_str());
    tableInfo.tableName = "simple";
    IndexInfos *indexInfos = new IndexInfos(tableInfo.getPath().c_str());
    IndexInfo *indexInfo = new IndexInfo(indexInfos->getPath().c_str());
    indexInfo->indexName = "phrase";
    indexInfo->indexType = it_text;
    indexInfos->addIndexInfo(indexInfo);
    tableInfo.setIndexInfos(indexInfos);
    tableInfo.setRankProfileConf(basePath + "/conf/rank.conf");

    _tableReader = new TableReader(tableInfo);
    bool rc = _tableReader->init();
    ASSERT_TRUE(rc);
    const IndexReaderManager *readerManager = _tableReader->getIndexReaderManager();
    ASSERT_TRUE(readerManager);
    _readerFacade = new ReaderFacade(readerManager, NULL);
    ASSERT_TRUE(_readerFacade);
    _matchData = new MatchData;
    ASSERT_TRUE(_matchData);
}

void UnpackMatchDataTest::tearDown() { 
    delete _tableReader;
    delete _readerFacade;
    delete _queryExecutor;
    delete _matchData;
}

TEST_F(UnpackMatchDataTest, testUnpackSegPos) { 
    HA3_LOG(DEBUG, "Begin Test!");
    prepareQueryExecutor("IN");
    _matchData->setMaxNumTerms(_queryExecutor->getNumTerms());

    ASSERT_EQ((docid_t)0, _queryExecutor->legacySeek(0));
    _queryExecutor->unpackMatchData(*_matchData);
    ASSERT_EQ((uint32_t)1, _matchData->getNumTerms());
    _matchData->reset();

    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    _queryExecutor->unpackMatchData(*_matchData);
    ASSERT_EQ((uint32_t)1, _matchData->getNumTerms());
    _matchData->reset();

    ASSERT_EQ((docid_t)2, _queryExecutor->legacySeek(2));
    _queryExecutor->unpackMatchData(*_matchData);
    ASSERT_EQ((uint32_t)1, _matchData->getNumTerms());
    _matchData->reset();

    ASSERT_EQ((docid_t)3, _queryExecutor->legacySeek(3));
    _queryExecutor->unpackMatchData(*_matchData);
    ASSERT_EQ((uint32_t)1, _matchData->getNumTerms());
    _matchData->reset();
}

void UnpackMatchDataTest::prepareQueryExecutor(const char *str) {
    Term term(str, "phrase", ALL_FIELDBITS);
    TermQuery termQuery(term);
    ASSERT_TRUE(_readerFacade);
    QueryExecutorCreator qeCreator(_readerFacade);
    termQuery.accept(&qeCreator);
    _queryExecutor = qeCreator.stealQuery();
    ASSERT_TRUE(_queryExecutor);
}

END_HA3_NAMESPACE(search);

