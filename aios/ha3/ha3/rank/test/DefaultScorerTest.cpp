#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/index/TableReader.h>
#include <ha3/index/ReaderFacade.h>
#include <ha3/search/QueryExecutor.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/rank/ScoringProvider.h>
#include <ha3/common/MatchDoc.h>
#include <ha3/search/MatchDocAllocator.h>
#include <ha3/rank/DefaultScorer.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/test/test.h>
#include <ha3/rank/DefaultScorer.h>
#include <ha3/index/TermMatchData.h>
#include <suez/turing/expression/util/IndexInfos.h>
#include <ha3/index/TableReader.h>
#include <ha3/index/GlobalMatchData.h>
#include <ha3/rank/MatchData.h>
#include <ha3/common/TermQuery.h>
#include <ha3/search/TermQueryExecutor.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <memory>
#include <string>

using namespace std;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(rank);

class DefaultScorerTest : public TESTBASE {
public:
    DefaultScorerTest();
    ~DefaultScorerTest();
public:
    void setUp();
    void tearDown();
protected:
    void prepareQueryExecutor(const char *str);
    
protected:
    autil::mem_pool::Pool _pool;
    index::TableReader *_tableReader;
    index::ReaderFacade *_readerFacade;
    search::QueryExecutor *_queryExecutor;
    config::TableInfo *_tableInfo;
    config::IndexInfos *_indexInfos;
    index::GlobalMatchData *_globalMatchData;

    ScoringProvider *_provider;
    common::MatchDoc *_matchDoc1;
    common::MatchDoc *_matchDoc2;
    search::MatchDocAllocator *_allocator;
    
    DefaultScorer *_scorer;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, DefaultScorerTest);


DefaultScorerTest::DefaultScorerTest() { 
    _tableReader = NULL;
    _readerFacade = NULL;
    _queryExecutor = NULL;
    _tableInfo = NULL;
    _indexInfos = NULL;
    _globalMatchData = NULL;
    _provider = NULL;
    _matchDoc1 = NULL;
    _matchDoc2 = NULL;
    _allocator = NULL;
    _scorer = NULL;
}

DefaultScorerTest::~DefaultScorerTest() { 
}

void DefaultScorerTest::setUp() {
    std::string basePath = TEST_DATA_PATH;
    std::string rootPath = basePath + "/newroot";

    _tableInfo = new TableInfo(rootPath.c_str());
    _tableInfo->tableName = "simple";
    _indexInfos = new IndexInfos(_tableInfo->getPath().c_str());
    IndexInfo *indexInfo = new IndexInfo(_indexInfos->getPath().c_str());
    indexInfo->indexName = "phrase";
    indexInfo->addFieldBit("TITLE", 10000);
    indexInfo->addFieldBit("BODY", 2000);
    indexInfo->addFieldBit("DESCRIPTION", 1000);
    _indexInfos->addIndexInfo(indexInfo);
    _tableInfo->setIndexInfos(_indexInfos);
    _tableInfo->setRankProfileConf(basePath + "/conf/rank.conf");
    
    _tableReader = new TableReader(*_tableInfo);
    bool rc = _tableReader->init();
    ASSERT_TRUE(rc);
    const IndexReaderManager *readerManager = _tableReader->getIndexReaderManager();
    ASSERT_TRUE(readerManager);
    _readerFacade = new ReaderFacade(readerManager, NULL);

    _allocator = new MatchDocAllocator(&_pool);
    prepareQueryExecutor("ALIBABA");
    _globalMatchData  = new GlobalMatchData(_indexInfos, 2);
    _provider = new ScoringProvider(_allocator, NULL, _queryExecutor, _globalMatchData);
    _matchDoc1 = NULL;
    _matchDoc2 = NULL;
    
    _scorer = new DefaultScorer("default");
}

void DefaultScorerTest::tearDown() { 
    if (_tableReader) {
        delete _tableReader;
        _tableReader = NULL;
    }
    if (_readerFacade) {
        delete _readerFacade;
        _readerFacade = NULL;
    }
    if (_queryExecutor) {
        delete _queryExecutor;
        _queryExecutor = NULL;
    }
    if (_matchDoc1) {
        _allocator->deallocateMatchDoc(_matchDoc1);
        _matchDoc1 = NULL;
    }
    if (_matchDoc2) {
        _allocator->deallocateMatchDoc(_matchDoc2);
        _matchDoc2 = NULL;
    }
    if (_globalMatchData) {
        delete _globalMatchData;
        _globalMatchData = NULL;
    }
    if (_tableInfo) {
        delete _tableInfo;
        _tableInfo = NULL;
    }
    if (_provider) {    
        delete _provider;
        _provider = NULL;
    }
    if (_allocator) {
        delete _allocator;
        _allocator = NULL;
    }
    if (_scorer) {
        _scorer->destroy();
        //delete _scorer;
        _scorer = NULL;
    }
}

TEST_F(DefaultScorerTest, testScore) { 
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_TRUE(_scorer->beginRequest(_provider, NULL));
    _matchDoc1 = _allocator->allocateMatchDoc((docid_t)0);
    ASSERT_EQ((docid_t)0, _queryExecutor->legacySeek(0));
    _provider->prepareMatchDoc(_matchDoc1);
    score_t score = _scorer->score(_matchDoc1);
    ASSERT_TRUE_DOUBLES_EQUAL((float)338.333, score, 0.001);
    _scorer->endRequest();

    _matchDoc2 = _allocator->allocateMatchDoc((docid_t)1);
    ASSERT_EQ((docid_t)1, _queryExecutor->legacySeek(1));
    _provider->prepareMatchDoc(_matchDoc2);
    score = _scorer->score(_matchDoc2);
    ASSERT_TRUE_DOUBLES_EQUAL((float)334.333, score, 0.001);
    _scorer->endRequest();
}

void DefaultScorerTest::prepareQueryExecutor(const char *str) {
    Term term(str, "phrase", ALL_FIELDBITS);
    TermQuery termQuery(term);
    QueryExecutorCreator qeCreator(_readerFacade);
    termQuery.accept(&qeCreator);
    _queryExecutor = qeCreator.stealQuery();
    ASSERT_TRUE(_queryExecutor);
}

END_HA3_NAMESPACE(rank);

