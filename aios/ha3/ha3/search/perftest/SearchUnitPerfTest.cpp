#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/Comparator.h>
#include <ha3/index/TableReader.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/queryparser/QueryParser.h>
#include <ha3/common/Term.h>
#include <ha3/common/TermQuery.h>
#include <ha3/index/TableReader.h>
#include <ha3/search/Searcher.h>
#include <ha3/common/Hits.h>
#include <ha3/test/test.h>
#include <string>
#include <memory>
#include <ha3/config/DatabaseConfigurator.h>
#include <ha3/rank/DistinctHitCollector.h>
#include <ha3/common/Request.h>
#include <build_service/plugin/PlugInManager.h>
#include <autil/TimeUtility.h>

using namespace std;
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
using namespace build_service::plugin;
USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(index);

BEGIN_HA3_NAMESPACE(search);
class Searcher;
class Hits;
class Request;
class SearchUnitPerfTest : public TESTBASE {
public:
    SearchUnitPerfTest();
    ~SearchUnitPerfTest();
public:
    void setUp();
    void tearDown();
protected:
    Query* parseQuery(const char*query, const char *msg);
protected:
    Searcher *_searcher;
    index::TableReader *_tableReader;
//    config::DatabaseInfo *_dbInfo;
    const config::TableInfo *_tableInfo;
    queryparser::QueryParser *_queryParser;
    queryparser::ParserContext *_ctx;
    Hits *_hits;
    Query *_query;
    Request *_request;
protected:
    HA3_LOG_DECLARE();
};

#define PARSE_QUERY(q) parseQuery(q, __FUNCTION__)

HA3_LOG_SETUP(search, SearchUnitPerfTest);

SearchUnitPerfTest::SearchUnitPerfTest() { 
    _searcher = NULL;
    _tableReader = NULL;
    _tableInfo = NULL;

    _queryParser = NULL;
    _dbInfo = NULL;
    _ctx = NULL;
    _hits = NULL;
    _query = NULL;
    _request = NULL;
}

SearchUnitPerfTest::~SearchUnitPerfTest() { 
}

void SearchUnitPerfTest::setUp() { 
    HA3_LOG(DEBUG, "SetUp!");

    DatabaseConfigurator dbConfig;
    std::string confFile = TOP_BUILDDIR;
    _dbInfo = dbConfig.parse(confFile + "/testdata/many.se.conf");
    ASSERT_TRUE(_dbInfo);

    _tableInfo = _dbInfo->getTableInfo("many");
    ASSERT_TRUE(_tableInfo);
    _tableReader = new TableReader(*_tableInfo);
    bool rc = _tableReader->init();
    ASSERT_TRUE(rc);

    _queryParser = new QueryParser(_tableInfo, "phrase");
    _request = new Request;
    _request->rerankSize = 0;
    _request->rankProfile = "DefaultProfile";

    
    PlugInManager *plugInMgr = PlugInManager::getInstance();
    ModuleInfo moduInfo;
    moduInfo.moduleName = "multiphase";
    moduInfo.modulePath = confFile + "/src/search/test/libmultiphasescorer.so";

    std::string plugConfPath = confFile + "/testdata/conf/plugin.conf";
    ASSERT_TRUE(PlugInManager::getInstance()->parse(plugConfPath.c_str()));
    plugInMgr->addModules(moduInfo);
    PlugInManager::init();

    _searcher = new Searcher(_tableReader);
    ASSERT_TRUE(_searcher);
}

void SearchUnitPerfTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete _dbInfo;
    delete _searcher;
    delete _tableReader;
    delete _queryParser;
    delete _ctx;
    _ctx = NULL;
    delete _hits;
    _hits = NULL;
    delete _query;
    _query = NULL;
    delete _request;
    _request = NULL;
    PlugInManager::destroy();
}

TEST_F(SearchUnitPerfTest, testSearchPerf) {
    _request->query = PARSE_QUERY("phrase:with");

    const int32_t CYCLE_COUNT = 100;
    const int32_t TIME_COST_PER_QUERY = 1600/*3000*/;
    const int32_t DELTA_COST_PER_QUERY = 1600/*200*/;

    uint64_t startTime = TimeUtility::currentTime();
    for (int32_t i = 0; i < CYCLE_COUNT; i++) {
        Hits *hits = _searcher->search(_request);
        delete hits;
    }
    uint64_t endTime = TimeUtility::currentTime();
    ASSERT_TRUE_DOUBLES_EQUAL(CYCLE_COUNT * TIME_COST_PER_QUERY, 
                                 endTime - startTime,
                                 CYCLE_COUNT * DELTA_COST_PER_QUERY);
}

Query* SearchUnitPerfTest::parseQuery(const char* query, const char*msg) { 
    _ctx = _queryParser->parse(query);
    ASSERT_TRUE_MESSAGE(msg, _ctx);
    ASSERT_EQ_MESSAGE(msg, ParserContext::OK, _ctx->getStatus()); 
    ASSERT_TRUE_MESSAGE(msg, _ctx->getQuery());
    return _ctx->stealQuery();
}

END_HA3_NAMESPACE(search);

