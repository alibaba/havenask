#include<unittest/unittest.h>
#include <ha3/index/TableReader.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/search/Searcher.h>
#include <ha3/test/test.h>
#include <string>
#include <memory>
#include <build_service/plugin/PlugInManager.h>

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Hits.h>
#include <ha3/common/Result.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/common/Request.h>

using namespace std;
USE_HA3_NAMESPACE(config);
using namespace build_service::plugin;
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);

class Query2SummaryBaseTest : public TESTBASE {
public:
    Query2SummaryBaseTest();
    ~Query2SummaryBaseTest();
public:
    void setUp();
    void tearDown();
protected:
    config::TableInfo prepareTableInfo();
    void initPluginManager();
protected:
    void processRequest(common::Request *request);
    common::Result *_result;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, Query2SummaryBaseTest);


Query2SummaryBaseTest::Query2SummaryBaseTest() { 
    _result = NULL;
}

Query2SummaryBaseTest::~Query2SummaryBaseTest() { 
}

TableInfo Query2SummaryBaseTest::prepareTableInfo() {
    string pathToNewRoot = TEST_DATA_PATH;
    pathToNewRoot += "/newroot";
    TableInfo tableInfo( pathToNewRoot.c_str());

    std::string rankConfPath = TEST_DATA_PATH;
    rankConfPath += "/conf/rank.conf";
    tableInfo.setRankProfileConf(rankConfPath);
    tableInfo.tableName = "simple";
    IndexInfos *indexInfos = new IndexInfos(tableInfo.getPath().c_str());
    IndexInfo *indexInfo = new IndexInfo(indexInfos->getPath().c_str());
    indexInfo->indexName = "phrase";
    indexInfos->addIndexInfo(indexInfo);
    tableInfo.setIndexInfos(indexInfos);
    return tableInfo;
}

void Query2SummaryBaseTest::initPluginManager() {
    std::string plugConfPath = TOP_BUILDDIR;
    plugConfPath += "/testdata/conf/plugin.conf";
    ASSERT_TRUE(PlugInManager::getInstance()->parse(plugConfPath.c_str()));
    PlugInManager::init();
}

void Query2SummaryBaseTest::processRequest(Request *request) {
    initPluginManager();
    TableInfo tableInfo = prepareTableInfo();
    TableReader tableReader(tableInfo);
    ASSERT_TRUE(tableReader.init());
    Searcher searcher(&tableReader);
    _result = searcher.search(request);
    ASSERT_TRUE(_result);
    Hits* hits = _result->getHits();
    ASSERT_TRUE(hits);
    searcher.fillHits(hits);    
}

void Query2SummaryBaseTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void Query2SummaryBaseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    if (_result) {
        delete _result;
    }
    _result= NULL;
    PlugInManager::destroy();
}
END_HA3_NAMESPACE(search);

