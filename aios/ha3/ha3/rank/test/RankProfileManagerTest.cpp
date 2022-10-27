#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <build_service/plugin/PlugInManager.h>
#include <suez/turing/expression/framework/RankAttributeExpression.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/config/ResourceReader.h>
#include <string>
#include <limits>
#include <ha3/test/test.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <suez/turing/expression/util/IndexInfos.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/rank/RankProfileManagerCreator.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>

using namespace std;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
using namespace build_service::plugin;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(rank);

class RankProfileManagerTest : public TESTBASE {
public:
    RankProfileManagerTest();
    ~RankProfileManagerTest();
public:
    void setUp();
    void tearDown();
protected:
    RankAttributeExpression *_rankAttrExpr;
    build_service::plugin::PlugInManagerPtr _plugInManagerPtr;
    config::ResourceReaderPtr _resourceReaderPtr;
    CavaPluginManagerPtr _cavaPluginManagerPtr;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, RankProfileManagerTest);


RankProfileManagerTest::RankProfileManagerTest() { 
    _rankAttrExpr = NULL;
}

RankProfileManagerTest::~RankProfileManagerTest() { 
}

void RankProfileManagerTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _rankAttrExpr = NULL;

    string basePath = TOP_BUILDDIR;
    string plugConfPath = basePath + "/testdata/conf/plugin.conf";
    _resourceReaderPtr.reset(new ResourceReader(TEST_DATA_CONF_PATH_HA3));
    _plugInManagerPtr.reset(new PlugInManager(_resourceReaderPtr));

    vector<ModuleInfo> moduleInfos;
    string moduleConfigStr  = FileUtil::readFile(plugConfPath);
    autil::legacy::Any any;
    ASSERT_EQ(ResourceReader::JSON_PATH_OK, ResourceReader::getJsonNode(moduleConfigStr, "", any));
    FromJson(moduleInfos, any);
    _plugInManagerPtr->addModules(moduleInfos);
}


void RankProfileManagerTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    
    if (_rankAttrExpr) {
        delete _rankAttrExpr;
        _rankAttrExpr = NULL;
    }
}

TEST_F(RankProfileManagerTest, testInitSucess) {
    RankProfileManager rankProfileManager(_plugInManagerPtr);
    RankProfile *rankProfile = new RankProfile("aaa");
    ScorerInfo scorerInfo;
    scorerInfo.scorerName = "FakeDefaultScorer";
    scorerInfo.moduleName = "mlr";
    rankProfile->addScorerInfo(scorerInfo);

    ASSERT_TRUE(rankProfileManager.addRankProfile(rankProfile));
    ASSERT_TRUE(rankProfileManager.init(_resourceReaderPtr, _cavaPluginManagerPtr, NULL));
}

TEST_F(RankProfileManagerTest, testInitFullFail) {
    RankProfileManager rankProfileManager(_plugInManagerPtr);
    RankProfile *rankProfile = new RankProfile("aaa");
    ScorerInfo scorerInfo;
    scorerInfo.scorerName = "NotExistScorer";
    scorerInfo.moduleName = "mlr";
    rankProfile->addScorerInfo(scorerInfo);
    ASSERT_TRUE(rankProfileManager.addRankProfile(rankProfile));

    rankProfile = new RankProfile("initError");
    scorerInfo.scorerName = "FakeDefaultScorer";
    scorerInfo.moduleName = "NotExistModule";
    rankProfile->addScorerInfo(scorerInfo);
    ASSERT_TRUE(rankProfileManager.addRankProfile(rankProfile));

    ASSERT_TRUE(rankProfileManager.getRankProfile("aaa"));
    ASSERT_TRUE(rankProfileManager.getRankProfile("initError"));

    ASSERT_TRUE(!rankProfileManager.init(_resourceReaderPtr, _cavaPluginManagerPtr, NULL));
}

TEST_F(RankProfileManagerTest, testNeedCreateRankProfile) {
    {
        string query = "config=cluster:cluster1,rerank_hint:false&&query=phrase:with&&sort=price";
        RequestPtr requestPtr = RequestCreator::prepareRequest(query);
        ASSERT_TRUE(!RankProfileManager::needCreateRankProfile(requestPtr.get()));
    }
    {
        string query = "config=cluster:cluster1,rerank_hint:true&&query=phrase:with&&sort=price";
        RequestPtr requestPtr = RequestCreator::prepareRequest(query);
        ASSERT_TRUE(RankProfileManager::needCreateRankProfile(requestPtr.get()));
    }
    {
        string query = "config=cluster:cluster1,rerank_hint:false&&query=phrase:with&&sort=price&&rank=rank_profile:DEFAULT";
        RequestPtr requestPtr = RequestCreator::prepareRequest(query);
        ASSERT_TRUE(RankProfileManager::needCreateRankProfile(requestPtr.get()));
    }
    {
        string query = "config=cluster:cluster1,rerank_hint:false&&query=phrase:with&&sort=RANK";
        RequestPtr requestPtr = RequestCreator::prepareRequest(query);
        ASSERT_TRUE(RankProfileManager::needCreateRankProfile(requestPtr.get()));
    }
    {
        string query = "config=cluster:cluster1,rerank_hint:false&&query=phrase:with&&rank_sort=sort:RANK,percent:40;sort:score2,percent:60";
        RequestPtr requestPtr = RequestCreator::prepareRequest(query);
        ASSERT_TRUE(RankProfileManager::needCreateRankProfile(requestPtr.get()));
    }
}

END_HA3_NAMESPACE(rank);

