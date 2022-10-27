#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/RankProfile.h>
#include <ha3/search/RankResource.h>
#include <build_service/plugin/PlugInManager.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/config/ResourceReader.h>
#include <autil/mem_pool/Pool.h>
#include <memory>
#include <ha3/test/test.h>
#include <suez/turing/expression/framework/RankAttributeExpression.h>
#include <ha3/rank/test/FakeScorerModuleFactory.h>
#include <suez/turing/common/FileUtil.h>

using namespace autil::legacy;
using namespace std;
using namespace build_service::plugin;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(rank);

class RankProfileTest : public TESTBASE {
public:
    RankProfileTest();
    ~RankProfileTest();
public:
    void setUp();
    void tearDown();
protected:
    TableInfo *createFakeTableInfo();
    RankProfile* createSuccessRankProfile();
    RankProfile* createFailedRankProfile();
protected:
    TableInfo *_tableInfo;
    RankProfile *_rankProfile1;
    RankProfile *_rankProfile2;
    build_service::plugin::PlugInManagerPtr _plugInManagerPtr;
    config::ResourceReaderPtr _resourceReaderPtr;
    autil::mem_pool::Pool *_pool;
    CavaPluginManagerPtr _cavaPluginManagerPtr;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, RankProfileTest);


RankProfileTest::RankProfileTest() { 
    _tableInfo = NULL;
    _rankProfile1 = NULL;
    _rankProfile2 = NULL;
    _pool = new autil::mem_pool::Pool(1024);
}

RankProfileTest::~RankProfileTest() {
    delete _pool;
}

RankProfile* RankProfileTest::createSuccessRankProfile() {
    RankProfileInfo profileInfo;
    profileInfo.rankProfileName = "success";

    profileInfo.scorerInfos.clear();

    ScorerInfo info;
    info.scorerName = "DefaultScorer";
    info.moduleName ="";
    info.rankSize = 1000;
    info.totalRankSize = 2000;
    profileInfo.scorerInfos.push_back(info);
    profileInfo.scorerInfos.push_back(info);//insert the same 'ScorerInfo' twice

    info.scorerName = "FakeDefaultScorer";
    info.moduleName ="mlr";
    info.rankSize = 999;
    info.totalRankSize = 9990;
    profileInfo.scorerInfos.push_back(info);

    //modify the 'FieldBoostTable' in config file.
    profileInfo.fieldBoostInfo.insert(make_pair("defaultIndex.body", 800));
    profileInfo.fieldBoostInfo.insert(make_pair("defaultIndex.description", 90));
    profileInfo.fieldBoostInfo.insert(make_pair("secondIndex.noSuchField", 1111));
    profileInfo.fieldBoostInfo.insert(make_pair("noSuchIndex.title", 2222));

    RankProfile *rankProfile  = new RankProfile(profileInfo);
    return rankProfile;
}

RankProfile* RankProfileTest::createFailedRankProfile() {
    ScorerInfos infos;
    ScorerInfo info;

    info.scorerName = "FakeDefaultScorer";
    info.moduleName ="NotExistModule";
    info.rankSize = 888;
    infos.push_back(info);

    RankProfile *rankProfile  = new RankProfile("failed", infos);
    return rankProfile;
}


void RankProfileTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");

    string basePath = TOP_BUILDDIR;
    string plugConfPath = basePath + "/testdata/conf/plugin.conf";
    string pluginDir = string(TEST_DATA_CONF_PATH_HA3) + "/plugins";
    _resourceReaderPtr.reset(new config::ResourceReader(TEST_DATA_CONF_PATH_HA3));
    _plugInManagerPtr.reset(new PlugInManager(_resourceReaderPtr));

    vector<ModuleInfo> moduleInfos;
    string moduleConfigStr = FileUtil::readFile(plugConfPath);
    autil::legacy::Any any;
    ASSERT_EQ(ResourceReader::JSON_PATH_OK, ResourceReader::getJsonNode(moduleConfigStr, "", any));
    FromJson(moduleInfos, any);
    _plugInManagerPtr->addModules(moduleInfos);

    _tableInfo = createFakeTableInfo();

    _rankProfile1 = createSuccessRankProfile();
    _rankProfile2 = createFailedRankProfile();

    _rankProfile1->mergeFieldBoostTable(*_tableInfo);
    _rankProfile2->mergeFieldBoostTable(*_tableInfo);

    // only for loading symbol for static build
    FakeDefaultScorer fakeScore("aaa");
}

void RankProfileTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");

    DELETE_AND_SET_NULL(_rankProfile1);
    DELETE_AND_SET_NULL(_rankProfile2);
    DELETE_AND_SET_NULL(_tableInfo);
}

TEST_F(RankProfileTest, testCreateRankExprSuccess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_TRUE(_rankProfile1->init(_plugInManagerPtr, _resourceReaderPtr,
                                    _cavaPluginManagerPtr, NULL));
    ASSERT_EQ(3, _rankProfile1->getPhaseCount());
    ASSERT_EQ((uint32_t)1000, _rankProfile1->getRankSize(0));
    ASSERT_EQ((uint32_t)1000, _rankProfile1->getRankSize(1));
    ASSERT_EQ((uint32_t)999, _rankProfile1->getRankSize(2));
    ASSERT_EQ((uint32_t)2000, _rankProfile1->getTotalRankSize(0));
    ASSERT_EQ((uint32_t)2000, _rankProfile1->getTotalRankSize(1));
    ASSERT_EQ((uint32_t)9990, _rankProfile1->getTotalRankSize(2));

    ScorerInfo info;
    ASSERT_TRUE(_rankProfile1->getScorerInfo(2u, info));
    ASSERT_EQ(string("mlr"), info.moduleName);
    ASSERT_EQ(string("FakeDefaultScorer"), info.scorerName);

    RankAttributeExpression *rankExpr = _rankProfile1->createRankExpression(_pool);
    ASSERT_TRUE(rankExpr);
    rankExpr->~RankAttributeExpression();
}

TEST_F(RankProfileTest, testCreateRankExprFailed) { 
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_TRUE(!_rankProfile2->init(_plugInManagerPtr, _resourceReaderPtr,
                    _cavaPluginManagerPtr, NULL));
    ASSERT_EQ(0, _rankProfile1->getPhaseCount());

    RankAttributeExpression *rankExpr = _rankProfile2->createRankExpression(_pool);
    ASSERT_TRUE(!rankExpr);
}

TEST_F(RankProfileTest, testMergedFieldBoostTable) {
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_TRUE(_rankProfile1->init(_plugInManagerPtr, _resourceReaderPtr,
                                    _cavaPluginManagerPtr, NULL));

    
    RankAttributeExpression *rankExpr
        = _rankProfile1->createRankExpression(_pool);
    ASSERT_TRUE(rankExpr);

    const FieldBoostTable *fieldBoostTable = _rankProfile1->getFieldBoostTable();
    ASSERT_TRUE(fieldBoostTable);
    ASSERT_EQ((fieldboost_t)1000, fieldBoostTable->getFieldBoost(0, 0));//defaultIndex.title
    ASSERT_EQ((fieldboost_t)800, fieldBoostTable->getFieldBoost(0, 1));//defaultIndex.body
    ASSERT_EQ((fieldboost_t)90, fieldBoostTable->getFieldBoost(0, 2));//defaultIndex.description
    ASSERT_EQ((fieldboost_t)1234, fieldBoostTable->getFieldBoost(1, 0));//secondIndex.field1

    rankExpr->~RankAttributeExpression();
}

TableInfo* RankProfileTest::createFakeTableInfo() {
    TableInfo *tableInfo = new TableInfo();
    IndexInfos *indexInfos = new IndexInfos();

    IndexInfo *indexInfo1 = new IndexInfo();
    indexInfo1->indexName = "defaultIndex";
    indexInfo1->addField("title", 1000);
    indexInfo1->addField("body", 200);
    indexInfo1->addField("description", 50);
    indexInfos->addIndexInfo(indexInfo1);

    IndexInfo *indexInfo2 = new IndexInfo();
    indexInfo2->indexName = "secondIndex";
    indexInfo2->addField("field1", 1234);
    indexInfos->addIndexInfo(indexInfo2);

    tableInfo->setIndexInfos(indexInfos);
    return tableInfo;
}

TEST_F(RankProfileTest, testCreateBuildInModuleScorerFailed) {
    PlugInManagerPtr plugInManagerPtr;
    RankProfile rankProfile("profileName");

    const string jsonConfigStr = "\
       {\"scorer_name\" : \"mlr_scorer\",\
        \"module_name\" : \"\",\
       \"parameters\" : {\"key\": \"value\" },\
       \"rank_size\" : \"1000\"}";
    ScorerInfo scorerInfo;
    FromJsonString(scorerInfo, jsonConfigStr);
    Scorer* scorer = rankProfile.createScorer(scorerInfo, plugInManagerPtr,
            _resourceReaderPtr, _cavaPluginManagerPtr, NULL);
    ASSERT_TRUE(!scorer);
    if (scorer) {
        scorer->destroy();
    }

    
const string jsonConfigStr2 = "{\
      \"scorer_name\" : \"mlr_scorer\",\
      \"module_name\" : \"BuildInModule\", \
      \"parameters\" : {\"key\": \"value\" },\
      \"rank_size\" : \"1000\"\
      }";
    FromJsonString(scorerInfo, jsonConfigStr2);
    Scorer* scorer2 = rankProfile.createScorer(scorerInfo, plugInManagerPtr,
            _resourceReaderPtr, _cavaPluginManagerPtr, NULL);
    ASSERT_TRUE(!scorer2);
    if (scorer2) {
        scorer2->destroy();
    }
}

TEST_F(RankProfileTest, testCreateBuildInModuleScorerSuccess) {
    PlugInManagerPtr plugInManagerPtr;
    RankProfile rankProfile("profileName");
    {
        const string jsonConfigStr = "{\
      \"scorer_name\" : \"DefaultScorer\", \
      \"module_name\" : \"\",\
      \"parameters\" : {\"key\": \"value\" },\"rank_size\" : \"1000\"\
     } ";
        ScorerInfo scorerInfo;
        FromJsonString(scorerInfo, jsonConfigStr);
        Scorer* scorer = rankProfile.createScorer(scorerInfo, plugInManagerPtr,
                _resourceReaderPtr, _cavaPluginManagerPtr, NULL);
        ASSERT_TRUE(scorer);
        if (scorer) {
            scorer->destroy();
        }
    }
    {
        const string jsonConfigStr = "{\
     \"scorer_name\" : \"DefaultScorer\", \
     \"module_name\" : \"BuildInModule\",\
      \"parameters\" : {\"key\": \"value\" },\"rank_size\" : \"1000\"\
     }"; 
        ScorerInfo scorerInfo;
        FromJsonString(scorerInfo, jsonConfigStr);
        Scorer *scorer = rankProfile.createScorer(scorerInfo, plugInManagerPtr,
                _resourceReaderPtr, _cavaPluginManagerPtr, NULL);
        ASSERT_TRUE(scorer);
        if (scorer) {
            scorer->destroy();
        }
    }
    {
        const string jsonConfigStr = "{\
     \"scorer_name\" : \"_@_build_in_RecordInfoScorer\", \
     \"module_name\" : \"\",\
      \"parameters\" : {\"key\": \"value\" },\"rank_size\" : \"1000\"\
     }"; 
        ScorerInfo scorerInfo;
        FromJsonString(scorerInfo, jsonConfigStr);
        Scorer *scorer = rankProfile.createScorer(scorerInfo, plugInManagerPtr,
                _resourceReaderPtr, _cavaPluginManagerPtr, NULL);
        ASSERT_TRUE(scorer);
        if (scorer) {
            scorer->destroy();
        }
    }
}

TEST_F(RankProfileTest, testGetScorers) {
    RankResource rankResource;
    vector<Scorer*> scorers;
    ASSERT_TRUE(_rankProfile1->init(_plugInManagerPtr, _resourceReaderPtr,
                                    _cavaPluginManagerPtr, NULL));
    _rankProfile1->getScorers(scorers, 0);
    ASSERT_EQ((size_t)3, scorers.size());
    for (size_t i = 0; i < scorers.size(); ++i) {
        delete scorers[i];
    }

    _rankProfile1->getScorers(scorers, 1);
    ASSERT_EQ((size_t)2, scorers.size());
    for (size_t i = 0; i < scorers.size(); ++i) {
        delete scorers[i];
    }

    _rankProfile1->getScorers(scorers, 2);
    ASSERT_EQ((size_t)1, scorers.size());
    for (size_t i = 0; i < scorers.size(); ++i) {
        delete scorers[i];
    }

    _rankProfile1->getScorers(scorers, 3);
    ASSERT_EQ((size_t)0, scorers.size());
}

END_HA3_NAMESPACE(rank);

