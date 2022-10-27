#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/ConfigAdapter.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/test/test.h>
#include <autil/legacy/json.h>
#include <autil/legacy/jsonizable.h>
#include <indexlib/partition/index_partition.h>
#include <ha3/config/FuncConfig.h>
#include <sap/util/environ_util.h>

using namespace suez::turing;
using namespace std;
using namespace fslib::fs;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(config);

class ConfigAdapterTest : public TESTBASE {
public:
    ConfigAdapterTest();
    ~ConfigAdapterTest();
public:
    void setUp();
    void tearDown();
protected:
    ConfigAdapterPtr _configAdapterPtr;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, ConfigAdapterTest);


ConfigAdapterTest::ConfigAdapterTest() {
}

ConfigAdapterTest::~ConfigAdapterTest() {
}

void ConfigAdapterTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    string testPath = TEST_DATA_PATH"/FsConfig/testCopyDir/";
    string baseDir = testPath + "exist_destination/";

    FileSystem::mkDir(baseDir);

    string configRoot = TEST_DATA_CONF_PATH_VERSION(1);
    _configAdapterPtr.reset(new ConfigAdapter(configRoot));

}

void ConfigAdapterTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    string testPath = TEST_DATA_PATH"/FsConfig/testCopyDir/";
    string baseDir = testPath + "exist_destination/";

    FileSystem::remove(baseDir);
}

TEST_F(ConfigAdapterTest, testGetTableInfo) {
    HA3_LOG(DEBUG, "Begin Test!");

    TableInfoPtr tableInfoPtr;
    ASSERT_TRUE(tableInfoPtr == NULL);
    ASSERT_TRUE(_configAdapterPtr->getTableInfo("auction", tableInfoPtr));
    ASSERT_TRUE(tableInfoPtr != NULL);
    ASSERT_TRUE(!_configAdapterPtr->getTableInfo("not_exist", tableInfoPtr));
}

TEST_F(ConfigAdapterTest, testGetClusterNames) {
    HA3_LOG(DEBUG, "Begin Test: testGetClusterNames");

    vector<string> clusterNames;
    ASSERT_TRUE(_configAdapterPtr->getClusterNames(clusterNames));
    ASSERT_EQ((size_t)6, clusterNames.size());
    string defaultSuffix = ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;
    vector<string>::iterator iter;
    iter = find(clusterNames.begin(), clusterNames.end(),string("auction" + defaultSuffix));
    ASSERT_TRUE(iter != clusterNames.end());
    iter = find(clusterNames.begin(), clusterNames.end(),string("auction_summary_so" + defaultSuffix));
    ASSERT_TRUE(iter != clusterNames.end());
    iter = find(clusterNames.begin(), clusterNames.end(),string("offer_gb" + defaultSuffix));
    ASSERT_TRUE(iter != clusterNames.end());
    iter = find(clusterNames.begin(), clusterNames.end(),string("mainse_searcher" + defaultSuffix));
    ASSERT_TRUE(iter != clusterNames.end());
    iter = find(clusterNames.begin(), clusterNames.end(),string("company" + defaultSuffix));
    ASSERT_TRUE(iter != clusterNames.end());
    vector<string> turingClusterNames;
    ASSERT_TRUE(_configAdapterPtr->getTuringClusterNames(turingClusterNames));
    ASSERT_EQ((size_t)0, turingClusterNames.size());
}

TEST_F(ConfigAdapterTest, testGetTuringClusterNames) {
    string configRoot = TEST_DATA_CONF_PATH_VERSION(turing);
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));
    vector<string> clusterNames;
    ASSERT_TRUE(configAdapterPtr->getTuringClusterNames(clusterNames));
    ASSERT_EQ((size_t)2, clusterNames.size());
    sort(clusterNames.begin(), clusterNames.end());
    ASSERT_EQ("offer_gb.user_graph_search", clusterNames[0]);
    ASSERT_EQ("turing_1.user_graph_search", clusterNames[1]);
}

TEST_F(ConfigAdapterTest, testReadConfigFile) {
    HA3_LOG(DEBUG, "Begin Test: testReadConfigFile");

    string result;
    string relativePath;
    string sectionName;

    relativePath = string(CLUSTER_CONFIG_DIR_NAME)  + "/auction/" +DEFAULT_BIZ_NAME +
                   CLUSTER_JSON_FILE_SUFFIX;
    ASSERT_TRUE(result.empty());
    ASSERT_TRUE(_configAdapterPtr->readConfigFile(result, relativePath));
    ASSERT_TRUE(!result.empty());
}

TEST_F(ConfigAdapterTest, testGetEmptyFunctionConfig) {
    HA3_LOG(DEBUG, "Begin Test!");

    string configRoot = TEST_DATA_PATH;
    configRoot += "/configuration";
    ConfigAdapter configAdapter(configRoot);

    FuncConfig fc;
    bool ret = configAdapter.getFuncConfig("auction", fc);
    ASSERT_TRUE(ret);
    ASSERT_EQ(size_t(0), fc._functionInfos.size());
}

TEST_F(ConfigAdapterTest, testGetFunctionConfig) {
    HA3_LOG(DEBUG, "Begin Test!");

    string configRoot = TEST_DATA_CONF_PATH;
    configRoot += "/configurations/configuration_1/";

    ConfigAdapter configAdapter(configRoot);

    FuncConfig fc;
    bool ret = configAdapter.getFuncConfig("auction", fc);
    ASSERT_TRUE(ret);
    ASSERT_EQ(size_t(2), fc._functionInfos.size());
}

TEST_F(ConfigAdapterTest, testGetClusterConfigInfo) {
    HA3_LOG(DEBUG, "Begin Test!");

    ClusterConfigInfo clusterConfigInfo;
    ASSERT_TRUE(_configAdapterPtr->getClusterConfigInfo("auction", clusterConfigInfo));

    ASSERT_EQ(string("auction"), clusterConfigInfo._tableName);

    ASSERT_EQ(string("HASH"), clusterConfigInfo._hashMode._hashFunction);
    ASSERT_EQ(string("uri"), clusterConfigInfo._hashMode._hashField);

    ASSERT_EQ(string("phrase"), clusterConfigInfo._queryInfo.getDefaultIndexName());
    ASSERT_TRUE(OP_AND == clusterConfigInfo._queryInfo.getDefaultOperator());
    ASSERT_TRUE(false == clusterConfigInfo._queryInfo.getDefaultMultiTermOptimizeFlag());

    const JoinConfig &joinConfig = clusterConfigInfo.getJoinConfig();
    JoinConfig expectConfig;
    expectConfig.addJoinInfo(JoinConfigInfo(false, "company", "company_id", false));
    ASSERT_TRUE(expectConfig == joinConfig);
}

TEST_F(ConfigAdapterTest, testGetNullTuringOptionsInfo) {
    HA3_LOG(DEBUG, "Begin Test!");

    TuringOptionsInfo turingOptionsInfo;
    ASSERT_TRUE(_configAdapterPtr->getTuringOptionsInfo("auction", turingOptionsInfo));
    ASSERT_EQ(0u, turingOptionsInfo._turingOptionsInfo.size());
}

TEST_F(ConfigAdapterTest, testGetTuringOptionsInfo) {
    HA3_LOG(DEBUG, "Begin Test!");
    string configRoot = TEST_DATA_CONF_PATH_VERSION(5);
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));
    ASSERT_TRUE(configAdapterPtr->check());

    TuringOptionsInfo turingOptionsInfo;
    ASSERT_TRUE(configAdapterPtr->getTuringOptionsInfo("auction", turingOptionsInfo));

    string graphConfigPath;
    map <string, vector<string> > dependencyTable;
    ASSERT_TRUE(turingOptionsInfo._turingOptionsInfo.find("dependency_table") != turingOptionsInfo._turingOptionsInfo.end());
    autil::legacy::FromJson(dependencyTable, turingOptionsInfo._turingOptionsInfo["dependency_table"]);
    ASSERT_EQ(1, dependencyTable.size());
    ASSERT_TRUE(dependencyTable.find("mainse_searcher_search") != dependencyTable.end());
    vector<string> mainSearchTable = dependencyTable["mainse_searcher_search"];
    ASSERT_EQ(3, mainSearchTable.size());
    ASSERT_EQ(string("table1"), mainSearchTable[0]);
    ASSERT_EQ(string("table2"), mainSearchTable[1]);
    ASSERT_EQ(string("table3"), mainSearchTable[2]);

    ASSERT_TRUE(turingOptionsInfo._turingOptionsInfo.find("graph_config_path") != turingOptionsInfo._turingOptionsInfo.end());
    autil::legacy::FromJson(graphConfigPath, turingOptionsInfo._turingOptionsInfo["graph_config_path"]);
    ASSERT_EQ(string("search_graph.pbtxt"), graphConfigPath);
}

TEST_F(ConfigAdapterTest, testInvalidHashFunc) {
    HA3_LOG(DEBUG, "Begin Test!");
    string configRoot = TEST_DATA_CONF_PATH_VERSION(invaid_hash);
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));
    ASSERT_FALSE(configAdapterPtr->check());
    ClusterConfigInfo clusterConfigInfo;
    ASSERT_TRUE(configAdapterPtr->getClusterConfigInfo("auction", clusterConfigInfo));
    ASSERT_EQ(string("invalid_hash"), clusterConfigInfo._hashMode._hashFunction);
    ASSERT_EQ(string("uri"), clusterConfigInfo._hashMode._hashField);
    ClusterConfigMapPtr clusterConfigMapPtr;
    clusterConfigMapPtr = configAdapterPtr->getClusterConfigMap();
    ASSERT_TRUE(clusterConfigMapPtr == nullptr);
}

TEST_F(ConfigAdapterTest, testRoutingHashFunc) {
    HA3_LOG(DEBUG, "Begin Test!");
    string configRoot = TEST_DATA_CONF_PATH_VERSION(routing_hash);
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));
    ClusterConfigInfo clusterConfigInfo;
    ASSERT_TRUE(configAdapterPtr->getClusterConfigInfo("auction", clusterConfigInfo));
    ASSERT_EQ(string("ROUTING_HASH"), clusterConfigInfo._hashMode._hashFunction);
    ASSERT_EQ(string("uri"), clusterConfigInfo._hashMode._hashField);
    ASSERT_TRUE(clusterConfigInfo._hashFunctionBasePtr == nullptr);
    ClusterConfigMapPtr clusterConfigMapPtr;
    clusterConfigMapPtr = configAdapterPtr->getClusterConfigMap();
    ASSERT_TRUE(clusterConfigMapPtr);
    ASSERT_TRUE((*clusterConfigMapPtr)["auction.default"]._hashFunctionBasePtr != nullptr);
}

TEST_F(ConfigAdapterTest, testGetClusterConfigMap) {
    HA3_LOG(DEBUG, "Begin Test!");

    ClusterConfigInfo clusterConfigInfo;
    ASSERT_TRUE(_configAdapterPtr->getClusterConfigInfo("auction_bucket",
                    clusterConfigInfo));

    string defaultSuffix = ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;
    ClusterConfigMapPtr clusterConfigMapPtr;
    clusterConfigMapPtr = _configAdapterPtr->getClusterConfigMap();
    ASSERT_TRUE(clusterConfigMapPtr);
    ASSERT_EQ((size_t)24, clusterConfigMapPtr->size());
    ASSERT_TRUE(clusterConfigMapPtr->find("auction" + defaultSuffix) != clusterConfigMapPtr->end());
    auto abDefaultIter = clusterConfigMapPtr->find("auction_bucket" + defaultSuffix);
    ASSERT_TRUE(abDefaultIter != clusterConfigMapPtr->end());
    auto paraIter2 = clusterConfigMapPtr->find("auction_bucket.para_search_2");
    ASSERT_TRUE(paraIter2 != clusterConfigMapPtr->end());
    ASSERT_EQ(ToJsonString(abDefaultIter->second), ToJsonString(paraIter2->second));
    auto paraIter4 = clusterConfigMapPtr->find("auction_bucket.para_search_4");
    ASSERT_TRUE(paraIter4 != clusterConfigMapPtr->end());
    ASSERT_EQ(ToJsonString(abDefaultIter->second), ToJsonString(paraIter4->second));
}

TEST_F(ConfigAdapterTest, testGetSearcherCacheConfig) {
    HA3_LOG(DEBUG, "Begin Test!");
    SearcherCacheConfig cacheConfig;

    ASSERT_TRUE(_configAdapterPtr->getSearcherCacheConfig("auction",
                    cacheConfig));

    ASSERT_EQ((uint32_t)2048, cacheConfig.maxSize);
    ASSERT_EQ((uint32_t)400000, cacheConfig.maxItemNum);
    ASSERT_EQ((uint32_t)2000, cacheConfig.incDocLimit);
    ASSERT_EQ((uint32_t)10, cacheConfig.incDeletionPercent);
    ASSERT_EQ((float)2.0, cacheConfig.latencyLimitInMs);
    ASSERT_EQ((uint32_t)0, cacheConfig.minAllowedCacheDocNum);
    ASSERT_EQ((uint32_t)100000, cacheConfig.maxAllowedCacheDocNum);

    // test not exist cluster
    ASSERT_TRUE(!_configAdapterPtr->getSearcherCacheConfig(
                    "not_exist_cluster", cacheConfig));

    // test not has searcher_cache_config section
    ASSERT_TRUE(!_configAdapterPtr->getSearcherCacheConfig(
                    "aution_bucket", cacheConfig));

    // test has searcher_cache_config section, but no content
    // TODO: allow empty
    ASSERT_TRUE(_configAdapterPtr->getSearcherCacheConfig(
                    "auction_summary_so", cacheConfig));
}

TEST_F(ConfigAdapterTest, testServiceDegradationConfig) {
    string configPath = string(TEST_DATA_PATH) + "/normal_full_config/";
    _configAdapterPtr.reset(new ConfigAdapter(configPath));
    ServiceDegradationConfig config;
    ASSERT_TRUE(_configAdapterPtr->getServiceDegradationConfig("service_degradation", config));
    ASSERT_EQ(uint32_t(100), config.condition.workerQueueSizeDegradeThreshold);
    ASSERT_EQ(uint32_t(10), config.condition.workerQueueSizeRecoverThreshold);
    ASSERT_EQ(int64_t(1000), config.condition.duration);
    ASSERT_EQ(uint32_t(5000), config.request.rankSize);
    ASSERT_EQ(uint32_t(200), config.request.rerankSize);
}

TEST_F(ConfigAdapterTest, testGetClusterTableInfoManagerMap) {
    ClusterTableInfoManagerMapPtr clusterTableInfoManagerMapPtr =
        _configAdapterPtr->getClusterTableInfoManagerMap();
    ASSERT_TRUE(clusterTableInfoManagerMapPtr);
    ASSERT_EQ(size_t(6), clusterTableInfoManagerMapPtr->size());
    string defaultSuffix = ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;
    ClusterTableInfoManagerPtr clusterTableInfoManagerPtr =
        clusterTableInfoManagerMapPtr->getClusterTableInfoManager("auction" + defaultSuffix);
    ASSERT_TRUE(clusterTableInfoManagerPtr);
    TableInfoPtr mainTableInfoPtr = clusterTableInfoManagerPtr->getMainTableInfo();
    ASSERT_TRUE(mainTableInfoPtr);
    ASSERT_EQ(std::string("auction"), mainTableInfoPtr->getTableName());
    ClusterTableInfoManager::JoinTableInfos  joinTableInfos =
        clusterTableInfoManagerPtr->getJoinTableInfos();
    ASSERT_EQ(size_t(1), joinTableInfos.size());
    ASSERT_EQ(std::string("company_id"), joinTableInfos["company"].first.getJoinField());
    ASSERT_EQ(std::string("company"),
                         joinTableInfos["company"].second->getTableName());

}

TEST_F(ConfigAdapterTest, testGetTableMap) {
    const TableMapPtr& tableMapPtr = _configAdapterPtr->getTableMap();
    ASSERT_EQ(size_t(2), tableMapPtr->size());
    ASSERT_TRUE(tableMapPtr->find("auction") != tableMapPtr->end());
    ASSERT_TRUE(tableMapPtr->find("company") != tableMapPtr->end());
}

TEST_F(ConfigAdapterTest, testGetTableInfoWithClusterName) {
    {
        TableInfoPtr tableInfoPtr =
            _configAdapterPtr->getTableInfoWithClusterName("auction", true);
        ASSERT_TRUE(tableInfoPtr);
    }
    {
        TableInfoPtr tableInfoPtr =
            _configAdapterPtr->getTableInfoWithClusterName("no_exist_cluster", true);
        ASSERT_TRUE(!tableInfoPtr);
    }
    {
        TableInfoPtr tableInfoPtr =
            _configAdapterPtr->getTableInfoWithClusterName("no_exist_cluster", false);
        ASSERT_TRUE(!tableInfoPtr);
    }
    {
        TableInfoPtr tableInfoPtr =
            _configAdapterPtr->getTableInfoWithClusterName("auction_no_zone", false);
        ASSERT_TRUE(tableInfoPtr);
    }
}

TEST_F(ConfigAdapterTest, testGetJoinClusters) {
    vector<string> joinClusters = _configAdapterPtr->getJoinClusters("auction");
    ASSERT_EQ(size_t(1), joinClusters.size());
    ASSERT_EQ(string("company"), joinClusters[0]);
}

TEST_F(ConfigAdapterTest, testGetClusterTypeWithClusterName) {

}

TEST_F(ConfigAdapterTest, testGetClusterTableInfoManager) {
    {
        ClusterTableInfoManagerPtr clusterTableInfoManagerPtr =
            _configAdapterPtr->getClusterTableInfoManager("auction");
        ASSERT_TRUE(clusterTableInfoManagerPtr);
    }
    {
        ClusterTableInfoManagerPtr clusterTableInfoManagerPtr =
            _configAdapterPtr->getClusterTableInfoManager("no_exist_cluster");
        ASSERT_TRUE(!clusterTableInfoManagerPtr);
    }

}

TEST_F(ConfigAdapterTest, testGetClusterTableInfoMap) {
    string defaultSuffix = ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;
    const ClusterTableInfoMapPtr& tableMapPtr = _configAdapterPtr->getClusterTableInfoMap();
    ASSERT_TRUE(tableMapPtr);
    ASSERT_EQ(size_t(24), tableMapPtr->size());
    ASSERT_TRUE(tableMapPtr->find("auction" + defaultSuffix) != tableMapPtr->end());
    ASSERT_TRUE(tableMapPtr->find("auction_summary_so" + defaultSuffix) != tableMapPtr->end());
    ASSERT_TRUE(tableMapPtr->find("company" + defaultSuffix) != tableMapPtr->end());
    ASSERT_TRUE(tableMapPtr->find("mainse_searcher" + defaultSuffix) != tableMapPtr->end());
    ASSERT_TRUE(tableMapPtr->find("offer_gb" + defaultSuffix) != tableMapPtr->end());
    ASSERT_TRUE(tableMapPtr->find("auction_bucket" + defaultSuffix) != tableMapPtr->end());
    auto aucIter = tableMapPtr->find("auction"
            + ZONE_BIZ_NAME_SPLITTER + HA3_DEFAULT_AGG);
    ASSERT_TRUE(aucIter != tableMapPtr->end());
    auto paraIter2 = tableMapPtr->find("auction.para_search_2");
    ASSERT_TRUE(paraIter2 != tableMapPtr->end());
    ASSERT_EQ(aucIter->second, paraIter2->second);
    auto paraIter4 = tableMapPtr->find("auction.para_search_4");
    ASSERT_TRUE(paraIter4 != tableMapPtr->end());
    ASSERT_EQ(aucIter->second, paraIter4->second);
}

TEST_F(ConfigAdapterTest, testCheck) {
    string configRoot = TEST_DATA_CONF_PATH_VERSION(4);
    ConfigAdapterPtr configAdapterPtr(new ConfigAdapter(configRoot));
    ASSERT_TRUE(!configAdapterPtr->check());

    configRoot = TEST_DATA_CONF_PATH_VERSION(5);
    configAdapterPtr.reset(new ConfigAdapter(configRoot));
    ASSERT_TRUE(configAdapterPtr->check());

    // not realtime cluster
    configRoot = TEST_DATA_CONF_PATH_VERSION(7);
    configAdapterPtr.reset(new ConfigAdapter(configRoot));
    ASSERT_TRUE(configAdapterPtr->check());
}

TEST_F(ConfigAdapterTest, testGetVersionConfig) {
    {
        ConfigAdapter config(TEST_DATA_CONF_PATH_VERSION(1));
        EXPECT_EQ("", config.getVersionConfig().getProtocolVersion());
        EXPECT_EQ("", config.getVersionConfig().getDataVersion());
    }
    {
        ConfigAdapter config(TEST_DATA_CONF_PATH_VERSION(2));
        EXPECT_EQ("", config.getVersionConfig().getProtocolVersion());
        EXPECT_EQ("", config.getVersionConfig().getDataVersion());
    }
    {
        ConfigAdapter config(TEST_DATA_CONF_PATH_VERSION(3));
        EXPECT_EQ("config_version_3", config.getVersionConfig().getProtocolVersion());
        EXPECT_EQ("config_data_3", config.getVersionConfig().getDataVersion());
    }
    {
        ConfigAdapter config(TEST_DATA_CONF_PATH_VERSION(4));
        EXPECT_EQ("", config.getVersionConfig().getProtocolVersion());
        EXPECT_EQ("", config.getVersionConfig().getDataVersion());
    }
}

TEST_F(ConfigAdapterTest, testGetAnomalyConfig) {
    {
        ConfigAdapter configAdapter(TEST_DATA_CONF_PATH_VERSION(3));
        {
            AnomalyProcessConfig config;
            ASSERT_TRUE(configAdapter.getAnomalyProcessConfig("company", config));
            EXPECT_FALSE(config.empty());
            EXPECT_EQ("10", ToJsonString(config["begin_degrade_latency"]));
        }
        {
            AnomalyProcessConfigT configT;
            ASSERT_TRUE(configAdapter.getAnomalyProcessConfigT("company", configT));
            EXPECT_FALSE(configT.empty());
            EXPECT_EQ("5", ToJsonString(configT["full_degrade_latency"]));
        }
    }
    {
        ConfigAdapter configAdapter(TEST_DATA_CONF_PATH_VERSION(3));
        {
            AnomalyProcessConfig config;
            ASSERT_TRUE(configAdapter.getAnomalyProcessConfig("auction", config));
            EXPECT_TRUE(config.empty());
        }
        {
            AnomalyProcessConfigT configT;
            ASSERT_FALSE(configAdapter.getAnomalyProcessConfigT("auction", configT));
            EXPECT_TRUE(configT.empty());
        }
    }
}

TEST_F(ConfigAdapterTest, testGetTuringFlowConfig) {
    {
        ConfigAdapter configAdapter(TEST_DATA_CONF_PATH_VERSION(turing));
        {
            map<string, autil::legacy::json::JsonMap> flowConfigs;
            ASSERT_TRUE(configAdapter.getTuringFlowConfigs("turing_1.user_graph_search", flowConfigs));
            EXPECT_EQ(2, flowConfigs.size());
            auto iter = flowConfigs.begin();
            ASSERT_EQ("__default_multicall_config__", iter->first);
            iter++;
            ASSERT_EQ("graph_search.user_graph_search", iter->first);
        }
        {
            map<string, autil::legacy::json::JsonMap> flowConfigs;
            ASSERT_TRUE(configAdapter.getTuringFlowConfigs("offer_gb.user_graph_search", flowConfigs));
            EXPECT_EQ(1, flowConfigs.size());
            auto iter = flowConfigs.begin();
            ASSERT_EQ("__default_multicall_config__", iter->first);
        }
    }
}

TEST_F(ConfigAdapterTest, testGetClusterConfigFileName) {
    ConfigAdapter config("test");
    string retPath;

    retPath = config.getClusterConfigFileName("daogou");
    ASSERT_EQ("test/zones/daogou/default_biz.json", retPath);

    retPath = config.getClusterConfigFileName("para_search_4");
    ASSERT_EQ("test/zones/para_search_4/default_biz.json", retPath);

    retPath = config.getClusterConfigFileName("daogou.para_search_4");
    ASSERT_EQ("test/zones/daogou/default_biz.json", retPath);

    retPath = config.getClusterConfigFileName("daogou.good");
    ASSERT_EQ("test/zones/daogou/good_biz.json", retPath);
}

TEST_F(ConfigAdapterTest, testGetClusterNamesWithExtendBizs) {
    const string &oldPsw = sap::EnvironUtil::getEnv("paraSearchWays", "");
    sap::EnvironUtil::setEnv("paraSearchWays", "");
    {
        std::vector<std::string> clusterNames;
        ASSERT_TRUE(_configAdapterPtr->getClusterNames(clusterNames));
        ASSERT_EQ(6u, clusterNames.size());
        std::sort(clusterNames.begin(), clusterNames.end());
        std::vector<std::string> expected{
            "auction.default",
            "auction_bucket.default",
            "auction_summary_so.default",
            "company.default",
            "mainse_searcher.default",
            "offer_gb.default"};
        ASSERT_EQ(expected, clusterNames);
    }
    {
        std::vector<std::string> clusterNamesWithExtendBizs;
        ASSERT_TRUE(_configAdapterPtr->getClusterNamesWithExtendBizs(clusterNamesWithExtendBizs));
        ASSERT_EQ(18u, clusterNamesWithExtendBizs.size());
        std::sort(clusterNamesWithExtendBizs.begin(), clusterNamesWithExtendBizs.end());
        std::vector<std::string> expected{
            "auction.default",
            "auction.para_search_2",
            "auction.para_search_4",
            "auction_bucket.default",
            "auction_bucket.para_search_2",
            "auction_bucket.para_search_4",
            "auction_summary_so.default",
            "auction_summary_so.para_search_2",
            "auction_summary_so.para_search_4",
            "company.default",
            "company.para_search_2",
            "company.para_search_4",
            "mainse_searcher.default",
            "mainse_searcher.para_search_2",
            "mainse_searcher.para_search_4",
            "offer_gb.default",
            "offer_gb.para_search_2",
            "offer_gb.para_search_4"};
        ASSERT_EQ(expected, clusterNamesWithExtendBizs);
    }
    {
        sap::EnvironUtil::setEnv("paraSearchWays", "8");
        std::vector<std::string> clusterNamesWithExtendBizs;
        ASSERT_TRUE(_configAdapterPtr->getClusterNamesWithExtendBizs(clusterNamesWithExtendBizs));
        ASSERT_EQ(12u, clusterNamesWithExtendBizs.size());
        std::sort(clusterNamesWithExtendBizs.begin(), clusterNamesWithExtendBizs.end());
        std::vector<std::string> expected{
                "auction.default",
                "auction.para_search_8",
                "auction_bucket.default",
                "auction_bucket.para_search_8",
                "auction_summary_so.default",
                "auction_summary_so.para_search_8",
                "company.default",
                "company.para_search_8",
                "mainse_searcher.default",
                "mainse_searcher.para_search_8",
                "offer_gb.default",
                "offer_gb.para_search_8"};
        ASSERT_EQ(expected, clusterNamesWithExtendBizs);
    }
    sap::EnvironUtil::setEnv("paraSearchWays", oldPsw);
}

TEST_F(ConfigAdapterTest, testGetParaWays) {
    const string &oldPsw = sap::EnvironUtil::getEnv("paraSearchWays", "");
    sap::EnvironUtil::setEnv("paraSearchWays", "");
    ConfigAdapter config("test");
    ASSERT_EQ(string("2,4"), config.getParaWays());

    sap::EnvironUtil::setEnv("paraSearchWays", "8");
    ASSERT_EQ(string("8"), config.getParaWays());

    sap::EnvironUtil::setEnv("paraSearchWays", "xxx");
    ASSERT_EQ(string("xxx"), config.getParaWays());

    sap::EnvironUtil::setEnv("paraSearchWays", "8,32");
    ASSERT_EQ(string("8,32"), config.getParaWays());
    sap::EnvironUtil::setEnv("paraSearchWays", oldPsw);
}

END_HA3_NAMESPACE(config);
