#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/ClusterConfigParser.h>
#include <ha3/test/test.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <ha3/config/SummaryProfileConfig.h>
#include <ha3/config/AggSamplerConfigInfo.h>
#include <indexlib/config/index_partition_options.h>
using namespace std;

IE_NAMESPACE_USE(config);

BEGIN_HA3_NAMESPACE(config);

class ClusterConfigParserTest : public TESTBASE {
public:
    ClusterConfigParserTest();
    ~ClusterConfigParserTest();
public:
    void setUp();
    void tearDown();
protected:
    std::string _configRoot;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, ClusterConfigParserTest);


ClusterConfigParserTest::ClusterConfigParserTest() {
}

ClusterConfigParserTest::~ClusterConfigParserTest() {
}

void ClusterConfigParserTest::setUp() {
    _configRoot = TEST_DATA_PATH;
    _configRoot += "/config_with_imports";
}

void ClusterConfigParserTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ClusterConfigParserTest, testSquareInherit) {
    string clusterFilePath = _configRoot + "/square_inherit/derived.json";
    ClusterConfigParser parser(_configRoot, clusterFilePath);

    ClusterConfigInfo clusterConfigInfo;
    ASSERT_EQ(ClusterConfigParser::PR_OK,
                         parser.getClusterConfigInfo(clusterConfigInfo));
    

    // square inherit
    SummaryProfileConfig summaryProfileConfig;
    ASSERT_EQ(ClusterConfigParser::PR_OK,
                         parser.getSummaryProfileConfig(summaryProfileConfig));
    ASSERT_EQ(string("base"), summaryProfileConfig.getSummaryProfileInfos()[0]._summaryProfileName);
    ASSERT_EQ(string("ProfileA"), summaryProfileConfig.getSummaryProfileInfos()[0]._extractorInfos[0]._moduleName);
    ASSERT_EQ(string("DefaultExtractor"), summaryProfileConfig.getSummaryProfileInfos()[0]._extractorInfos[0]._extractorName);
    ASSERT_EQ(string("base"), summaryProfileConfig.getSummaryProfileInfos()[1]._summaryProfileName);
    ASSERT_EQ(string("ProfileB"), summaryProfileConfig.getSummaryProfileInfos()[1]._extractorInfos[0]._moduleName);
    ASSERT_EQ(string("DefaultExtractor"), summaryProfileConfig.getSummaryProfileInfos()[1]._extractorInfos[0]._extractorName);

    // section without override, from inherit
    AggSamplerConfigInfo aggSamplerInfo;
    ASSERT_EQ(ClusterConfigParser::PR_OK,
                         parser.getAggSamplerConfigInfo(aggSamplerInfo));
    ASSERT_EQ((uint32_t)2000, aggSamplerInfo.getAggThreshold());
    ASSERT_EQ((uint32_t)100000, aggSamplerInfo.getSampleStep());
}

TEST_F(ClusterConfigParserTest, testWithDeepInherit) {
    string clusterFilePath = _configRoot + "/deep_inherit/derived.json";
    ClusterConfigParser parser(_configRoot, clusterFilePath);

    // deep inherit
    ClusterConfigInfo clusterConfigInfo;
    ASSERT_EQ(ClusterConfigParser::PR_OK,
                         parser.getClusterConfigInfo(clusterConfigInfo));
    ASSERT_EQ(string("section replaced"),
                         clusterConfigInfo.getHashMode()._hashFunction);
    ASSERT_EQ(string("subsection replaced"),
                         clusterConfigInfo.getHashMode()._hashField);
    
    SummaryProfileConfig summaryProfileConfig;
    ASSERT_EQ(ClusterConfigParser::PR_OK,
                         parser.getSummaryProfileConfig(summaryProfileConfig));
    ASSERT_EQ(string("Inherit"), summaryProfileConfig.getSummaryProfileInfos()[0]._summaryProfileName);
    ASSERT_EQ(string("Inherit"), summaryProfileConfig.getSummaryProfileInfos()[0]._extractorInfos[0]._moduleName);
    ASSERT_EQ(string("DefaultExtractor"), summaryProfileConfig.getSummaryProfileInfos()[0]._extractorInfos[0]._extractorName);

    // section without override, from inherit
    AggSamplerConfigInfo aggSamplerInfo;
    ASSERT_EQ(ClusterConfigParser::PR_OK,
                         parser.getAggSamplerConfigInfo(aggSamplerInfo));
    ASSERT_EQ((uint32_t)2000, aggSamplerInfo.getAggThreshold());
    ASSERT_EQ((uint32_t)100000, aggSamplerInfo.getSampleStep());
}

TEST_F(ClusterConfigParserTest, testWithInherit) {
    string clusterFilePath = _configRoot + "/" + CLUSTER_CONFIG_DIR_NAME
                             + "/mainse_searcher/" + DEFAULT_BIZ_NAME + CLUSTER_JSON_FILE_SUFFIX;
    ClusterConfigParser parser(_configRoot, clusterFilePath);

    // sections with override
    ClusterConfigInfo clusterConfigInfo;
    ASSERT_EQ(ClusterConfigParser::PR_OK,
                         parser.getClusterConfigInfo(clusterConfigInfo));

    // section without override, from inherit
    AggSamplerConfigInfo aggSamplerInfo;
    ASSERT_EQ(ClusterConfigParser::PR_OK,
                         parser.getAggSamplerConfigInfo(aggSamplerInfo));
    ASSERT_EQ((uint32_t)2000, aggSamplerInfo.getAggThreshold());
    ASSERT_EQ((uint32_t)100000, aggSamplerInfo.getSampleStep());
}

TEST_F(ClusterConfigParserTest, testWithSectionInherit) {
    string clusterFilePath = _configRoot + "/build/mainse_searcher_full_build.json";
    ClusterConfigParser parser(_configRoot, clusterFilePath);

    //IndexPartitionOptions indexPartOption;
        // override
    // ASSERT_EQ(int64_t(512), indexPartOption.GetBuildConfig(true).buildTotalMemory);
    // ASSERT_EQ(int64_t(5120), indexPartOption.GetBuildConfig(false).buildTotalMemory);
    // int64_t maxReopenMemoryUse;
    // ASSERT_TRUE(indexPartOption.GetOnlineConfig().GetMaxReopenMemoryUse(maxReopenMemoryUse));
    // ASSERT_EQ(int64_t(20480), maxReopenMemoryUse);
    // // inherit
    // ASSERT_EQ(uint32_t(4), indexPartOption.GetMergeConfig().mergeThreadCount);
    //todo add UT case for sectinInherit

}

TEST_F(ClusterConfigParserTest, testClusterInheritAndSectionInherit) {
    string clusterFilePath = _configRoot + "/" + CLUSTER_CONFIG_DIR_NAME
                             + "/mainse_searcher_bad/" + DEFAULT_BIZ_NAME + CLUSTER_JSON_FILE_SUFFIX;
    ClusterConfigParser parser(_configRoot, clusterFilePath);

    ClusterConfigInfo clusterConfigInfo;
    ASSERT_EQ(ClusterConfigParser::PR_OK,
                         parser.getClusterConfigInfo(clusterConfigInfo));
    // from cluster.json
    ASSERT_EQ(string("mainse_searcher"), clusterConfigInfo.getTableName());
    // from section inherit
        // from cluster inherit
    AggSamplerConfigInfo aggSamplerInfo;
    ASSERT_EQ(ClusterConfigParser::PR_OK,
                         parser.getAggSamplerConfigInfo(aggSamplerInfo));
    ASSERT_EQ((uint32_t)2000, aggSamplerInfo.getAggThreshold());
    ASSERT_EQ((uint32_t)100000, aggSamplerInfo.getSampleStep());
}

TEST_F(ClusterConfigParserTest, testWithInheritLoop) {
    string clusterFilePath = _configRoot + "/invalid_configs/loop_cluster.json";
    ClusterConfigParser parser(_configRoot, clusterFilePath);

    ClusterConfigInfo clusterConfigInfo;
    ASSERT_EQ(ClusterConfigParser::PR_FAIL,
                         parser.getClusterConfigInfo(clusterConfigInfo));

}

TEST_F(ClusterConfigParserTest, testGetAnomalyConfig) {
    {
        string filePath = string(TEST_DATA_PATH) +
                          "/testadmin/taobao/taobao_daogou/configurations/configuration_3/zones/company/default_biz.json";
        ClusterConfigParser parser(_configRoot, filePath);
        {
            AnomalyProcessConfig config;
            ASSERT_EQ(ClusterConfigParser::PR_OK, parser.getAnomalyProcessConfig(config));
            EXPECT_FALSE(config.empty());
            EXPECT_EQ("10", ToJsonString(config["begin_degrade_latency"]));
        }
        {
            AnomalyProcessConfigT configT;
            ASSERT_EQ(ClusterConfigParser::PR_OK, parser.getAnomalyProcessConfigT(configT));
            EXPECT_FALSE(configT.empty());
            EXPECT_EQ("5", ToJsonString(configT["full_degrade_latency"]));
        }
    }
    {
        string filePath = string(TEST_DATA_PATH) +
                          "/testadmin/taobao/taobao_daogou/configurations/configuration_3/zones/auction/default_biz.json";
        ClusterConfigParser parser(_configRoot, filePath);
        {
            AnomalyProcessConfig config;
            ASSERT_EQ(ClusterConfigParser::PR_SECTION_NOT_EXIST, parser.getAnomalyProcessConfig(config));
            EXPECT_TRUE(config.empty());
        }
        {
            AnomalyProcessConfigT configT;
            ASSERT_EQ(ClusterConfigParser::PR_SECTION_NOT_EXIST, parser.getAnomalyProcessConfigT(configT));
            EXPECT_TRUE(configT.empty());
        }
    }
}

END_HA3_NAMESPACE(config);

