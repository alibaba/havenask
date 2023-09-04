#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/DocReclaimSource.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/test/unittest.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace testing;
using namespace build_service::util;

using namespace indexlib::config;

namespace build_service { namespace config {

class ResourceReaderTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

protected:
    template <typename T>
    void checkTypedValue(const std::string& jsonPath, const T& expectValue);

    void CheckClusterConfigWithInherit(const indexlibv2::config::OfflineConfig& offlineConfig);
};

void ResourceReaderTest::setUp() { ResourceReaderManager::GetInstance()->_readerCache.clear(); }

void ResourceReaderTest::tearDown() {}

class UserDefineStruct : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
    {
        json.Jsonize("my_value1", myValue1);
        json.Jsonize("my_value2", myValue2);
    }
    bool operator==(const UserDefineStruct& other) const
    {
        return myValue1 == other.myValue1 && myValue2 == other.myValue2;
    }

public:
    int myValue1;
    string myValue2;
};

TEST_F(ResourceReaderTest, testGetConfigWithJsonPath)
{
    checkTypedValue("root_str_key", string("root_str_value"));
    checkTypedValue("root_int_key", int(10));

    // check for inherit config
    checkTypedValue("base_str_key", string("base_str_value"));
    checkTypedValue("base_int_key", int(5));

    vector<int> vec;
    vec.push_back(10);
    vec.push_back(20);
    vec.push_back(30);
    checkTypedValue("root_vec_key3", vec);

    checkTypedValue("root_vec_key1[0]", string("str1"));

    checkTypedValue("root_map_key.child_map_key1.child_int_key", int(100000));
    checkTypedValue("root_map_key.child_map_key1.child_vec_key[2]", false);
    checkTypedValue("root_map_key.child_vec_key[0][1][1]", string("bcd2"));

    checkTypedValue("root_map_key.not_exist_path", string(""));

    KeyValueMap map;
    map["child_map_key3"] = "child_map_value3";
    map["child_map_key4"] = "child_map_value4";
    checkTypedValue("root_map_key.child_map_key1.child_map_key2", map);

    UserDefineStruct uds;
    uds.myValue1 = 1;
    uds.myValue2 = "aaa";
    checkTypedValue("root_map_key.user_define_struct", uds);
}

TEST_F(ResourceReaderTest, testGetAllClusters)
{
    // old
    {
        ResourceReaderPtr resourceReader =
            ResourceReaderManager::getResourceReader(GET_TEST_DATA_PATH() + "resource_reader_test/multi_table_config");
        {
            vector<string> clusterNames;
            EXPECT_TRUE(resourceReader->getAllClusters("simple1", clusterNames));
            EXPECT_THAT(clusterNames, UnorderedElementsAre(string("cluster1"), string("cluster2")));
        }
        {
            vector<string> clusterNames;
            EXPECT_TRUE(resourceReader->getAllClusters("simple2", clusterNames));
            EXPECT_THAT(clusterNames, UnorderedElementsAre(string("cluster11"), string("cluster13")));
        }
    }

    // new
    {
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(
            GET_TEST_DATA_PATH() + "resource_reader_test/multi_table_config_new");
        {
            vector<string> clusterNames;
            EXPECT_TRUE(resourceReader->getAllClusters("simple1", clusterNames));
            EXPECT_THAT(clusterNames, UnorderedElementsAre(string("cluster1"), string("cluster2")));
        }
        {
            vector<string> clusterNames;
            EXPECT_TRUE(resourceReader->getAllClusters("simple2", clusterNames));
            EXPECT_THAT(clusterNames, UnorderedElementsAre(string("cluster11"), string("cluster13")));
        }
    }
}

TEST_F(ResourceReaderTest, testGetAllDataTables)
{
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(GET_TEST_DATA_PATH() + "resource_reader_test/multi_table_config");
    vector<string> dataTables;
    EXPECT_TRUE(resourceReader->getAllDataTables(dataTables));
    EXPECT_THAT(dataTables, UnorderedElementsAre(string("simple1"), string("simple2")));
}

TEST_F(ResourceReaderTest, testGetAgentGroupConfig)
{
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(GET_TEST_DATA_PATH() + "resource_reader_test/multi_table_config");
    AgentGroupConfig config;
    EXPECT_FALSE(resourceReader->getAgentGroupConfig(false, config));

    resourceReader =
        ResourceReaderManager::getResourceReader(GET_TEST_DATA_PATH() + "resource_reader_test/agent_node_config");
    EXPECT_TRUE(resourceReader->getAgentGroupConfig(false, config));
    ASSERT_EQ(2, config.getAgentConfigs().size());

    AgentGroupConfig generalConfig;
    EXPECT_TRUE(resourceReader->getAgentGroupConfig(true, generalConfig));
    ASSERT_EQ(1, generalConfig.getAgentConfigs().size());
}

TEST_F(ResourceReaderTest, testGetDataTableFromClusterName)
{
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(GET_TEST_DATA_PATH() + "resource_reader_test/multi_table_config");
    string dataTable;
    EXPECT_TRUE(resourceReader->getDataTableFromClusterName("cluster1", dataTable));
    EXPECT_EQ("simple1", dataTable);
    EXPECT_FALSE(resourceReader->getDataTableFromClusterName("not exist", dataTable));
}

TEST_F(ResourceReaderTest, testGetGenerationMeta)
{
    {
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader("");
        EXPECT_TRUE(resourceReader->initGenerationMeta(""));
        EXPECT_EQ("", resourceReader->getGenerationMetaValue("value"));
    }
    {
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader("");
        EXPECT_TRUE(resourceReader->initGenerationMeta(GET_TEST_DATA_PATH() + "generation_meta_test/indexroot1"));
        EXPECT_EQ("value1", resourceReader->getGenerationMetaValue("key1"));
    }
    {
        ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader("");
        EXPECT_FALSE(resourceReader->initGenerationMeta(GET_TEST_DATA_PATH() + "generation_meta_test/indexroot3"));
    }
}

TEST_F(ResourceReaderTest, testGetSchemaBySchemaTableName)
{
    string configDir = GET_TEST_DATA_PATH() + "admin_test/config_updatable_cluster1";
    string configDirCopy = GET_TEMP_DATA_PATH() + "admin_test/config_updatable_cluster1";
    fslib::util::FileUtil::atomicCopy(configDir, configDirCopy);
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(configDirCopy);
    ASSERT_TRUE(fslib::util::FileUtil::atomicCopy(configDirCopy + "/schemas/mainse_searcher_schema.json",
                                                  configDirCopy + "/schemas/test_schema.json"));
    IndexPartitionSchemaPtr schema = resourceReader->getSchemaBySchemaTableName("test");
    ASSERT_TRUE(schema);
    ASSERT_EQ(165, schema->GetFieldCount());

    // will read schema from cache
    ASSERT_TRUE(fslib::util::FileUtil::remove(configDirCopy + "/schemas/test_schema.json"));
    IndexPartitionSchemaPtr schema2 = resourceReader->getSchemaBySchemaTableName("test");
    ASSERT_NO_THROW(schema->AssertEqual(*(schema2.get())));
}

TEST_F(ResourceReaderTest, testGetTabletSchemaBySchemaTableName)
{
    string configDir = GET_TEST_DATA_PATH() + "admin_test/config_updatable_cluster1";
    string configDirCopy = GET_TEMP_DATA_PATH() + "admin_test/config_updatable_cluster1";
    fslib::util::FileUtil::atomicCopy(configDir, configDirCopy);
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(configDirCopy);
    ASSERT_TRUE(fslib::util::FileUtil::atomicCopy(configDirCopy + "/schemas/mainse_searcher_schema.json",
                                                  configDirCopy + "/schemas/test_schema.json"));
    std::shared_ptr<indexlibv2::config::ITabletSchema> schema =
        resourceReader->getTabletSchemaBySchemaTableName("test");
    ASSERT_TRUE(schema);
    // 165 + 3
    // 165: user specified fields
    // 3:  virtual field, messageid, hash id and timestamp, added by NormalSchemaResolver
    ASSERT_EQ(168, schema->GetFieldCount());

    // will read schema from cache
    ASSERT_TRUE(fslib::util::FileUtil::remove(configDirCopy + "/schemas/test_schema.json"));
    std::shared_ptr<indexlibv2::config::ITabletSchema> schema2 =
        resourceReader->getTabletSchemaBySchemaTableName("test");
    ASSERT_NO_THROW(schema->GetLegacySchema()->AssertEqual(*(schema2->GetLegacySchema().get())));
}

TEST_F(ResourceReaderTest, testInitGenerationMeta)
{
    {
        // test normal
        ResourceReaderPtr resourceReader =
            ResourceReaderManager::getResourceReader(GET_TEST_DATA_PATH() + "resource_reader_test/multi_table_config");
        ASSERT_TRUE(
            resourceReader->initGenerationMeta(0, "simple1", GET_TEST_DATA_PATH() + "resource_reader_test/index_root"));
        ASSERT_EQ("value1", resourceReader->getGenerationMetaValue("key1"));
        ASSERT_EQ("value3", resourceReader->getGenerationMetaValue("key3"));
    }
    {
        ResourceReaderPtr resourceReader =
            ResourceReaderManager::getResourceReader(GET_TEST_DATA_PATH() + "resource_reader_test/multi_table_config");
        ASSERT_FALSE(
            resourceReader->initGenerationMeta(0, "simple2", GET_TEST_DATA_PATH() + "resource_reader_test/index_root"));
    }
}

TEST_F(ResourceReaderTest, testReadZipConfig)
{
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(GET_TEST_DATA_PATH() + "resource_reader_test/zip_config");
    setenv(BS_ENV_ENABLE_RESOURCE_READER_MAX_ZIP_SIZE.c_str(), "1", true);
    ASSERT_EQ(10, resourceReader->_jsonFileCache.size());

    SwiftConfig swiftConfig;
    ASSERT_TRUE(resourceReader->getClusterConfigWithJsonPath("cluster1", "swift_config", swiftConfig));
    ASSERT_EQ("mode=async", swiftConfig.getSwiftWriterConfig(FULL_SWIFT_BROKER_TOPIC_CONFIG));
    int keepCheckpointCount = 0;
    ASSERT_TRUE(resourceReader->getClusterConfigWithJsonPath("cluster1", "cluster_config.keep_checkpoint_count",
                                                             keepCheckpointCount));
    ASSERT_EQ(2, keepCheckpointCount);
    {
        // cluster4_cluster.json exists in both zip file and clusters dir.
        // this file in ZIP shall prevail
        int partCount = 0;
        ASSERT_TRUE(resourceReader->getClusterConfigWithJsonPath(
            "cluster4", "cluster_config.builder_rule_config.partition_count", partCount));
        ASSERT_EQ(2, partCount);

        // if file is not in ZIP, resourceReader will try to read it from dir
        string tableName = "";
        ASSERT_TRUE(resourceReader->getClusterConfigWithJsonPath("cluster6", "cluster_config.table_name", tableName));
        ASSERT_EQ("cluster6", tableName);
    }

    IndexPartitionSchemaPtr schema = resourceReader->getSchema("cluster1");
    ASSERT_TRUE(schema);
    ASSERT_EQ(165, schema->GetFieldCount());

    vector<string> allSchemas = resourceReader->getAllSchemaFileNames();
    EXPECT_THAT(allSchemas, UnorderedElementsAre("schemas/cluster1_schema.json", "schemas/cluster2_schema.json",
                                                 "schemas/cluster3_schema.json", "schemas/cluster4_schema.json",
                                                 "schemas/cluster5_schema.json"));
    unsetenv(BS_ENV_ENABLE_RESOURCE_READER_MAX_ZIP_SIZE.c_str());
}

TEST_F(ResourceReaderTest, testReadZipConfigFailed)
{
    setenv(BS_ENV_ENABLE_RESOURCE_READER_MAX_ZIP_SIZE.c_str(), "10", true);
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(GET_TEST_DATA_PATH() + "resource_reader_test/zip_config");
    ASSERT_EQ(5, resourceReader->_jsonFileCache.size());

    SwiftConfig swiftConfig;
    ASSERT_TRUE(resourceReader->getClusterConfigWithJsonPath("cluster1", "swift_config", swiftConfig));
    ASSERT_EQ("mode=async", swiftConfig.getSwiftWriterConfig(FULL_SWIFT_BROKER_TOPIC_CONFIG));
    int keepCheckpointCount = 0;
    ASSERT_TRUE(resourceReader->getClusterConfigWithJsonPath("cluster1", "cluster_config.keep_checkpoint_count",
                                                             keepCheckpointCount));
    ASSERT_EQ(2, keepCheckpointCount);

    IndexPartitionSchemaPtr schema = resourceReader->getSchema("cluster1");
    ASSERT_FALSE(schema);
    unsetenv(BS_ENV_ENABLE_RESOURCE_READER_MAX_ZIP_SIZE.c_str());
}

TEST_F(ResourceReaderTest, testParseClusterConfigWithInherit)
{
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(GET_TEST_DATA_PATH() + "resource_reader_test/inherit_config");
    indexlibv2::config::OfflineConfig offlineConfig;
    ASSERT_TRUE(
        resourceReader->getConfigWithJsonPath("clusters/legacy_cluster.json", "offline_index_config", offlineConfig));
    CheckClusterConfigWithInherit(offlineConfig);
}

TEST_F(ResourceReaderTest, testParseTabletOptionsWithInherit)
{
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(GET_TEST_DATA_PATH() + "resource_reader_test/inherit_config");
    auto tabletOptions = resourceReader->getTabletOptions("tablet");
    CheckClusterConfigWithInherit(tabletOptions->GetOfflineConfig());
}

TEST_F(ResourceReaderTest, testParseLegacyTabletOptionsWithInherit)
{
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(GET_TEST_DATA_PATH() + "resource_reader_test/inherit_config");
    auto tabletOptions = resourceReader->getTabletOptions("legacy");
    CheckClusterConfigWithInherit(tabletOptions->GetOfflineConfig());
}

TEST_F(ResourceReaderTest, getTabletSchemaWithOverwriteSchemaName)
{
    string configDir = GET_TEST_DATA_PATH() + "resource_reader_test/config_with_schema/";
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(configDir);
    std::shared_ptr<indexlibv2::config::ITabletSchema> schema1 = resourceReader->getTabletSchema("mainse_summary");
    ASSERT_TRUE(schema1);
    ASSERT_EQ("mainse_summary", schema1->GetTableName());
    std::shared_ptr<indexlibv2::config::ITabletSchema> schema2 =
        resourceReader->getTabletSchema("mainse_summary_extra");
    ASSERT_TRUE(schema2);
    ASSERT_EQ("mainse_summary_extra", schema2->GetTableName());
}

void ResourceReaderTest::CheckClusterConfigWithInherit(const indexlibv2::config::OfflineConfig& offlineConfig)
{
    auto indexTaskConfigs = offlineConfig.GetAllIndexTaskConfigs();
    ASSERT_EQ(5u, indexTaskConfigs.size());

    {
        ASSERT_EQ("full_merge", indexTaskConfigs[0].GetTaskName());
        ASSERT_EQ("merge", indexTaskConfigs[0].GetTaskType());
        ASSERT_EQ("manual", indexTaskConfigs[0].GetTrigger().GetTriggerStr());
        ASSERT_EQ(indexlibv2::config::Trigger::MANUAL, indexTaskConfigs[0].GetTrigger().GetTriggerType());
        indexlib::Status status;
        uint32_t parallelNum;
        std::tie(status, parallelNum) = indexTaskConfigs[0].GetSetting<uint32_t>("parallel_num");
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(16, parallelNum);
        uint32_t threadNum;
        std::tie(status, threadNum) = indexTaskConfigs[0].GetSetting<uint32_t>("thread_num");
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(10, threadNum);
        indexlibv2::config::MergeConfig mergeConfig;
        std::tie(status, mergeConfig) = indexTaskConfigs[0].GetSetting<indexlibv2::config::MergeConfig>("merge_config");
        ASSERT_EQ("optimize", mergeConfig.GetMergeStrategyStr());
    }

    {
        ASSERT_EQ("inc", indexTaskConfigs[1].GetTaskName());
        ASSERT_EQ("merge", indexTaskConfigs[1].GetTaskType());
        ASSERT_EQ("period=1800", indexTaskConfigs[1].GetTrigger().GetTriggerStr());
        ASSERT_EQ(indexlibv2::config::Trigger::PERIOD, indexTaskConfigs[1].GetTrigger().GetTriggerType());
        ASSERT_EQ(1800, indexTaskConfigs[1].GetTrigger().GetTriggerPeriod());
        indexlib::Status status;
        uint32_t parallelNum;
        std::tie(status, parallelNum) = indexTaskConfigs[1].GetSetting<uint32_t>("parallel_num");
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(8, parallelNum);
        uint32_t threadNum;
        std::tie(status, threadNum) = indexTaskConfigs[1].GetSetting<uint32_t>("thread_num");
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(10, threadNum);
        indexlibv2::config::MergeConfig mergeConfig;
        std::tie(status, mergeConfig) = indexTaskConfigs[1].GetSetting<indexlibv2::config::MergeConfig>("merge_config");
        ASSERT_EQ("optimize", mergeConfig.GetMergeStrategyStr());
    }
    {
        ASSERT_EQ("inc_2", indexTaskConfigs[2].GetTaskName());
        ASSERT_EQ("merge", indexTaskConfigs[2].GetTaskType());
        ASSERT_EQ("daytime=12:00", indexTaskConfigs[2].GetTrigger().GetTriggerStr());
        ASSERT_EQ(indexlibv2::config::Trigger::DATE_TIME, indexTaskConfigs[2].GetTrigger().GetTriggerType());
        indexlib::Status status;
        uint32_t threadNum;
        std::tie(status, threadNum) = indexTaskConfigs[2].GetSetting<uint32_t>("thread_num");
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(10, threadNum);
        indexlibv2::config::MergeConfig mergeConfig;
        std::tie(status, mergeConfig) = indexTaskConfigs[2].GetSetting<indexlibv2::config::MergeConfig>("merge_config");
        ASSERT_EQ("balance_tree", mergeConfig.GetMergeStrategyStr());
    }
    {
        ASSERT_EQ("inc_3", indexTaskConfigs[3].GetTaskName());
        ASSERT_EQ("merge", indexTaskConfigs[3].GetTaskType());
        ASSERT_EQ("period=7200", indexTaskConfigs[3].GetTrigger().GetTriggerStr());
        ASSERT_EQ(indexlibv2::config::Trigger::PERIOD, indexTaskConfigs[3].GetTrigger().GetTriggerType());
        indexlib::Status status;
        uint32_t threadNum;
        std::tie(status, threadNum) = indexTaskConfigs[2].GetSetting<uint32_t>("thread_num");
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(10, threadNum);
    }
    {
        ASSERT_EQ("inc_3_legacy_reclaim", indexTaskConfigs[4].GetTaskName());
        ASSERT_EQ("reclaim", indexTaskConfigs[4].GetTaskType());
        ASSERT_EQ("period=7200", indexTaskConfigs[4].GetTrigger().GetTriggerStr());
        ASSERT_EQ(indexlibv2::config::Trigger::PERIOD, indexTaskConfigs[4].GetTrigger().GetTriggerType());
        auto [status, source] = indexTaskConfigs[4].GetSetting<DocReclaimSource>("doc_reclaim_source");
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(source.swiftRoot, "http://test/opensearch");
        ASSERT_EQ(source.topicName, "app_reclaim");
        ASSERT_EQ(source.clientConfigStr, "");
        ASSERT_EQ(source.readerConfigStr, "hashByClusterName=true;clusterHashFunc=HASH;");
        ASSERT_EQ(source.msgTTLInSeconds, -1);
    }
}

template <typename T>
void ResourceReaderTest::checkTypedValue(const string& jsonPath, const T& expectValue)
{
    ResourceReaderPtr resourceReader =
        ResourceReaderManager::getResourceReader(GET_TEST_DATA_PATH() + "/resource_reader_test/");
    T value;
    ASSERT_TRUE(resourceReader->getConfigWithJsonPath("test_json_file.json", jsonPath, value)) << jsonPath;
    EXPECT_TRUE(value == expectValue);
}

}} // namespace build_service::config
