#include "suez/table/PartitionProperties.h"

#include "autil/StringUtil.h"
#include "fslib/fs/FileSystem.h"
#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

namespace suez {

class PartitionPropertiesTest : public TESTBASE {
private:
    void checkMap(const std::map<std::string, std::string> &expected,
                  const std::map<std::string, std::string> &current) {
        ASSERT_EQ(expected.size(), current.size());
        for (const auto &[k, v] : current) {
            auto iter = expected.find(k);
            ASSERT_TRUE(iter != expected.end());
            ASSERT_EQ(iter->second, v);
        }
    }
};

TEST_F(PartitionPropertiesTest, testSimple) {
    auto pid = TableMetaUtil::makePid("t", 1);
    auto target = TableMetaUtil::makeTarget(1, "/path/to/index/t", "/path/to/config/t/12345");

    PartitionProperties properties(pid);

    TableConfig tableConfig;
    tableConfig._realtime = true;
    ASSERT_TRUE(properties.init(target, tableConfig));
    ASSERT_TRUE(properties.hasRt);
    ASSERT_FALSE(properties.needReadRemoteIndex());
    ASSERT_EQ("/path/to/index/t/t/generation_1/partition_0_65535", properties.secondaryIndexRoot);
    ASSERT_TRUE(
        autil::StringUtil::endsWith(properties.primaryIndexRoot, "runtimedata/t/generation_1/partition_0_65535"));
    ASSERT_EQ(properties.primaryIndexRoot, properties.schemaRoot);
}

TEST_F(PartitionPropertiesTest, testLoadTableConfig) {
    TableConfig tableConfig;
    ASSERT_TRUE(
        tableConfig.loadFromFile(GET_TEST_DATA_PATH() + "/table_test/table_config", "table_cluster.json", false));
    ASSERT_EQ(std::string("normal"), tableConfig.GetDataLinkMode());
}

TEST_F(PartitionPropertiesTest, testLoadRealtimeInfo) {
    { // normal case
        TearDown();
        SetUp();
        auto pid = TableMetaUtil::makePid("t", 1);
        std::string rootDir = GET_TEMP_DATA_PATH();
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::mkDir(rootDir + "config/clusters", true));
        ASSERT_EQ(fslib::EC_OK,
                  fslib::fs::FileSystem::writeFile(rootDir + "config/clusters/t_cluster.json",
                                                   "{\"realtime_info\":{\"a\":\"b\"}}"));
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::mkDir(rootDir + "index/partition_0_65535", true));
        ASSERT_EQ(fslib::EC_OK,
                  fslib::fs::FileSystem::writeFile(rootDir + "index/realtime_info.json",
                                                   "{\"app_name\":\"appName\",\"data_table\":\"dataTable\"}"));
        build_service::workflow::RealtimeInfoWrapper realtimeInfo;
        ASSERT_TRUE(PartitionProperties::loadRealtimeInfo(
            pid, rootDir + "config", rootDir + "index/partition_0_65535", &realtimeInfo));
        checkMap({{"app_name", "appName"}, {"data_table", "dataTable"}, {"a", "b"}}, realtimeInfo.getKvMap());
    }
    { // app_name/data_table in index and config is different
        TearDown();
        SetUp();
        auto pid = TableMetaUtil::makePid("t", 1);
        std::string rootDir = GET_TEMP_DATA_PATH();
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::mkDir(rootDir + "config/clusters", true));
        ASSERT_EQ(fslib::EC_OK,
                  fslib::fs::FileSystem::writeFile(
                      rootDir + "config/clusters/t_cluster.json",
                      "{\"realtime_info\":{\"a\":\"b\",\"app_name\":\"app1\",\"data_table\":\"table1\"}}"));
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::mkDir(rootDir + "index/partition_0_65535", true));
        ASSERT_EQ(fslib::EC_OK,
                  fslib::fs::FileSystem::writeFile(rootDir + "index/realtime_info.json",
                                                   "{\"app_name\":\"appName\",\"data_table\":\"dataTable\"}"));
        build_service::workflow::RealtimeInfoWrapper realtimeInfo;
        ASSERT_FALSE(PartitionProperties::loadRealtimeInfo(
            pid, rootDir + "config", rootDir + "index/partition_0_65535", &realtimeInfo));
    }
    { // app_name/data_table in index and config is same
        TearDown();
        SetUp();
        auto pid = TableMetaUtil::makePid("t", 1);
        std::string rootDir = GET_TEMP_DATA_PATH();
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::mkDir(rootDir + "config/clusters", true));
        ASSERT_EQ(fslib::EC_OK,
                  fslib::fs::FileSystem::writeFile(
                      rootDir + "config/clusters/t_cluster.json",
                      "{\"realtime_info\":{\"a\":\"b\",\"app_name\":\"appName\",\"data_table\":\"dataTable\"}}"));
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::mkDir(rootDir + "index/partition_0_65535", true));
        ASSERT_EQ(fslib::EC_OK,
                  fslib::fs::FileSystem::writeFile(rootDir + "index/realtime_info.json",
                                                   "{\"app_name\":\"appName\",\"data_table\":\"dataTable\"}"));
        build_service::workflow::RealtimeInfoWrapper realtimeInfo;
        ASSERT_TRUE(PartitionProperties::loadRealtimeInfo(
            pid, rootDir + "config", rootDir + "index/partition_0_65535", &realtimeInfo));
        checkMap({{"app_name", "appName"}, {"data_table", "dataTable"}, {"a", "b"}}, realtimeInfo.getKvMap());
    }
    { // app_name/data_table only in config
        TearDown();
        SetUp();
        auto pid = TableMetaUtil::makePid("t", 1);
        std::string rootDir = GET_TEMP_DATA_PATH();
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::mkDir(rootDir + "config/clusters", true));
        ASSERT_EQ(fslib::EC_OK,
                  fslib::fs::FileSystem::writeFile(
                      rootDir + "config/clusters/t_cluster.json",
                      "{\"realtime_info\":{\"a\":\"b\",\"app_name\":\"appName\",\"data_table\":\"dataTable\"}}"));
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::mkDir(rootDir + "index/partition_0_65535", true));
        ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::writeFile(rootDir + "index/realtime_info.json", "{}"));
        build_service::workflow::RealtimeInfoWrapper realtimeInfo;
        ASSERT_TRUE(PartitionProperties::loadRealtimeInfo(
            pid, rootDir + "config", rootDir + "index/partition_0_65535", &realtimeInfo));
        checkMap({{"app_name", "appName"}, {"data_table", "dataTable"}, {"a", "b"}}, realtimeInfo.getKvMap());
    }
}

} // namespace suez
