#include "build_service/config/ConfigReaderAccessor.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/test/unittest.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/util/FileUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace build_service::config;
using namespace build_service::util;

namespace build_service { namespace config {

class ConfigReaderAccessorTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

public:
    ConfigReaderAccessorPtr _configReaderAccessor;
    void checkEqual(ConfigReaderAccessorPtr a, ConfigReaderAccessorPtr b);
};

void ConfigReaderAccessorTest::checkEqual(ConfigReaderAccessorPtr a, ConfigReaderAccessorPtr b)
{
    ASSERT_TRUE(a->_dataTableName == b->_dataTableName);
    ASSERT_TRUE(a->_latestConfigPath == b->_latestConfigPath);
    vector<ResourceReaderPtr> readersA, readersB;
    for (size_t i = 0; i < a->_resourceReaders.size(); ++i) {
        if (a->_resourceReaders[i]) {
            readersA.push_back(a->_resourceReaders[i]);
        }
    }
    for (size_t i = 0; i < b->_resourceReaders.size(); ++i) {
        if (b->_resourceReaders[i]) {
            readersB.push_back(b->_resourceReaders[i]);
        }
    }
    ASSERT_TRUE(readersA.size() == readersB.size());
    for (size_t i = 0; i < readersA.size(); ++i) {
        ASSERT_TRUE(readersA[i]->getConfigPath() == readersB[i]->getConfigPath());
    }

    ASSERT_TRUE(a->_configToResourceReader.size() == b->_configToResourceReader.size());
    {
        auto iter1 = a->_configToResourceReader.begin();
        auto iter2 = b->_configToResourceReader.begin();
        for (size_t i = 0; i < a->_configToResourceReader.size(); ++i) {
            ASSERT_TRUE(iter1->first == iter2->first);
            ASSERT_TRUE(iter1->second == iter2->second);
        }
    }
    ASSERT_TRUE(a->_clusterInfoToResourceReader.size() == b->_clusterInfoToResourceReader.size());
    {
        auto iter1 = a->_clusterInfoToResourceReader.begin();
        auto iter2 = b->_clusterInfoToResourceReader.begin();
        for (size_t i = 0; i < a->_clusterInfoToResourceReader.size(); ++i) {
            ASSERT_TRUE(iter1->first == iter2->first);
            ASSERT_TRUE(iter1->second == iter2->second);
        }
    }
}

void ConfigReaderAccessorTest::setUp() {}

void ConfigReaderAccessorTest::tearDown() {}

TEST_F(ConfigReaderAccessorTest, testNormalUpdateConfig)
{
    _configReaderAccessor.reset(new ConfigReaderAccessor("simple2"));

    string configPath1 = GET_TEST_DATA_PATH() + "/config_reader_accessor_test/config";
    ResourceReaderPtr resourceReader1(new ResourceReader(configPath1));
    ASSERT_TRUE(_configReaderAccessor->addConfig(resourceReader1, true));
    string configPath2 = GET_TEST_DATA_PATH() + "/config_reader_accessor_test/config_update";
    ResourceReaderPtr resourceReader2(new ResourceReader(configPath2));
    ASSERT_TRUE(_configReaderAccessor->addConfig(resourceReader2, true));
    ASSERT_EQ(configPath2, _configReaderAccessor->getLatestConfigPath());
    ASSERT_FALSE(_configReaderAccessor->getConfig(configPath1));
    ASSERT_EQ(resourceReader2, _configReaderAccessor->getConfig(configPath2));
}

TEST_F(ConfigReaderAccessorTest, testSimple)
{
    _configReaderAccessor.reset(new ConfigReaderAccessor("simple2"));

    string configPath1 = GET_TEST_DATA_PATH() + "/config_reader_accessor_test/config";
    configPath1 = fslib::util::FileUtil::normalizeDir(configPath1);
    ResourceReaderPtr resourceReader1(new ResourceReader(configPath1));
    ASSERT_TRUE(_configReaderAccessor->addConfig(resourceReader1, true));

    string configPath2 = GET_TEST_DATA_PATH() + "/config_reader_accessor_test/config_change_schema";
    configPath2 = fslib::util::FileUtil::normalizeDir(configPath2);
    ResourceReaderPtr resourceReader2(new ResourceReader(configPath2));
    ASSERT_TRUE(_configReaderAccessor->addConfig(resourceReader2, true));

    // test getLatestConfigPath()
    ASSERT_EQ(configPath2, _configReaderAccessor->getLatestConfigPath());
    // test getLatestConfig()
    ASSERT_EQ(resourceReader2, _configReaderAccessor->getLatestConfig());
    // test getConfig()
    ASSERT_EQ(resourceReader1, _configReaderAccessor->getConfig(configPath1));
    ASSERT_EQ(resourceReader1, _configReaderAccessor->getConfig("cluster1", 0));
    ASSERT_EQ(resourceReader1, _configReaderAccessor->getConfig("cluster4", 1));
    ASSERT_EQ(resourceReader2, _configReaderAccessor->getConfig("cluster2", 0));
    // test getClusterInfos()
    vector<ConfigReaderAccessor::ClusterInfo> clusterInfos;
    _configReaderAccessor->getClusterInfos(resourceReader1, "simple2", clusterInfos);
    ASSERT_EQ(ConfigReaderAccessor::ClusterInfo("cluster1", 0), clusterInfos[0]);
    ASSERT_EQ(ConfigReaderAccessor::ClusterInfo("cluster2", 0), clusterInfos[1]);
    ASSERT_EQ(ConfigReaderAccessor::ClusterInfo("cluster3", 0), clusterInfos[2]);
    ASSERT_EQ(ConfigReaderAccessor::ClusterInfo("cluster4", 1), clusterInfos[3]);
    // test getAllClusterInfos()
    _configReaderAccessor->getAllClusterInfos(clusterInfos);
    ASSERT_EQ(ConfigReaderAccessor::ClusterInfo("cluster1", 0), clusterInfos[0]);
    ASSERT_EQ(ConfigReaderAccessor::ClusterInfo("cluster2", 0), clusterInfos[1]);
    ASSERT_EQ(ConfigReaderAccessor::ClusterInfo("cluster3", 0), clusterInfos[2]);
    ASSERT_EQ(ConfigReaderAccessor::ClusterInfo("cluster4", 0), clusterInfos[3]);
    ASSERT_EQ(ConfigReaderAccessor::ClusterInfo("cluster1", 1), clusterInfos[4]);
    ASSERT_EQ(ConfigReaderAccessor::ClusterInfo("cluster4", 1), clusterInfos[5]);
    // test getAllClusterNames()
    vector<string> clusterNames;
    _configReaderAccessor->getAllClusterNames(clusterNames);
    ASSERT_EQ("cluster1", clusterNames[0]);
    ASSERT_EQ("cluster2", clusterNames[1]);
    ASSERT_EQ("cluster3", clusterNames[2]);
    ASSERT_EQ("cluster4", clusterNames[3]);
    // test getMinSchemaId() && getMaxSchemaId()
    ASSERT_EQ(0u, _configReaderAccessor->getMinSchemaId("cluster1"));
    ASSERT_EQ(0u, _configReaderAccessor->getMinSchemaId("cluster2"));
    ASSERT_EQ(1u, _configReaderAccessor->getMaxSchemaId("cluster1"));
    ASSERT_EQ(0u, _configReaderAccessor->getMaxSchemaId("cluster3"));
}

TEST_F(ConfigReaderAccessorTest, testJsonize)
{
    _configReaderAccessor.reset(new ConfigReaderAccessor("simple2"));

    string configPath1 = GET_TEST_DATA_PATH() + "/config_reader_accessor_test/config";
    configPath1 = fslib::util::FileUtil::normalizeDir(configPath1);
    ResourceReaderPtr resourceReader1(new ResourceReader(configPath1));
    ASSERT_TRUE(_configReaderAccessor->addConfig(resourceReader1, true));

    string configPath2 = GET_TEST_DATA_PATH() + "/config_reader_accessor_test/config_change_schema";
    configPath2 = fslib::util::FileUtil::normalizeDir(configPath2);
    ResourceReaderPtr resourceReader2(new ResourceReader(configPath2));
    ASSERT_TRUE(_configReaderAccessor->addConfig(resourceReader2, true));

    // string jsonStr = _configReaderAccessor->ToJsonString();
    string jsonStr = ToJsonString(*(_configReaderAccessor.get()));
    ConfigReaderAccessorPtr accessor(new ConfigReaderAccessor("simple2"));
    FromJsonString(*(accessor.get()), jsonStr);
    checkEqual(_configReaderAccessor, accessor);
    string jsonStr1 = ToJsonString(*(accessor.get()));
    ASSERT_EQ(jsonStr, jsonStr1);
}

TEST_F(ConfigReaderAccessorTest, testBug48178961)
{
    _configReaderAccessor.reset(new ConfigReaderAccessor("simple2"));

    string configPath1 = GET_TEST_DATA_PATH() + "/config_reader_accessor_test/config_add_field";
    configPath1 = fslib::util::FileUtil::normalizeDir(configPath1);
    ResourceReaderPtr resourceReader1(new ResourceReader(configPath1));
    ASSERT_TRUE(_configReaderAccessor->addConfig(resourceReader1, true));

    string configPath2 = GET_TEST_DATA_PATH() + "/config_reader_accessor_test/config_old";
    configPath2 = fslib::util::FileUtil::normalizeDir(configPath2);
    ResourceReaderPtr resourceReader2(new ResourceReader(configPath2));
    ASSERT_TRUE(_configReaderAccessor->addConfig(resourceReader2, true));

    _configReaderAccessor->deleteConfig("cluster1", 0);
    ASSERT_TRUE(_configReaderAccessor->getLatestConfig());
    ASSERT_EQ(2, _configReaderAccessor->_resourceReaders.size());
}

TEST_F(ConfigReaderAccessorTest, testConfigWithZip)
{
    ConfigReaderAccessorPtr configReaderAccessor(new ConfigReaderAccessor("simple2"));
    string configPath1 = GET_TEST_DATA_PATH() + "/config_reader_accessor_test/config_with_zip";
    configPath1 = fslib::util::FileUtil::normalizeDir(configPath1);
    ResourceReaderPtr resourceReader1(new ResourceReader(configPath1));
    ASSERT_FALSE(configReaderAccessor->addConfig(resourceReader1, true));

    string configPath2 = GET_TEST_DATA_PATH() + "/config_reader_accessor_test/config_with_zip";
    configPath2 = fslib::util::FileUtil::normalizeDir(configPath2);
    ResourceReaderPtr resourceReader2(new ResourceReader(configPath2));
    resourceReader2->init();
    ASSERT_TRUE(configReaderAccessor->addConfig(resourceReader2, true));
    vector<string> clusters;
    ASSERT_TRUE(configReaderAccessor->getAllClusterNames(clusters));
    ASSERT_EQ("cluster1", clusters[0]);
    ASSERT_EQ("cluster2", clusters[1]);
    ASSERT_EQ("cluster3", clusters[2]);
    ASSERT_EQ("cluster4", clusters[3]);
}

}} // namespace build_service::config
