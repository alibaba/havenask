#include "build_service/config/OfflineIndexConfigMap.h"

#include "build_service/test/unittest.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace testing;
using namespace build_service::util;

namespace build_service { namespace config {

class OfflineIndexConfigMapTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void OfflineIndexConfigMapTest::setUp() {}

void OfflineIndexConfigMapTest::tearDown() {}

TEST_F(OfflineIndexConfigMapTest, testJsonizeSimple)
{
    OfflineIndexConfigMap loadConfigMap;
    string configFilePath = GET_TEST_DATA_PATH() + "//offline_index_config_map_test/simple.json";
    string jsonStr;

    fslib::util::FileUtil::readFile(configFilePath, jsonStr);
    ASSERT_FALSE(jsonStr.empty());
    ASSERT_NO_THROW(FromJsonString(loadConfigMap, jsonStr));

    string toJsonStr = ToJsonString(loadConfigMap);
    OfflineIndexConfigMap configMap;
    ASSERT_NO_THROW(FromJsonString(configMap, toJsonStr));

    ASSERT_EQ((size_t)3, configMap.size());
    ASSERT_EQ((size_t)10, configMap[""].buildConfig.keepVersionCount);
    ASSERT_EQ(string(""), configMap[""].offlineMergeConfig.periodMergeDescription);
    ASSERT_EQ((uint32_t)0, configMap[""].offlineMergeConfig.mergeParallelNum);

    ASSERT_EQ((size_t)10, configMap["full"].buildConfig.keepVersionCount);
    ASSERT_EQ(string("period=100"), configMap["full"].offlineMergeConfig.periodMergeDescription);
    ASSERT_EQ((uint32_t)100, configMap["full"].offlineMergeConfig.mergeParallelNum);

    ASSERT_EQ((size_t)10, configMap["inc-merge"].buildConfig.keepVersionCount);
    ASSERT_EQ(string("period=200"), configMap["inc-merge"].offlineMergeConfig.periodMergeDescription);
    ASSERT_EQ((uint32_t)200, configMap["inc-merge"].offlineMergeConfig.mergeParallelNum);
}

TEST_F(OfflineIndexConfigMapTest, testJsonizeNoCustomizedConfig)
{
    OfflineIndexConfigMap configMap;
    string configFilePath = GET_TEST_DATA_PATH() + "//offline_index_config_map_test/no_customized_merge_config.json";
    string jsonStr;
    fslib::util::FileUtil::readFile(configFilePath, jsonStr);
    ASSERT_FALSE(jsonStr.empty());
    ASSERT_NO_THROW(FromJsonString(configMap, jsonStr));
    ASSERT_EQ((size_t)1, configMap.size());
    ASSERT_EQ((size_t)10, configMap[""].buildConfig.keepVersionCount);
    ASSERT_EQ(string(""), configMap[""].offlineMergeConfig.periodMergeDescription);
    ASSERT_EQ((uint32_t)0, configMap[""].offlineMergeConfig.mergeParallelNum);
}

TEST_F(OfflineIndexConfigMapTest, testJsonizeJustCustomizedConfig)
{
    OfflineIndexConfigMap configMap;
    string configFilePath = GET_TEST_DATA_PATH() + "//offline_index_config_map_test/just_customized_merge_config.json";
    string jsonStr;
    fslib::util::FileUtil::readFile(configFilePath, jsonStr);
    ASSERT_FALSE(jsonStr.empty());
    ASSERT_NO_THROW(FromJsonString(configMap, jsonStr));
    ASSERT_EQ((size_t)2, configMap.size());
    ASSERT_EQ((size_t)(indexlib::config::BuildConfig::DEFAULT_KEEP_VERSION_COUNT),
              configMap[""].buildConfig.keepVersionCount);
    ASSERT_EQ(string(""), configMap[""].offlineMergeConfig.periodMergeDescription);
    ASSERT_EQ((uint32_t)0, configMap[""].offlineMergeConfig.mergeParallelNum);

    ASSERT_EQ((size_t)(indexlib::config::BuildConfig::DEFAULT_KEEP_VERSION_COUNT),
              configMap["full"].buildConfig.keepVersionCount);
    ASSERT_EQ(string("period=100"), configMap["full"].offlineMergeConfig.periodMergeDescription);
    ASSERT_EQ((uint32_t)0, configMap["full"].offlineMergeConfig.mergeSleepTime);
    ASSERT_EQ((uint32_t)100, configMap["full"].offlineMergeConfig.mergeParallelNum);
}

}} // namespace build_service::config
