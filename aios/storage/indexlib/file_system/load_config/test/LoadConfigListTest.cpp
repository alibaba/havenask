#include "indexlib/file_system/load_config/LoadConfigList.h"

#include "gtest/gtest.h"
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <stdint.h>

#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/util/Exception.h"
#include "unittest/unittest.h"

namespace indexlib { namespace util {
class BadParameterException;
}} // namespace indexlib::util

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace file_system {
class LoadConfigListTest : public TESTBASE
{
private:
    void DoTestLoadSpeedLimit(bool isEnable);
};

TEST_F(LoadConfigListTest, TestConfigParse)
{
    LoadConfigList loadConfigList;
    LoadConfig loadConfig;
    loadConfigList.PushBack(loadConfig);

    string resultLoadConfigListStr = ToJsonString(loadConfigList);
    LoadConfigList resultLoadConfigList;
    FromJsonString(resultLoadConfigList, resultLoadConfigListStr);

    ASSERT_EQ(resultLoadConfigList.Size(), loadConfigList.Size());
    ASSERT_EQ((size_t)1, resultLoadConfigList.Size());
    LoadConfig resultLoadConfig = resultLoadConfigList.GetLoadConfig(0);
    ASSERT_TRUE(resultLoadConfig.EqualWith(loadConfig));
}

TEST_F(LoadConfigListTest, TestGetTotalCacheSize)
{
    string jsonStr = "\
    {                                                    \
        \"file_patterns\" : [\"_ATTRIBUTE_\"],           \
        \"load_strategy\" : \"cache\",                   \
        \"load_strategy_param\" : {                      \
            \"cache_size\" : 512                         \
        }                                                \
    }";
    LoadConfig loadConfig;
    FromJsonString(loadConfig, jsonStr);

    LoadConfigList loadConfigList;
    loadConfigList.PushBack(loadConfig);
    ASSERT_EQ((size_t)512 * 1024 * 1024, loadConfigList.GetTotalCacheMemorySize());
}

TEST_F(LoadConfigListTest, TestLoadConfigName)
{
    string jsonStr = "                                                \
    {                                                                 \
    \"load_config\" :                                                 \
    [                                                                 \
    {                                                                 \
        \"file_patterns\" : [\"_ATTRIBUTE_\"],                        \
        \"load_strategy\" : \"mmap\"                                  \
    },                                                                \
    {                                                                 \
        \"file_patterns\" : [\"_SUMMARY_\"],                          \
        \"load_strategy\" : \"cache\",                                \
        \"name\" : \"summary\"                                        \
    },                                                                \
    {                                                                 \
        \"file_patterns\" : [\"_INDEX_\"],                            \
        \"load_strategy\" : \"mmap\"                                  \
    }                                                                 \
    ]                                                                 \
    }                                                                 \
    ";
    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, jsonStr);
    ASSERT_NO_THROW(loadConfigList.Check());
    ASSERT_EQ((size_t)3, loadConfigList.Size());
    ASSERT_EQ("load_config0", loadConfigList.GetLoadConfigs()[0].GetName());
    ASSERT_EQ("summary", loadConfigList.GetLoadConfigs()[1].GetName());
    ASSERT_EQ("load_config2", loadConfigList.GetLoadConfigs()[2].GetName());
}

TEST_F(LoadConfigListTest, TestLoadConfigDupName)
{
    string jsonStr = "                                                 \
    {                                                                  \
    \"load_config\" :                                                   \
    [                                                                  \
    {                                                                  \
        \"file_patterns\" : [\"_SUMMARY_\"],                           \
        \"load_strategy\" : \"cache\",                                 \
        \"name\" : \"dup_name\"                                        \
    },                                                                 \
    {                                                                  \
        \"file_patterns\" : [\"_INDEX_\"],                             \
        \"load_strategy\" : \"mmap\",                                  \
        \"name\" : \"dup_name\"                                        \
    }                                                                  \
    ]                                                                  \
    }                                                                  \
    ";
    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, jsonStr);
    ASSERT_THROW(loadConfigList.Check(), util::BadParameterException);
}

TEST_F(LoadConfigListTest, TestCheck)
{
    {
        LoadConfigList loadConfigList;
        FromJsonString(loadConfigList, R"( {
            "load_config": [
                {"file_patterns": ["attribute"], "remote": true, "deploy": true }
            ]
        } )");
        ASSERT_NO_THROW(loadConfigList.Check());
    }
    {
        LoadConfigList loadConfigList;
        FromJsonString(loadConfigList, R"( {
            "load_config": [
                {"file_patterns": ["attribute"], "remote": false, "deploy": false }
            ]
        } )");
        ASSERT_THROW(loadConfigList.Check(), util::BadParameterException);
    }
}

TEST_F(LoadConfigListTest, TestEnableLoadSpeedLimit) { DoTestLoadSpeedLimit(true); }

TEST_F(LoadConfigListTest, TestDisableLoadSpeedLimit) { DoTestLoadSpeedLimit(false); }

void LoadConfigListTest::DoTestLoadSpeedLimit(bool isEnable)
{
    string jsonStr = "                                                 \
    {                                                                  \
    \"load_config\" :                                                   \
    [{                                                                 \
        \"file_patterns\" : [\".*\"],                                  \
        \"load_strategy\" : \"mmap\",                                  \
        \"load_strategy_param\" : {                                    \
            \"lock\" : true,                                           \
            \"slice\" : 4096,                                          \
            \"interval\" : 1000                                        \
        }                                                              \
    }]                                                                 \
    }                                                                  \
    ";
    LoadConfigList loadConfigList;
    FromJsonString(loadConfigList, jsonStr);
    loadConfigList.SwitchLoadSpeedLimit(isEnable);

    LoadConfig loadConfig = loadConfigList.GetLoadConfig(0);
    MmapLoadStrategyPtr loadStrategy = std::dynamic_pointer_cast<MmapLoadStrategy>(loadConfig.GetLoadStrategy());
    if (isEnable) {
        ASSERT_EQ((uint32_t)1000, loadStrategy->GetInterval());
    } else {
        ASSERT_EQ((uint32_t)0, loadStrategy->GetInterval());
    }
}
}} // namespace indexlib::file_system
