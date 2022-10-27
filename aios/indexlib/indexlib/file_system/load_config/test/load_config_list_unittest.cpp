#include "indexlib/file_system/load_config/test/load_config_list_unittest.h"
#include "indexlib/file_system/load_config/mmap_load_strategy.h"
#include "indexlib/file_system/load_config/cache_load_strategy.h"
#include "indexlib/common_define.h"

using namespace std;
using namespace autil::legacy;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, LoadConfigListTest);

LoadConfigListTest::LoadConfigListTest()
{
}

LoadConfigListTest::~LoadConfigListTest()
{
}

void LoadConfigListTest::CaseSetUp()
{
}

void LoadConfigListTest::CaseTearDown()
{
}

void LoadConfigListTest::TestConfigParse()
{
    LoadConfigList loadConfigList;
    LoadConfig loadConfig;
    loadConfigList.PushBack(loadConfig);

    string resultLoadConfigListStr = ToJsonString(loadConfigList);
    LoadConfigList resultLoadConfigList;
    FromJsonString(resultLoadConfigList, resultLoadConfigListStr);

    INDEXLIB_TEST_EQUAL(resultLoadConfigList.Size(), loadConfigList.Size());
    INDEXLIB_TEST_EQUAL((size_t)1, resultLoadConfigList.Size());
    LoadConfig resultLoadConfig = resultLoadConfigList.GetLoadConfig(0);
    INDEXLIB_TEST_TRUE(resultLoadConfig.EqualWith(loadConfig));
}

void LoadConfigListTest::TestGetTotalCacheSize()
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
    ASSERT_EQ((size_t)512*1024*1024, loadConfigList.GetTotalCacheSize());
}

void LoadConfigListTest::TestLoadConfigName()
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

void LoadConfigListTest::TestLoadConfigDupName()
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
    ASSERT_THROW(loadConfigList.Check(), misc::BadParameterException);
}

void LoadConfigListTest::TestCheck()
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
        ASSERT_THROW(loadConfigList.Check(), misc::BadParameterException);
    }
}

void LoadConfigListTest::TestEnableLoadSpeedLimit()
{
    DoTestLoadSpeedLimit(true);
}

void LoadConfigListTest::TestDisableLoadSpeedLimit()
{
    DoTestLoadSpeedLimit(false);
}

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
    if (isEnable)
    {
        loadConfigList.EnableLoadSpeedLimit();
    }
    else
    {
        loadConfigList.DisableLoadSpeedLimit();
    }

    LoadConfig loadConfig = loadConfigList.GetLoadConfig(0);
    MmapLoadStrategyPtr loadStrategy = DYNAMIC_POINTER_CAST(MmapLoadStrategy, 
            loadConfig.GetLoadStrategy());
    if (isEnable)
    {
        ASSERT_EQ((uint32_t)1000, loadStrategy->GetInterval());
    }
    else
    {
        ASSERT_EQ((uint32_t)0, loadStrategy->GetInterval());
    }
}

IE_NAMESPACE_END(file_system);

