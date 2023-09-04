#include "indexlib/config/TabletOptions.h"

#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace config {

class TabletOptionsTest : public TESTBASE
{
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(TabletOptionsTest, TestSimpleProcess) {}

TEST_F(TabletOptionsTest, TestDefaultOnline)
{
    TabletOptions tabletOptions;
    ASSERT_FALSE(tabletOptions.IsLeader());
    ASSERT_TRUE(tabletOptions.IsOnline());
    ASSERT_TRUE(tabletOptions.FlushLocal());
    ASSERT_FALSE(tabletOptions.FlushRemote());

    // TabletOptions
    ASSERT_EQ(8L << 30 /* 8G */, tabletOptions.GetBuildMemoryQuota());
    ASSERT_FALSE(tabletOptions.GetNeedReadRemoteIndex());
    ASSERT_TRUE(tabletOptions.GetNeedDeployIndex());
    ASSERT_EQ(0, tabletOptions.GetLoadConfigList().Size());

    // OnlineConfig
    auto onlineConfig = tabletOptions.GetOnlineConfig();
    ASSERT_EQ(8L << 30 /* 8G */, onlineConfig.GetMaxRealtimeMemoryUse());
    ASSERT_EQ(1200, onlineConfig.GetPrintMetricsInterval());
    ASSERT_EQ(-1, onlineConfig.GetMaxRealtimeDumpIntervalSecond());
    ASSERT_FALSE(onlineConfig.GetNeedReadRemoteIndex());
    ASSERT_TRUE(onlineConfig.GetNeedDeployIndex());
    ASSERT_EQ(0, onlineConfig.GetLoadConfigList().Size());

    // BuildConfig
    auto buildConfig = tabletOptions.GetBuildConfig();
    ASSERT_EQ(-1, buildConfig.GetBuildingMemoryLimit());
    ASSERT_EQ(5, buildConfig.GetMaxCommitRetryCount());
    ASSERT_EQ((uint64_t)-1, buildConfig.GetMaxDocCount());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), buildConfig.GetMaxDocCount());
    ASSERT_EQ(2, buildConfig.GetKeepVersionCount());
    ASSERT_FALSE(buildConfig.GetIsEnablePackageFile());
}

TEST_F(TabletOptionsTest, TestDefaultOffline)
{
    TabletOptions tabletOptions;
    tabletOptions.SetIsOnline(false);
    ASSERT_FALSE(tabletOptions.IsLeader());
    ASSERT_FALSE(tabletOptions.IsOnline());
    ASSERT_TRUE(tabletOptions.FlushLocal());
    ASSERT_FALSE(tabletOptions.FlushRemote());

    // TabletOptions
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), tabletOptions.GetBuildMemoryQuota());
    ASSERT_FALSE(tabletOptions.GetNeedReadRemoteIndex());
    ASSERT_TRUE(tabletOptions.GetNeedDeployIndex());
    ASSERT_EQ(0, tabletOptions.GetLoadConfigList().Size());

    // OnlineConfig
    auto offlineConfig = tabletOptions.GetOfflineConfig();
    ASSERT_EQ(0, offlineConfig.GetLoadConfigList().Size());

    // BuildConfig
    auto buildConfig = tabletOptions.GetBuildConfig();
    ASSERT_EQ(-1, buildConfig.GetBuildingMemoryLimit());
    ASSERT_EQ(5, buildConfig.GetMaxCommitRetryCount());
    ASSERT_EQ((uint64_t)-1, buildConfig.GetMaxDocCount());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), buildConfig.GetMaxDocCount());
    ASSERT_EQ(2, buildConfig.GetKeepVersionCount());
    ASSERT_TRUE(buildConfig.GetIsEnablePackageFile());

    // MergeConfig
    auto mergeConfig = tabletOptions.GetDefaultMergeConfig();
    ASSERT_EQ(40L << 30 /* 40GB */, mergeConfig.GetMaxMergeMemoryUse());
    ASSERT_EQ(20, mergeConfig.GetMergeThreadCount());
    ASSERT_EQ("optimize", mergeConfig.GetMergeStrategyStr());
}

TEST_F(TabletOptionsTest, TestJsonize)
{
    string jsonStr = R"( {
    "offline_index_config": {
        "build_config": {
            "keep_version_count": 2,
            "max_memory_use": 1024
        },
        "merge_config": {
            "keep_version_count": 2,
            "merge_strategy": "optimize",
            "merge_strategy_param": ""
        },
        "load_config": [
            {
                "file_patterns": [
                    "_SUMMARY_"
                ],
                "load_strategy": "cache",
                "load_strategy_param": {
                    "block_size": 4096,
                    "cache_size": 10,
                    "direct_io": true
                },
                "name": "_SUMMARY_",
                "warmup_strategy": "none"
            }
        ]
    },
    "online_index_config": {
        "allow_locator_rollback": true,
        "need_read_remote_index": true,
        "need_deploy_index": false,
        "load_config": [
            {
                "file_patterns": [
                    "_SUMMARY_"
                ],
                "load_strategy": "cache",
                "load_strategy_param": {
                    "block_size": 4096,
                    "cache_size": 10,
                    "direct_io": true
                },
                "name": "_SUMMARY_",
                "warmup_strategy": "none"
            }
        ]
    }
    } )";
    {
        TabletOptions tabletOptions;
        tabletOptions.SetIsOnline(true);
        FromJsonString(tabletOptions, jsonStr);
        ASSERT_EQ(1, tabletOptions.GetLoadConfigList().Size());
        ASSERT_TRUE(tabletOptions.GetLoadConfigList().GetLoadConfig(0).IsRemote());
        ASSERT_TRUE(tabletOptions.GetOnlineConfig().GetAllowLocatorRollback());
    }
    {
        TabletOptions tabletOptions;
        tabletOptions.SetIsOnline(false);
        FromJsonString(tabletOptions, jsonStr);
        ASSERT_EQ(1, tabletOptions.GetLoadConfigList().Size());
        ASSERT_EQ("_SUMMARY_", tabletOptions.GetLoadConfigList().GetLoadConfig(0).GetName());
    }
}

TEST_F(TabletOptionsTest, TestJsonizeCheck)
{
    TabletOptions tabletOptions;
    ASSERT_NO_THROW(FromJsonString(tabletOptions, "{}"));
    ASSERT_NO_THROW(FromJsonString(tabletOptions, R"( {"offline_index_config":{}} )"));
    ASSERT_NO_THROW(FromJsonString(tabletOptions, R"( {"online_index_config":{}} )"));

    ASSERT_THROW(FromJsonString(tabletOptions, ""), autil::legacy::ParameterInvalidException);
    // ASSERT_THROW(
    //     FromJsonString(tabletOptions, R"( {"offline_index_config":{"build_config":{"keep_version_count":0 }}} )"),
    //     autil::legacy::ParameterInvalidException);
}

TEST_F(TabletOptionsTest, TestDeserializeSimple)
{
    TabletOptions tabletOptions;

    // defalut
    ASSERT_TRUE(tabletOptions.GetOnlineConfig().GetNeedDeployIndex());

    // empty content
    FromJsonString(tabletOptions, "{}");
    ASSERT_TRUE(tabletOptions.GetOnlineConfig().GetNeedDeployIndex());

    // normal
    string jsonStr = R"( {
        "online_index_config" : {
            "need_deploy_index" : false
        }
    } )";
    FromJsonString(tabletOptions, jsonStr);
    ASSERT_FALSE(tabletOptions.GetOnlineConfig().GetNeedDeployIndex());
}

TEST_F(TabletOptionsTest, TestDeserializeNormal)
{
    string jsonStr = R"( {
    "build_option_config": {
        "sort_build": false
    },
    "cluster_config": {
        "cluster_name": "simple",
        "hash_mode": {
            "hash_field": "id",
            "hash_function": "HASH"
        },
        "table_name": "simple"
    },
    "offline_index_config": {
        "build_config": {
            "keep_version_count": 2,
            "max_memory_use": 1024
        },
        "merge_config": {
            "keep_version_count": 2,
            "merge_strategy": "optimize",
            "merge_strategy_param": ""
        }
    },
    "online_index_config": {
        "need_read_remote_index": true,
        "load_config": [
            {
                "file_patterns": [
                    "_SUMMARY_"
                ],
                "load_strategy": "cache",
                "load_strategy_param": {
                    "block_size": 4096,
                    "cache_size": 10,
                    "direct_io": true
                },
                "remote": true,
                "name": "_SUMMARY_",
                "warmup_strategy": "none"
            }
        ]
    }
    } )";
    TabletOptions tabletOptions;
    ASSERT_EQ(0, tabletOptions.GetOnlineConfig().GetLoadConfigList().Size());
    FromJsonString(tabletOptions, jsonStr);
    ASSERT_EQ(1, tabletOptions.GetLoadConfigList().Size());
    ASSERT_TRUE(tabletOptions.GetOnlineConfig().GetLoadConfigList().GetLoadConfig(0).IsRemote());

    string expectJsonStr = ToJsonString(tabletOptions, true);

    // copy construction
    TabletOptions tabletOptions2(tabletOptions);
    ASSERT_EQ(expectJsonStr, ToJsonString(tabletOptions2, true)) << expectJsonStr << ToJsonString(tabletOptions2, true);

    // assignment operator
    TabletOptions tabletOptions3;
    tabletOptions3 = tabletOptions;
    ASSERT_EQ(expectJsonStr, ToJsonString(tabletOptions3, true)) << expectJsonStr << ToJsonString(tabletOptions3, true);
}

TEST_F(TabletOptionsTest, TestIndexTaskConfig)
{
    string jsonStr = R"( {
    "build_option_config": {
        "sort_build": false
    },
    "offline_index_config": {
        "build_config": {
            "keep_version_count": 2,
            "max_memory_use": 1024
        },
        "merge_config": {
            "keep_version_count": 2,
            "merge_strategy": "optimize",
            "merge_strategy_param": ""
        },
        "index_task_configs" : [
            {
                "task_type" : "merge",
                "task_name" : "merge_inc",
                "trigger" : "period=300",
                "settings" : {
                    "merge_config": {
                         "merge_strategy" : "optimize",
                         "merge_strategy_param" : ""
                    }
                }
            },
            {
                "task_type" : "reclaim",
                "task_name" : "reclaim_index",
                "trigger" : "daytime=10:30"
            }
        ]
    },
    "online_index_config": {
        "load_config": [
            {
                "file_patterns": [
                    "_SUMMARY_"
                ],
                "load_strategy": "cache",
                "load_strategy_param": {
                    "block_size": 4096,
                    "cache_size": 10,
                    "direct_io": true
                },
                "remote": true,
                "name": "_SUMMARY_",
                "warmup_strategy": "none"
            }
        ]
    }
    } )";

    TabletOptions tmp;
    FromJsonString(tmp, jsonStr);
    std::string toJsonStr = ToJsonString(tmp);

    TabletOptions tabletOptions;
    FromJsonString(tabletOptions, toJsonStr);
    // copy construction
    TabletOptions tabletOptions2(tabletOptions);

    // assignment operator
    TabletOptions tabletOptions3;
    tabletOptions3 = tabletOptions2;

    ASSERT_EQ(2, tabletOptions3.GetAllIndexTaskConfigs().size());
    auto config = tabletOptions3.GetIndexTaskConfig("merge", "merge_inc");
    ASSERT_TRUE(config);
    config = tabletOptions3.GetIndexTaskConfig("non_exist", "name");
    ASSERT_EQ(nullopt, config);

    string errorJsonStr = R"( {
    "build_option_config": {
        "sort_build": false
    },
    "offline_index_config": {
        "build_config": {
            "keep_version_count": 2,
            "max_memory_use": 1024
        },
        "merge_config": {
            "keep_version_count": 2,
            "merge_strategy": "optimize",
            "merge_strategy_param": ""
        },
        "index_task_configs" : [
            {
                "task_type" : "merge",
                "task_name" : "merge_inc",
                "trigger" : "period=300",
                "settings" : {
                    "merge_config": {
                         "merge_strategy" : "optimize",
                         "merge_strategy_param" : ""
                    }
                }
            },
            {
                "task_type" : "merge",
                "task_name" : "merge_inc",
                "trigger" : "daytime=10:30"
            }
        ]
    },
    "online_index_config": {
        "load_config": [
            {
                "file_patterns": [
                    "_SUMMARY_"
                ],
                "load_strategy": "cache",
                "load_strategy_param": {
                    "block_size": 4096,
                    "cache_size": 10,
                    "direct_io": true
                },
                "remote": true,
                "name": "_SUMMARY_",
                "warmup_strategy": "none"
            }
        ]
    }
    } )";

    // test taskType & taskName duplicated
    ASSERT_ANY_THROW(FromJsonString(tmp, errorJsonStr));

    errorJsonStr = R"( {
    "build_option_config": {
        "sort_build": false
    },
    "offline_index_config": {
        "build_config": {
            "keep_version_count": 2,
            "max_memory_use": 1024
        },
        "merge_config": {
            "keep_version_count": 2,
            "merge_strategy": "optimize",
            "merge_strategy_param": ""
        },
        "index_task_configs" : [
            {
                "task_type" : "merge",
                "task_name" : "",
                "trigger" : "period=300",
                "settings" : {
                    "merge_config": {
                         "merge_strategy" : "optimize",
                         "merge_strategy_param" : ""
                    }
                }
            }
        ]
    },
    "online_index_config": {
        "load_config": [
            {
                "file_patterns": [
                    "_SUMMARY_"
                ],
                "load_strategy": "cache",
                "load_strategy_param": {
                    "block_size": 4096,
                    "cache_size": 10,
                    "direct_io": true
                },
                "remote": true,
                "name": "_SUMMARY_",
                "warmup_strategy": "none"
            }
        ]
    }
    } )";
    // test taskType or taskName is empty string
    ASSERT_ANY_THROW(FromJsonString(tmp, errorJsonStr));
}

TEST_F(TabletOptionsTest, TestCheckMemory)
{
    {
        string jsonStr = R"( {
            "online_index_config": {
            "max_realtime_memory_use":1024,
            "build_config":{
                "building_memory_limit_mb":2048
            }
        }
        } )";

        TabletOptions option;
        FromJsonString(option, jsonStr);
        option.SetIsOnline(true);
        option.SetFlushRemote(false);
        option.SetFlushLocal(true);
        ASSERT_FALSE(option.Check("", "localPath").IsOK());
    }
    {
        string jsonStr = R"( {
            "online_index_config": {
            "max_realtime_memory_use":1024,
            "build_config":{
                "building_memory_limit_mb":512
            }
        }
        } )";

        TabletOptions option;
        FromJsonString(option, jsonStr);
        option.SetIsOnline(true);
        option.SetFlushRemote(false);
        option.SetFlushLocal(true);
        ASSERT_TRUE(option.Check("", "localPath").IsOK());
    }
    {
        string jsonStr = R"( {
            "online_index_config": {
            "max_realtime_memory_use":1024,
            "build_config":{
                "building_memory_limit_mb":1024
            }
        }
        } )";

        TabletOptions option;
        FromJsonString(option, jsonStr);
        option.SetIsOnline(true);
        option.SetFlushRemote(false);
        option.SetFlushLocal(true);
        ASSERT_TRUE(option.Check("", "localPath").IsOK());
    }
}

TEST_F(TabletOptionsTest, TestGetFromRawJson)
{
    TabletOptions tabletOptions;
    string jsonStr = R"( {
        "online_index_config" : {
            "need_deploy_index" : false,
            "a" : {
                "b" : {
                    "c" : 8
                }
            }
        }
    } )";
    FromJsonString(tabletOptions, jsonStr);
    int value = 0;
    ASSERT_TRUE(tabletOptions.GetFromRawJson("online_index_config.a.b.c", &value).IsOK());
    ASSERT_EQ(8, value);
}

TEST_F(TabletOptionsTest, TestCheck)
{
    // normal
    string jsonStr = R"( {
        "online_index_config" : {
            "need_deploy_index" : false,
            "disable_fields" : {
                "attributes" : ["index_1"]
            }
        }
    } )";
    TabletOptions tabletOptions;
    FromJsonString(tabletOptions, jsonStr);
    ASSERT_FALSE(tabletOptions.Check("remote", "local").IsOK());
}

}} // namespace indexlibv2::config
