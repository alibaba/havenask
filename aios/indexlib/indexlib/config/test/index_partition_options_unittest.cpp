#include "indexlib/config/test/index_partition_options_unittest.h"
#include "indexlib/config/build_config_base.h"
#include "indexlib/config/merge_config_base.h"
#include "indexlib/config/offline_config_base.h"
#include "indexlib/config/online_config_base.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, IndexPartitionOptionsTest);

IndexPartitionOptionsTest::IndexPartitionOptionsTest()
{
}

IndexPartitionOptionsTest::~IndexPartitionOptionsTest()
{
}

void IndexPartitionOptionsTest::CaseSetUp()
{
}

void IndexPartitionOptionsTest::CaseTearDown()
{
}

void IndexPartitionOptionsTest::TestJsonize()
{
    string jsonStr =              \
    "{                            \
       \"online_index_config\" :  \
        {                                                             \
            \"online_keep_version_count\" : 4,                        \
            \"on_disk_flush_realtime_index\" : true,                  \
            \"build_config\" : {                                      \
                \"keep_version_count\" : 3                            \
            }                                                         \
        },                                                            \
       \"offline_index_config\" :                                     \
       {                                                              \
            \"build_config\" :                                        \
            {                                                         \
                \"keep_version_count\" : 4                            \
            },                                                        \
            \"merge_config\" :                                        \
            {                                                         \
                \"max_merge_memory_use\" : 200                        \
            }                                                         \
       }                                                              \
    }";
    IndexPartitionOptions indexPartitionOptions;
    FromJsonString(indexPartitionOptions, jsonStr);
    const OnlineConfig& onlineConfig = indexPartitionOptions.GetOnlineConfig(); 
    ASSERT_EQ((uint32_t)4, onlineConfig.onlineKeepVersionCount);
    ASSERT_EQ(true, onlineConfig.onDiskFlushRealtimeIndex);

    const OfflineConfig& offlineConfig = indexPartitionOptions.GetOfflineConfig();
    ASSERT_EQ((int64_t)200, offlineConfig.mergeConfig.maxMemUseForMerge);

    indexPartitionOptions.SetIsOnline(true);
    const BuildConfig& onlineBuildConfig = indexPartitionOptions.GetBuildConfig();
    ASSERT_EQ((uint32_t)3, onlineBuildConfig.keepVersionCount);

    indexPartitionOptions.SetIsOnline(false);
    const BuildConfig& offlineBuildConfig = indexPartitionOptions.GetBuildConfig();
    ASSERT_EQ((uint32_t)4, offlineBuildConfig.keepVersionCount);
}

void IndexPartitionOptionsTest::TestGetBuildConfig()
{
    IndexPartitionOptions options;

    const BuildConfig& constBuildConf = options.GetBuildConfig(true);
    BuildConfig& buildConfig = options.GetBuildConfig(true);

    buildConfig.maxDocCount = 100;
    ASSERT_EQ((uint32_t)100, constBuildConf.maxDocCount);

    ASSERT_FALSE(options.GetBuildConfig(true).usePathMetaCache);
    ASSERT_TRUE(options.GetBuildConfig(false).usePathMetaCache);
}

IE_NAMESPACE_END(config);

