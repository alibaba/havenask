#include "indexlib/config/test/online_config_unittest.h"

#include "autil/EnvUtil.h"
#include "indexlib/config/disable_fields_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/LifecycleConfig.h"
#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, OnlineConfigTest);

OnlineConfigTest::OnlineConfigTest() {}

OnlineConfigTest::~OnlineConfigTest() {}

void OnlineConfigTest::CaseSetUp() { unsetenv("IS_INDEXLIB_TEST_MODE"); }

void OnlineConfigTest::CaseTearDown() {}

void OnlineConfigTest::TestJsonize()
{
    string jsonStr = R"(
    {
        "max_operation_queue_block_size" : 100,
        "var_num_attribute_memory_reclaim_threshold" : 2048,
        "max_realtime_memory_use" : 4096,
        "max_realtime_dump_interval" : 40,
        "online_keep_version_count" : 4,
        "on_disk_flush_realtime_index" : true,
        "is_inc_consistent_with_realtime" : true,
        "enable_load_speed_control_for_open" : true,
        "disable_sse_pfor_delta_optimize" : true,
        "enable_validate_index" : false,
        "enable_redo_speedup" : true,
        "version_timestamp_alignment" : 2000000,
        "enable_reopen_retry_io_exception" : false,
        "is_optimized_reopen" : true,
        "operation_log_flush_to_disk": true,
        "load_config" :
        [
            {
                "file_patterns" : ["_ATTRIBUTE_"],
                "load_strategy" : "mmap"
            }
        ],
        "lifecycle_config": {
            "strategy" : "static",
            "enable_local_deploy_manifest" : true
        },
        "build_config" : {
            "keep_version_count" : 3
        },
        "index_dictionary_bloom_filter_param" :  [
           {
              "index_name" : "index1",
              "multiple_num" : 7
           },
           {
              "index_name" : "index2"
           }
       ]
    })";

    OnlineConfig onlineConfig;
    ASSERT_FALSE(onlineConfig.IsRedoSpeedupEnabled());
    ASSERT_TRUE(onlineConfig.IsReopenRetryOnIOExceptionEnabled());
    ASSERT_FALSE(onlineConfig.EnableLocalDeployManifestChecking());
    ASSERT_EQ(file_system::LifecycleConfig::STATIC_STRATEGY, onlineConfig.GetLifecycleConfig().GetStrategy());
    ASSERT_FALSE(onlineConfig.GetLifecycleConfig().EnableLocalDeployManifestChecking());

    FromJsonString(onlineConfig, jsonStr);

    ASSERT_EQ((size_t)100, onlineConfig.maxOperationQueueBlockSize);
    ASSERT_TRUE(onlineConfig.OperationLogFlushToDiskEnabled());
    ASSERT_EQ((int64_t)2048, onlineConfig.maxCurReaderReclaimableMem);
    ASSERT_EQ((int64_t)4096, onlineConfig.maxRealtimeMemSize);
    ASSERT_EQ((uint32_t)40, onlineConfig.maxRealtimeDumpInterval);
    ASSERT_EQ((uint32_t)4, onlineConfig.onlineKeepVersionCount);
    ASSERT_TRUE(onlineConfig.onDiskFlushRealtimeIndex);
    ASSERT_TRUE(onlineConfig.isIncConsistentWithRealtime);
    ASSERT_TRUE(onlineConfig.enableLoadSpeedControlForOpen);
    ASSERT_TRUE(onlineConfig.disableSsePforDeltaOptimize);
    ASSERT_EQ((size_t)1, onlineConfig.loadConfigList.Size());
    ASSERT_TRUE(onlineConfig.IsRedoSpeedupEnabled());
    ASSERT_FALSE(onlineConfig.IsValidateIndexEnabled());
    ASSERT_FALSE(onlineConfig.IsReopenRetryOnIOExceptionEnabled());
    ASSERT_TRUE(onlineConfig.IsOptimizedReopen());
    ASSERT_EQ((int64_t)2000000, onlineConfig.GetVersionTsAlignment());

    auto params = onlineConfig.GetIndexDictBloomFilterParams();
    ASSERT_EQ(2, params.size());
    ASSERT_EQ(string("index1"), params[0].indexName);
    ASSERT_EQ(7, params[0].multipleNum);
    ASSERT_EQ(string("index2"), params[1].indexName);
    ASSERT_EQ(5, params[1].multipleNum);

    ASSERT_EQ((int64_t)2000000, onlineConfig.GetVersionTsAlignment());
    ASSERT_TRUE(onlineConfig.EnableLocalDeployManifestChecking());
    ASSERT_EQ(file_system::LifecycleConfig::STATIC_STRATEGY, onlineConfig.GetLifecycleConfig().GetStrategy());
    ASSERT_TRUE(onlineConfig.GetLifecycleConfig().EnableLocalDeployManifestChecking());

    auto checkFunc = [](OnlineConfig& otherConfig) {
        ASSERT_EQ((size_t)100, otherConfig.maxOperationQueueBlockSize);
        ASSERT_TRUE(otherConfig.OperationLogFlushToDiskEnabled());
        ASSERT_EQ((int64_t)2048, otherConfig.maxCurReaderReclaimableMem);
        ASSERT_EQ((int64_t)4096, otherConfig.maxRealtimeMemSize);
        ASSERT_EQ((uint32_t)40, otherConfig.maxRealtimeDumpInterval);
        ASSERT_EQ((uint32_t)4, otherConfig.onlineKeepVersionCount);
        ASSERT_TRUE(otherConfig.onDiskFlushRealtimeIndex);
        ASSERT_TRUE(otherConfig.isIncConsistentWithRealtime);
        ASSERT_TRUE(otherConfig.enableLoadSpeedControlForOpen);
        ASSERT_TRUE(otherConfig.disableSsePforDeltaOptimize);
        ASSERT_EQ((size_t)1, otherConfig.loadConfigList.Size());
        ASSERT_TRUE(otherConfig.IsRedoSpeedupEnabled());
        ASSERT_FALSE(otherConfig.IsValidateIndexEnabled());
        ASSERT_FALSE(otherConfig.IsReopenRetryOnIOExceptionEnabled());
        ASSERT_TRUE(otherConfig.IsOptimizedReopen());
        ASSERT_EQ(int64_t(2000000), otherConfig.GetVersionTsAlignment());
        ASSERT_TRUE(otherConfig.EnableLocalDeployManifestChecking());
        ASSERT_EQ(file_system::LifecycleConfig::STATIC_STRATEGY, otherConfig.GetLifecycleConfig().GetStrategy());
        ASSERT_TRUE(otherConfig.GetLifecycleConfig().EnableLocalDeployManifestChecking());
    };
    {
        OnlineConfig otherConfig = onlineConfig; // copy constructor
        checkFunc(otherConfig);
    }
    {
        OnlineConfig otherConfig;
        otherConfig = onlineConfig; // operator=
        checkFunc(otherConfig);
    }
    {
        string jsonStr = ToJsonString(onlineConfig);
        OnlineConfig otherConfig;
        FromJsonString(otherConfig, jsonStr);
        checkFunc(otherConfig);
    }
    BuildConfig expectBuildConfig;
    expectBuildConfig.keepVersionCount = 3;
    expectBuildConfig.buildTotalMemory = OnlineConfig::DEFAULT_BUILD_TOTAL_MEMORY;
    expectBuildConfig.hashMapInitSize = OnlineConfig::DEFAULT_REALTIME_BUILD_HASHMAP_INIT_SIZE;
    expectBuildConfig.dumpThreadCount = OnlineConfig::DEFAULT_REALTIME_DUMP_THREAD_COUNT;
    expectBuildConfig.enablePackageFile = OnlineConfig::DEFAULT_REALTIME_BUILD_USE_PACKAGE_FILE;
    expectBuildConfig.usePathMetaCache = false;
    expectBuildConfig.buildPhase = indexlib::config::BuildConfigBase::BP_RT;
    ASSERT_EQ(expectBuildConfig, onlineConfig.buildConfig) << ToJsonString(expectBuildConfig) << endl
                                                           << ToJsonString(onlineConfig.buildConfig);
}

void OnlineConfigTest::TestCheck()
{
    OnlineConfig onlineConfig;
    ASSERT_NO_THROW(onlineConfig.Check());

    {
        OnlineConfig abnormalOnlineConfig = onlineConfig;
        abnormalOnlineConfig.onlineKeepVersionCount = 0;
        ASSERT_THROW(abnormalOnlineConfig.Check(), util::BadParameterException);
    }
    {
        OnlineConfig abnormalOnlineConfig = onlineConfig;
        abnormalOnlineConfig.maxRealtimeMemSize = DEFAULT_CHUNK_SIZE * 2 - 1;
        ASSERT_THROW(abnormalOnlineConfig.Check(), util::BadParameterException);
    }
    {
        OnlineConfig abnormalOnlineConfig = onlineConfig;
        abnormalOnlineConfig.buildConfig.keepVersionCount = 0;
        ASSERT_THROW(abnormalOnlineConfig.Check(), util::BadParameterException);
    }
    {
        OnlineConfig abnormalOnlineConfig = onlineConfig;
        LoadConfig loadConf;
        loadConf.SetName("load_config_1");
        abnormalOnlineConfig.loadConfigList.PushBack(loadConf);
        abnormalOnlineConfig.loadConfigList.PushBack(loadConf);
        ASSERT_THROW(abnormalOnlineConfig.Check(), util::BadParameterException);
    }
    {
        OnlineConfig abnormalOnlineConfig = onlineConfig;
        abnormalOnlineConfig.maxRealtimeMemSize = 256;
        ASSERT_THROW(abnormalOnlineConfig.Check(), util::BadParameterException);
    }
    {
        OnlineConfig abnormalOnlineConfig = onlineConfig;
        abnormalOnlineConfig.onDiskFlushRealtimeIndex = false;
        abnormalOnlineConfig.prohibitInMemDump = true;
        ASSERT_THROW(abnormalOnlineConfig.Check(), util::BadParameterException);
    }
}

void OnlineConfigTest::TestGetMaxReopenMemoryUse()
{
    OnlineConfig onlineConfig;
    int64_t maxReopenMemUse;
    ASSERT_FALSE(onlineConfig.GetMaxReopenMemoryUse(maxReopenMemUse));

    onlineConfig.SetMaxReopenMemoryUse(1024);
    ASSERT_TRUE(onlineConfig.GetMaxReopenMemoryUse(maxReopenMemUse));
    ASSERT_EQ((int64_t)1024, maxReopenMemUse);
}

void OnlineConfigTest::TestNeedDeployIndex()
{
    // Default
    {
        SCOPED_TRACE("default");
        string jsonStr = R"( {
            "load_config" : [
                { "file_patterns" : ["_ATTRIBUTE_"], "load_strategy" : "mmap" }
            ]
        } )";

        OnlineConfig onlineConfig;
        FromJsonString(onlineConfig, jsonStr);

        ASSERT_TRUE(onlineConfig.NeedDeployIndex());
        ASSERT_FALSE(onlineConfig.NeedReadRemoteIndex());
        const auto& loadConfigList = onlineConfig.loadConfigList;
        ASSERT_FALSE(loadConfigList.GetDefaultLoadConfig().IsRemote());
        ASSERT_TRUE(loadConfigList.GetDefaultLoadConfig().NeedDeploy());
        ASSERT_FALSE(loadConfigList.GetLoadConfig(0).IsRemote());
        ASSERT_TRUE(loadConfigList.GetLoadConfig(0).NeedDeploy());
    }
    {
        SCOPED_TRACE("illegal");
        string jsonStr = R"( {
            "need_read_remote_index" : true,
            "load_config" : [
                { "file_patterns" : ["_ATTRIBUTE_"], "load_strategy" : "mmap" },
                { "file_patterns" : ["_INDEX_"], "load_strategy" : "mmap", "deploy": false }
            ]
        } )";

        OnlineConfig onlineConfig;
        ASSERT_THROW(FromJsonString(onlineConfig, jsonStr), util::BadParameterException);
    }
    {
        SCOPED_TRACE("normal");
        string jsonStr = R"( {
            "need_deploy_index" : true,
            "need_read_remote_index" : true,
            "load_config" : [
                { "file_patterns" : ["_ATTRIBUTE_"], "load_strategy" : "mmap", "remote": true },
                { "file_patterns" : ["_INDEX_"], "load_strategy" : "mmap", "remote": true, "deploy": false }
            ]
        } )";

        OnlineConfig onlineConfig;
        FromJsonString(onlineConfig, jsonStr);

        ASSERT_TRUE(onlineConfig.NeedDeployIndex());
        ASSERT_TRUE(onlineConfig.NeedReadRemoteIndex());
        const auto& loadConfigList = onlineConfig.loadConfigList;
        ASSERT_FALSE(loadConfigList.GetDefaultLoadConfig().IsRemote());
        ASSERT_TRUE(loadConfigList.GetDefaultLoadConfig().NeedDeploy());
        ASSERT_TRUE(loadConfigList.GetLoadConfig(0).IsRemote());
        ASSERT_TRUE(loadConfigList.GetLoadConfig(0).NeedDeploy());

        ASSERT_TRUE(loadConfigList.GetLoadConfig(1).IsRemote());
        ASSERT_FALSE(loadConfigList.GetLoadConfig(1).NeedDeploy());
    }
    {
        SCOPED_TRACE("rewrite load config remote to false when not need_read_remote_index and need_deploy_index");
        string jsonStr = R"( {
            "need_read_remote_index" : false,
            "load_config" : [
                { "file_patterns" : ["_ATTRIBUTE_"], "load_strategy" : "mmap", "remote": true, "deploy": false }
            ]
        } )";

        OnlineConfig onlineConfig;
        FromJsonString(onlineConfig, jsonStr);

        ASSERT_TRUE(onlineConfig.NeedDeployIndex());
        ASSERT_FALSE(onlineConfig.NeedReadRemoteIndex());
        const auto& loadConfigList = onlineConfig.loadConfigList;
        ASSERT_FALSE(loadConfigList.GetDefaultLoadConfig().IsRemote());
        ASSERT_FALSE(loadConfigList.GetLoadConfig(0).IsRemote());
        ASSERT_TRUE(loadConfigList.GetLoadConfig(0).NeedDeploy());
    }
    {
        SCOPED_TRACE("need_deploy_index will rewrite need_read_remote_index and load_config");
        string jsonStr = R"( {
            "need_deploy_index" : false,
            "load_config" : [
                { "file_patterns" : ["_ATTRIBUTE_"], "load_strategy" : "mmap", "deploy": true}
            ]
        } )";

        OnlineConfig onlineConfig;
        FromJsonString(onlineConfig, jsonStr);

        ASSERT_FALSE(onlineConfig.NeedDeployIndex());
        ASSERT_TRUE(onlineConfig.NeedReadRemoteIndex());
        const auto& loadConfigList = onlineConfig.loadConfigList;
        ASSERT_TRUE(loadConfigList.GetDefaultLoadConfig().IsRemote());
        ASSERT_TRUE(loadConfigList.GetLoadConfig(0).IsRemote());
        ASSERT_FALSE(loadConfigList.GetLoadConfig(0).NeedDeploy());
    }
    {
        SCOPED_TRACE("need_read_remote_index will rewrite load_config");
        string jsonStr = R"( {
            "need_read_remote_index" : false,
            "load_config" : [
                { "file_patterns" : ["_ATTRIBUTE_"], "load_strategy" : "mmap", "deploy": false, "remote": false}
            ]
        } )";

        OnlineConfig onlineConfig;
        FromJsonString(onlineConfig, jsonStr);

        ASSERT_TRUE(onlineConfig.NeedDeployIndex());
        ASSERT_FALSE(onlineConfig.NeedReadRemoteIndex());
        const auto& loadConfigList = onlineConfig.loadConfigList;
        ASSERT_FALSE(loadConfigList.GetDefaultLoadConfig().IsRemote());
        ASSERT_FALSE(loadConfigList.GetLoadConfig(0).IsRemote());
        ASSERT_TRUE(loadConfigList.GetLoadConfig(0).NeedDeploy());
    }
}

void OnlineConfigTest::TestDisableFields()
{
    string jsonStr = R"( {
        "need_read_remote_index" : false,
        "load_config" : [ {
             "file_patterns" : ["_ATTRIBUTE_"],
             "load_strategy" : "mmap",
             "deploy": false,
             "remote": false
         } ],
        "disable_fields": {
            "attributes": ["attr1", "attr2"],
            "indexes": ["idx1", "idx2"],
            "pack_attributes": ["pack_attr_1"],
            "summarys":"__SUMMARY_FIELD_ALL__",
            "sources":"__SOURCE_FIELD_ALL__",
            "rewrite_load_config": true
        }
    } )";

    OnlineConfig onlineConfig;
    FromJsonString(onlineConfig, jsonStr);
    ASSERT_NO_THROW(onlineConfig.Check());

    const auto& disableFieldList = onlineConfig.GetDisableFieldsConfig();
    EXPECT_THAT(disableFieldList.attributes, UnorderedElementsAre("attr1", "attr2"));
    EXPECT_THAT(disableFieldList.packAttributes, UnorderedElementsAre("pack_attr_1"));
    EXPECT_THAT(disableFieldList.indexes, UnorderedElementsAre("idx1", "idx2"));
    ASSERT_EQ(DisableFieldsConfig::SDF_FIELD_ALL, disableFieldList.summarys);
    ASSERT_EQ(DisableFieldsConfig::CDF_FIELD_ALL, disableFieldList.sources);
}

void OnlineConfigTest::TestDisableSourceGroup()
{
    string jsonStr = R"( {
        "need_read_remote_index" : false,
        "load_config" : [ {
             "file_patterns" : ["_ATTRIBUTE_"],
             "load_strategy" : "mmap",
             "deploy": false,
             "remote": false
         } ],
        "disable_fields": {
            "attributes": ["attr1", "attr2"],
            "indexes": ["idx1", "idx2"],
            "pack_attributes": ["pack_attr_1"],
            "summarys":"__SUMMARY_FIELD_ALL__",
            "sources":"__SOURCE_FIELD_GROUP__:1,2",
            "rewrite_load_config": true
        }
    } )";

    OnlineConfig onlineConfig;
    FromJsonString(onlineConfig, jsonStr);
    ASSERT_NO_THROW(onlineConfig.Check());

    const auto& disableFieldList = onlineConfig.GetDisableFieldsConfig();
    EXPECT_THAT(disableFieldList.attributes, UnorderedElementsAre("attr1", "attr2"));
    EXPECT_THAT(disableFieldList.packAttributes, UnorderedElementsAre("pack_attr_1"));
    EXPECT_THAT(disableFieldList.indexes, UnorderedElementsAre("idx1", "idx2"));
    ASSERT_EQ(DisableFieldsConfig::SDF_FIELD_ALL, disableFieldList.summarys);
    ASSERT_EQ(DisableFieldsConfig::CDF_FIELD_GROUP, disableFieldList.sources);

    vector<index::groupid_t> expectIds = {1, 2};
    ASSERT_EQ(expectIds, disableFieldList.disabledSourceGroupIds);

    const auto& loadConfigs = onlineConfig.loadConfigList.GetLoadConfigs();
    ASSERT_EQ(2, loadConfigs.size());

    // for disable source group
    EXPECT_FALSE(loadConfigs[0].Match("segment_2_level_0/source/meta/data", ""));
    EXPECT_FALSE(loadConfigs[0].Match("segment_2_level_0/source/group_0/data", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/source/group_1/data", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/source/group_2/data", ""));
}

void OnlineConfigTest::TestDisableFieldsWithRewrite()
{
    string jsonStr = R"( {
        "need_read_remote_index" : false,
        "load_config" : [ {
             "file_patterns" : ["_ATTRIBUTE_"],
             "load_strategy" : "mmap",
             "deploy": false,
             "remote": false
         } ],
        "disable_fields": {
            "attributes": ["attr1", "attr2"],
            "indexes": ["idx1", "idx2"],
            "pack_attributes": ["pack_attr_1"],
            "summarys":"__SUMMARY_FIELD_ALL__",
            "sources":"__SOURCE_FIELD_ALL__",
            "rewrite_load_config": true
        }
    } )";

    OnlineConfig onlineConfig;
    FromJsonString(onlineConfig, jsonStr);
    ASSERT_NO_THROW(onlineConfig.Check());

    const auto& disableFieldList = onlineConfig.GetDisableFieldsConfig();
    EXPECT_THAT(disableFieldList.attributes, UnorderedElementsAre("attr1", "attr2"));
    EXPECT_THAT(disableFieldList.packAttributes, UnorderedElementsAre("pack_attr_1"));
    EXPECT_THAT(disableFieldList.indexes, UnorderedElementsAre("idx1", "idx2"));

    const auto& loadConfigs = onlineConfig.loadConfigList.GetLoadConfigs();
    ASSERT_EQ(2, loadConfigs.size());
    EXPECT_FALSE(loadConfigs[0].Match("segment_2_level_0/attribute/attr22/", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/attribute/attr1", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/attribute/attr1/", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/attribute/attr2", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/attribute/attr2/", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/attribute/attr1/data", ""));
    EXPECT_TRUE(loadConfigs[0].Match("patch_index_3/segment_2/attribute/attr1", ""));
    EXPECT_TRUE(loadConfigs[0].Match("patch_index_3/segment_2/sub_segment/attribute/attr1", ""));

    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/attribute/pack_attr_1", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2/attribute/pack_attr_1/", ""));
    EXPECT_FALSE(loadConfigs[0].Match("segment_2/attribute/pack_attr_", ""));
    EXPECT_FALSE(loadConfigs[0].Match("segment_2/attribute/pack_attr_/", ""));
    EXPECT_FALSE(loadConfigs[0].Match("segment_2/attribute/pack_attr_11/", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2/sub_segment/attribute/pack_attr_1", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2/sub_segment/attribute/pack_attr_1/", ""));
    EXPECT_TRUE(loadConfigs[0].Match("patch_index_3/segment_2/attribute/pack_attr_1", ""));
    EXPECT_TRUE(loadConfigs[0].Match("patch_index_3/segment_2/sub_segment/attribute/pack_attr_1", ""));

    // for disable index
    EXPECT_FALSE(loadConfigs[0].Match("segment_2_level/index/idx1", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/index/idx1", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/index/idx1/data", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/sub_segment/index/idx1/data", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/index/idx1/data/data2", ""));
    EXPECT_TRUE(loadConfigs[0].Match("patch_index_3/segment_5_level_0/index/idx1/data2", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/index/idx2", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/index/idx2/data", ""));
    EXPECT_FALSE(loadConfigs[0].Match("segment_2_level_0/index/idx3/data", ""));

    // for disable summary
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/summary/data", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/summary/offset", ""));

    // for disable source
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/source/meta_offset", ""));
    EXPECT_TRUE(loadConfigs[0].Match("segment_2_level_0/source/data", ""));

    IndexPartitionOptions options;
    options.SetOnlineConfig(onlineConfig);
    ASSERT_EQ(2, options.GetOnlineConfig().GetDisableFieldsConfig().attributes.size());
    ASSERT_EQ(1, options.GetOnlineConfig().GetDisableFieldsConfig().packAttributes.size());

    auto assertEqual = [](const IndexPartitionOptions& o1, const IndexPartitionOptions& o2) {
        ASSERT_EQ(o1.GetOnlineConfig().GetDisableFieldsConfig().attributes.size(),
                  o2.GetOnlineConfig().GetDisableFieldsConfig().attributes.size());
        ASSERT_EQ(o1.GetOnlineConfig().GetDisableFieldsConfig().packAttributes.size(),
                  o2.GetOnlineConfig().GetDisableFieldsConfig().packAttributes.size());
    };
    {
        IndexPartitionOptions newOptions;
        ASSERT_NO_THROW(FromJsonString(newOptions, ToJsonString(options)));
        assertEqual(options, newOptions);
    }
    {
        IndexPartitionOptions newOptions(options);
        assertEqual(options, newOptions);
    }
}

void OnlineConfigTest::TestDisableSeparation()
{
    autil::EnvGuard env("INDEXLIB_DISABLE_SCS", "true");

    string jsonStr = R"( {
        "need_read_remote_index": true,
        "need_deploy_index": true,
        "load_config": [
            {
                "file_patterns" : ["_ATTRIBUTE_"],
                "load_strategy" : "mmap",
                "remote": true,
                "deploy": false
            },
            {
                "file_patterns" : ["_INDEX_"],
                "load_strategy" : "cache",
                "remote": true,
                "deploy": false
            }
        ]
    } )";

    OnlineConfig onlineConfig;
    FromJsonString(onlineConfig, jsonStr);
    const auto& loadConfigs = onlineConfig.loadConfigList.GetLoadConfigs();
    ASSERT_EQ(2, loadConfigs.size());

    ASSERT_TRUE(loadConfigs[0].IsRemote());
    ASSERT_FALSE(loadConfigs[0].NeedDeploy());

    ASSERT_FALSE(loadConfigs[1].IsRemote());
    ASSERT_TRUE(loadConfigs[1].NeedDeploy());

    const auto& defaultLoadConfig = onlineConfig.loadConfigList.GetDefaultLoadConfig();
    ASSERT_FALSE(defaultLoadConfig.IsRemote());
    ASSERT_TRUE(defaultLoadConfig.NeedDeploy());
}
}} // namespace indexlib::config
