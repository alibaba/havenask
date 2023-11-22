#include "indexlib/config/test/build_config_unittest.h"

#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, BuildConfigTest);

BuildConfigTest::BuildConfigTest() {}

BuildConfigTest::~BuildConfigTest() {}

void BuildConfigTest::CaseSetUp() {}

void BuildConfigTest::CaseTearDown() {}

void BuildConfigTest::TestCheck()
{
    BuildConfig buildConfig;
    buildConfig.hashMapInitSize = 1024;
    buildConfig.buildTotalMemory = 512;
    buildConfig.maxDocCount = 10;
    buildConfig.keepVersionCount = 8;
    buildConfig.dumpThreadCount = 4;
    buildConfig.enablePackageFile = false;
    buildConfig.speedUpPrimaryKeyReader = true;
    buildConfig.useUserTimestamp = true;
    ASSERT_NO_THROW(buildConfig.Check());

    {
        BuildConfig abnormalConfig = buildConfig;
        abnormalConfig.buildTotalMemory = DEFAULT_CHUNK_SIZE * 2 - 1;
        ASSERT_THROW(abnormalConfig.Check(), util::BadParameterException);
    }
    {
        BuildConfig abnormalConfig = buildConfig;
        abnormalConfig.maxDocCount = 0;
        ASSERT_THROW(abnormalConfig.Check(), util::BadParameterException);
    }
    {
        BuildConfig abnormalConfig = buildConfig;
        abnormalConfig.keepVersionCount = 0;
        ASSERT_THROW(abnormalConfig.Check(), util::BadParameterException);
    }
    {
        BuildConfig abnormalConfig = buildConfig;
        abnormalConfig.dumpThreadCount = 0;
        ASSERT_THROW(abnormalConfig.Check(), util::BadParameterException);
        abnormalConfig.dumpThreadCount = BuildConfig::MAX_DUMP_THREAD_COUNT + 1;
        ASSERT_THROW(abnormalConfig.Check(), util::BadParameterException);
    }

    {
        // check param not valid
        BuildConfig abnormalConfig = buildConfig;
        abnormalConfig.speedUpPrimaryKeyReader = true;
        abnormalConfig.speedUpPrimaryKeyReaderParam = "combine_segment=true";
        ASSERT_THROW(abnormalConfig.Check(), util::BadParameterException);

        // check when not speedup pk ignore check param
        abnormalConfig.speedUpPrimaryKeyReader = false;
        ASSERT_NO_THROW(abnormalConfig.Check());
    }

    {
        BuildConfig abnormalConfig = buildConfig;
        abnormalConfig.levelTopology = indexlibv2::framework::topo_default;
        abnormalConfig.shardingColumnNum = 2;
        ASSERT_THROW(abnormalConfig.Check(), util::BadParameterException);
        abnormalConfig.shardingColumnNum = 1;
        ASSERT_NO_THROW(abnormalConfig.Check());
    }
}

void BuildConfigTest::TestJsonize()
{
    string jsonStr = R"(
    {
        "hash_map_init_size" : 1024,
        "build_total_memory" : 512,
        "max_doc_count" : 10,
        "keep_version_count" : 8,
        "dump_thread_count" : 4,
        "sharding_column_num" : 5,
        "level_topology" : "key_range",
        "level_num" : 2,
        "time_to_live" : 10,
        "enable_package_file" : false,
        "speedup_primary_key_reader" : true,
        "use_user_timestamp" : true,
        "speedup_primary_key_param":"combine_segments=true,max_doc_count=3",
        "parameters":{
            "k1": "v1",
            "k2": "v2",
            "jk1": 1.23,
            "jk2": {
                "params": {
                    "kk": "vv"
                }
            }
        },
        "enable_build_parallel_lookup_pk" : true,
        "enable_async_dump_segment" : true,
        "enable_consistent_mode_batch_build" : true,
        "enable_inconsistent_mode_batch_build" : true,
        "batch_build_consistent_mode_max_thread_count" :5,
        "batch_build_inconsistent_mode_max_thread_count" :3,
        "batch_build_max_batch_size" : 1234,
        "batch_build_max_collect_interval" : 654321,
        "batch_build_max_collect_memory_mb" : 77,
        "building_memory_limit_mb" : 76
    })";

    BuildConfig expectBuildConfig;
    ASSERT_FALSE(expectBuildConfig.EnableAsyncDump());

    expectBuildConfig.hashMapInitSize = 1024;
    expectBuildConfig.buildTotalMemory = 512;
    expectBuildConfig.SetBuildingMemoryLimit(76);
    expectBuildConfig.maxDocCount = 10;
    expectBuildConfig.keepVersionCount = 8;
    expectBuildConfig.dumpThreadCount = 4;
    expectBuildConfig.shardingColumnNum = 8; // shardingColumnNum must be 2^n
    expectBuildConfig.levelTopology = indexlibv2::framework::topo_key_range;
    expectBuildConfig.levelNum = 2;
    expectBuildConfig.ttl = 10;
    expectBuildConfig.enablePackageFile = false;
    expectBuildConfig.speedUpPrimaryKeyReader = true;
    expectBuildConfig.useUserTimestamp = true;
    expectBuildConfig.speedUpPrimaryKeyReaderParam = "combine_segments=true,max_doc_count=3";
    expectBuildConfig.SetCustomizedParams("k1", "v1");
    expectBuildConfig.SetCustomizedParams("k2", "v2");
    expectBuildConfig.SetCustomizedParams("jk1", "1.23");
    expectBuildConfig.SetCustomizedParams("jk2", "{\"params\":{\"kk\":\"vv\"}}");
    expectBuildConfig.SetAsyncDump(true);
    expectBuildConfig.EnablePkBuildParallelLookup();

    expectBuildConfig.SetBatchBuild(/*isConsistentMode=*/true, /*enableBatchBuild=*/true);
    expectBuildConfig.SetBatchBuild(/*isConsistentMode=*/false, /*enableBatchBuild=*/true);
    expectBuildConfig.SetBatchBuildMaxThreadCount(true, 5);
    expectBuildConfig.SetBatchBuildMaxThreadCount(false, 3);
    expectBuildConfig.SetBatchBuildMaxBatchSize(1234);
    expectBuildConfig.SetBatchBuildMaxCollectInterval(654321);
    expectBuildConfig.SetBatchBuildMaxCollectMemoryInM(77);

    BuildConfig buildConfig;
    FromJsonString(buildConfig, jsonStr);
    ASSERT_EQ(expectBuildConfig, buildConfig);

    string jsonStr1 = ToJsonString(buildConfig);
    BuildConfig buildConfig2;
    FromJsonString(buildConfig2, jsonStr1);
    ASSERT_EQ(expectBuildConfig, buildConfig2);
    string jsonStr2 = ToJsonString(buildConfig2);
    ASSERT_EQ(jsonStr1, jsonStr2);

    BuildConfig buildConfig3 = buildConfig2;
    string jsonStr3 = ToJsonString(buildConfig3);
    ASSERT_EQ(expectBuildConfig, buildConfig3);
    ASSERT_EQ(jsonStr3, jsonStr1);

    ASSERT_TRUE(buildConfig3.SetCustomizedParams("k3", "v3"));
    ASSERT_FALSE(buildConfig3.SetCustomizedParams("k3", "v4"));
    ASSERT_EQ(string("v3"), buildConfig3.GetCustomizedParams().at("k3"));
    ASSERT_FALSE(expectBuildConfig == buildConfig3);
}

void BuildConfigTest::TestEnvBuildTotalMem()
{
    setenv("BUILD_TOTAL_MEM", "100", 1);
    BuildConfig config;
    ASSERT_EQ((int64_t)100, config.buildTotalMemory);
    setenv("BUILD_TOTAL_MEM", "", 1);
}
}} // namespace indexlib::config
