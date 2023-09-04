#include "indexlib/config/test/merge_config_unittest.h"

#include "indexlib/file_system/package/PackageFileTagConfigList.h"

using namespace std;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, MergeConfigTest);

MergeConfigTest::MergeConfigTest() {}

MergeConfigTest::~MergeConfigTest() {}

void MergeConfigTest::CaseSetUp() {}

void MergeConfigTest::CaseTearDown() {}

void MergeConfigTest::TestJsonize()
{
    // cout << "autil::legacy::Jsonizable:"  << sizeof(autil::legacy::Jsonizable) << endl;
    // cout << "string:"  << sizeof(string) << endl;
    // cout << "config::MergeIOConfig:"  << sizeof(config::MergeIOConfig) << endl;
    // cout << "config::TruncateOptionConfigPtr:"  << sizeof(config::TruncateOptionConfigPtr) << endl;
    // cout << "MergeStrategyParameter:"  << sizeof(MergeStrategyParameter) << endl;
    string jsonStr = R"(
    {
        "merge_strategy" : "optimize",
        "merge_strategy_param" : "abcd",
        "need_calculate_temperature" : true,
        "document_reclaim_config_path" : "/indexlib/config.json",
        "max_merge_memory_use" : 1024,
        "merge_thread_count" : 8,
        "enable_package_file": true,
        "checkpoint_interval": 100,
        "package_file_tag_config":
        [
            {"file_patterns": ["_SUMMARY_DATA_"], "tag": "summary_data" },
            {"file_patterns": ["_SUMMARY_"], "tag": "summary" }
        ],
        "merge_io_config" : {"enable_async_read" : true},
        "truncate_strategy" :
        [
            {
    	        "strategy_name" : "distinct_sort",
        	    "strategy_type" : "default",
    	        "threshold" : 100000,
    	        "truncate_profiles" : [
    	            "profile1",
    		        "profile2"
    	        ]
    	    }
        ]
    }
    )";
    MergeConfig mergeConfig;
    FromJsonString(mergeConfig, jsonStr);
    ASSERT_EQ(string("optimize"), mergeConfig.mergeStrategyStr);
    ASSERT_EQ(string("abcd"), mergeConfig.mergeStrategyParameter.GetLegacyString());
    ASSERT_EQ(string("/indexlib/config.json"), mergeConfig.docReclaimConfigPath);
    ASSERT_EQ((int64_t)1024, mergeConfig.maxMemUseForMerge);
    ASSERT_EQ((uint32_t)8, mergeConfig.mergeThreadCount);
    ASSERT_TRUE(mergeConfig.mergeIOConfig.enableAsyncRead);
    TruncateOptionConfigPtr truncateOptionConfig = mergeConfig.truncateOptionConfig;
    ASSERT_TRUE(truncateOptionConfig);
    ASSERT_EQ((size_t)1, truncateOptionConfig->GetOriTruncateStrategy().size());
    ASSERT_TRUE(mergeConfig.GetEnablePackageFile());
    ASSERT_EQ(100u, mergeConfig.GetCheckpointInterval());
    ASSERT_EQ(2, mergeConfig.GetPackageFileTagConfigList().configs.size());
    ASSERT_EQ("summary_data",
              mergeConfig.GetPackageFileTagConfigList().Match("segment_0_level_0/summary/data", "NOMATCH"));
    ASSERT_EQ("NOMATCH",
              mergeConfig.GetPackageFileTagConfigList().Match("BAD.segment_0_level_0/summary/data", "NOMATCH"));
    ASSERT_EQ("summary",
              mergeConfig.GetPackageFileTagConfigList().Match("segment_0_level_0/summary/offset", "NOMATCH"));
    ASSERT_TRUE(mergeConfig.NeedCalculateTemperature());
}

void MergeConfigTest::TestDefault()
{
    string jsonStr = R"( {} )";
    MergeConfig mergeConfig;
    FromJsonString(mergeConfig, jsonStr);
    ASSERT_FALSE(mergeConfig.GetEnablePackageFile());
    ASSERT_EQ(0, mergeConfig.GetPackageFileTagConfigList().configs.size());
    ASSERT_EQ(600u, mergeConfig.GetCheckpointInterval());
}

void MergeConfigTest::TestCheck()
{
    MergeConfig mergeConfig;
    ASSERT_NO_THROW(mergeConfig.Check());

    {
        MergeConfig abnormalConfig = mergeConfig;
        abnormalConfig.mergeThreadCount = 0;
        ASSERT_THROW(abnormalConfig.Check(), util::BadParameterException);

        abnormalConfig.mergeThreadCount = MergeConfig::MAX_MERGE_THREAD_COUNT + 1;
        ASSERT_THROW(abnormalConfig.Check(), util::BadParameterException);
    }
}

void MergeConfigTest::TestPackageFile()
{
    string jsonStr = R"(
    {
        "enable_package_file": true,
        "checkpoint_interval": 100,
        "package_file_tag_config":
        [
            {"file_patterns": ["summary/data", "summary/offset"], "tag": "SUMMARY" },
            {"file_patterns": [".*"], "tag": "MERGE" }
        ]
    }
    )";
    MergeConfig mergeConfig;
    FromJsonString(mergeConfig, jsonStr);
    ASSERT_TRUE(mergeConfig.GetEnablePackageFile());
    ASSERT_EQ(100u, mergeConfig.GetCheckpointInterval());
}
}} // namespace indexlib::config
