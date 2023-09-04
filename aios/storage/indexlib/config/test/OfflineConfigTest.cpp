#include "indexlib/config/OfflineConfig.h"

#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace config {

class OfflineConfigTest : public TESTBASE
{
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(OfflineConfigTest, TestSimpleProcess)
{
    std::string jsonStr = R"({ "customized_merge_config": {
            "full": {
                "merge_config" : {
                    "max_merge_memory_use": 15528,
                    "merge_thread_count": 10,
                    "merge_strategy_param": "",
                    "merge_strategy": "optimize",
                    "keep_version_count": 100,
                    "enable_package_file": true,
                    "enable_archive_file": true,
                    "package_file_tag_config": [
                        {
                            "file_patterns": [
                                "segment_[0-9]+_level_[0-9]+/index/.*vec_recall",
                                "segment_[0-9]+_level_[0-9]+/index/.*aitheta_recall"
                            ],
                            "tag": "AITHETA"
                        },
                        {
                            "file_patterns": [
                                "segment_[0-9]+_level_[0-9]+/index/.*.patch",
                                "_PATCH_"
                            ],
                            "tag": "PATCH"
                        },
                        {
                            "file_patterns": [".*"],
                            "tag": "MERGE"
                        }
                    ]
                },
                "merge_parallel_num" : 16
            },
            "inc": {
                "merge_config" : {
                    "max_merge_memory_use": 15528,
                    "merge_strategy_param": "",
                    "merge_strategy": "optimize",
                    "keep_version_count": 100,
                    "enable_package_file": false
               },
               "merge_parallel_num" : 8,
               "period" : "period=1800"
            },
            "inc_2": {
                "merge_config" : {
                    "max_merge_memory_use": 15528,
                    "merge_thread_count": 10,
                    "merge_strategy_param": "",
                    "merge_strategy": "balance_tree",
                    "keep_version_count": 100
               },
               "period" : "daytime=12:00",
               "doc_reclaim_source" :
                {
                  "swift_root" : "http://fs-proxy.vip.tbsite.net:3066/opensearch_et2_swift_7u",
                  "topic_name" : "app_reclaim",
                  "swift_reader_config" : "hashByClusterName=true;clusterHashFunc=HASH;"
                }
            }
        }})";

    OfflineConfig offlineConfig;
    offlineConfig.TEST_GetMergeConfig().TEST_SetMergeThreadCount(2);
    offlineConfig.TEST_GetMergeConfig().TEST_SetEnablePackageFile(true);
    FromJsonString(offlineConfig, jsonStr);
    auto indexTaskConfigs = offlineConfig.GetAllIndexTaskConfigs();
    ASSERT_EQ(4u, indexTaskConfigs.size());

    {
        ASSERT_EQ("full_merge", indexTaskConfigs[0].GetTaskName());
        ASSERT_EQ("merge", indexTaskConfigs[0].GetTaskType());
        ASSERT_EQ("manual", indexTaskConfigs[0].GetTrigger().GetTriggerStr());
        ASSERT_EQ(Trigger::MANUAL, indexTaskConfigs[0].GetTrigger().GetTriggerType());
        Status status;
        uint32_t parallelNum;
        std::tie(status, parallelNum) = indexTaskConfigs[0].GetSetting<uint32_t>("parallel_num");
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(16, parallelNum);
        uint32_t threadNum;
        std::tie(status, threadNum) = indexTaskConfigs[0].GetSetting<uint32_t>("thread_num");
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(10, threadNum);
        MergeConfig mergeConfig;
        std::tie(status, mergeConfig) = indexTaskConfigs[0].GetSetting<MergeConfig>("merge_config");
        ASSERT_EQ("optimize", mergeConfig.GetMergeStrategyStr());
        ASSERT_TRUE(mergeConfig.IsPackageFileEnabled());
    }

    {
        ASSERT_EQ("inc", indexTaskConfigs[1].GetTaskName());
        ASSERT_EQ("merge", indexTaskConfigs[1].GetTaskType());
        ASSERT_EQ("period=1800", indexTaskConfigs[1].GetTrigger().GetTriggerStr());
        ASSERT_EQ(Trigger::PERIOD, indexTaskConfigs[1].GetTrigger().GetTriggerType());
        ASSERT_EQ(1800, indexTaskConfigs[1].GetTrigger().GetTriggerPeriod());
        Status status;
        uint32_t parallelNum;
        std::tie(status, parallelNum) = indexTaskConfigs[1].GetSetting<uint32_t>("parallel_num");
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(8, parallelNum);
        uint32_t threadNum;
        std::tie(status, threadNum) = indexTaskConfigs[1].GetSetting<uint32_t>("thread_num");
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(20, threadNum);
        MergeConfig mergeConfig;
        std::tie(status, mergeConfig) = indexTaskConfigs[1].GetSetting<MergeConfig>("merge_config");
        ASSERT_EQ("optimize", mergeConfig.GetMergeStrategyStr());
        ASSERT_FALSE(mergeConfig.IsPackageFileEnabled());
    }
    {
        ASSERT_EQ("inc_2", indexTaskConfigs[2].GetTaskName());
        ASSERT_EQ("merge", indexTaskConfigs[2].GetTaskType());
        ASSERT_EQ("daytime=12:00", indexTaskConfigs[2].GetTrigger().GetTriggerStr());
        ASSERT_EQ(Trigger::DATE_TIME, indexTaskConfigs[2].GetTrigger().GetTriggerType());
        Status status;
        uint32_t threadNum;
        std::tie(status, threadNum) = indexTaskConfigs[2].GetSetting<uint32_t>("thread_num");
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(10, threadNum);
        MergeConfig mergeConfig;
        std::tie(status, mergeConfig) = indexTaskConfigs[2].GetSetting<MergeConfig>("merge_config");
        ASSERT_EQ("balance_tree", mergeConfig.GetMergeStrategyStr());
        ASSERT_TRUE(mergeConfig.IsPackageFileEnabled());
    }

    {
        ASSERT_EQ("inc_2_legacy_reclaim", indexTaskConfigs[3].GetTaskName());
        ASSERT_EQ("reclaim", indexTaskConfigs[3].GetTaskType());
        ASSERT_EQ("daytime=12:00", indexTaskConfigs[3].GetTrigger().GetTriggerStr());
        ASSERT_EQ(Trigger::DATE_TIME, indexTaskConfigs[3].GetTrigger().GetTriggerType());
        auto [status, value] = indexTaskConfigs[3].GetSetting<autil::legacy::Any>("doc_reclaim_source");
        ASSERT_TRUE(status.IsOK());
    }
    // ASSERT_EQ(100, mergeConfig["keep_version_count"]);
}

}} // namespace indexlibv2::config
