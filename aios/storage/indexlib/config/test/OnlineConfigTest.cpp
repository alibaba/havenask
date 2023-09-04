#include "indexlib/config/OnlineConfig.h"

#include "indexlib/file_system/LifecycleConfig.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/util/Exception.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace config {

class OnlineConfigTest : public TESTBASE
{
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(OnlineConfigTest, TestLifecycleConfig)
{
    string jsonStr = R"( {
    "need_read_remote_index": true,
    "need_deploy_index": true,
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
    ],
    "lifecycle_config": {
        "strategy" : "dynamic",
        "patterns" : [
            {
                "lifecycle": "hot",
                "range" : [-550, -600],
                "is_offset": true,
                "statistic_field" : "event_time",
                "statistic_type" : "integer",
                "offset_base" : "CURRENT_TIME"
            },
            {
                "lifecycle": "warm",
                "range" : [300, 550],
                "statistic_field" : "event_time",
                "statistic_type" : "integer",
                "offset_base" : "CURRENT_TIME"
            },
            {
                "lifecycle": "cold",
                "range" : [0, 300],
                "statistic_field" : "event_time",
                "statistic_type" : "integer",
                "offset_base" : "CURRENT_TIME"
            }
         ]
    }
    } )";

    auto validateOnlineConfig = [](const std::string& inputJson, OnlineConfig& inputConfig, string& outputJson) {
        FromJsonString(inputConfig, inputJson);
        OnlineConfig onlineConfig = inputConfig;
        ASSERT_EQ(8L << 30 /* 8G */, onlineConfig.GetMaxRealtimeMemoryUse());
        ASSERT_EQ(1200, onlineConfig.GetPrintMetricsInterval());
        ASSERT_EQ(-1, onlineConfig.GetMaxRealtimeDumpIntervalSecond());
        ASSERT_TRUE(onlineConfig.GetNeedReadRemoteIndex());
        ASSERT_TRUE(onlineConfig.GetNeedDeployIndex());
        ASSERT_EQ(1, onlineConfig.GetLoadConfigList().Size());
        ASSERT_TRUE(onlineConfig.EnableLocalDeployManifestChecking());

        const auto& lifecycleConfig = onlineConfig.GetLifecycleConfig();

        ASSERT_EQ("dynamic", lifecycleConfig.GetStrategy());
        auto patterns = lifecycleConfig.GetPatterns();
        ASSERT_EQ(3, patterns.size());
        for (const auto& pattern : patterns) {
            ASSERT_EQ("event_time", pattern->statisticField);
            ASSERT_EQ("integer", pattern->statisticType);
            ASSERT_EQ("CURRENT_TIME", pattern->offsetBase);
        }

        ASSERT_TRUE(lifecycleConfig.EnableLocalDeployManifestChecking());

        const auto& pattern0 = patterns[0];
        auto typedPattern0 = std::dynamic_pointer_cast<indexlib::file_system::IntegerLifecyclePattern>(pattern0);
        EXPECT_EQ("hot", typedPattern0->lifecycle);
        auto pair0Expect = std::pair<int64_t, int64_t>(-550, -600);
        EXPECT_EQ(pair0Expect, typedPattern0->range);
        EXPECT_TRUE(typedPattern0->isOffset);

        const auto& pattern1 = patterns[1];
        auto typedPattern1 = std::dynamic_pointer_cast<indexlib::file_system::IntegerLifecyclePattern>(pattern1);
        EXPECT_EQ("warm", typedPattern1->lifecycle);
        auto pair1Expect = std::pair<int64_t, int64_t>(300, 550);
        EXPECT_EQ(pair1Expect, typedPattern1->range);
        EXPECT_FALSE(typedPattern1->isOffset);

        const auto& pattern2 = patterns[2];
        auto typedPattern2 = std::dynamic_pointer_cast<indexlib::file_system::IntegerLifecyclePattern>(pattern2);
        EXPECT_EQ("cold", typedPattern2->lifecycle);
        auto pair2Expect = std::pair<int64_t, int64_t>(0, 300);
        EXPECT_EQ(pair2Expect, typedPattern2->range);
        EXPECT_FALSE(typedPattern2->isOffset);
        outputJson = ToJsonString(onlineConfig);
    };
    OnlineConfig onlineConfig;
    std::string outputJson;
    validateOnlineConfig(jsonStr, onlineConfig, outputJson);
    OnlineConfig onlineConfig2;
    std::string outputJson2;
    validateOnlineConfig(outputJson, onlineConfig2, outputJson2);
    using LifecycleConfig = indexlib::file_system::LifecycleConfig;
    {
        LifecycleConfig defaultConfig;
        ASSERT_NO_THROW(FromJsonString(defaultConfig, "{}"));
        EXPECT_EQ(LifecycleConfig::STATIC_STRATEGY, defaultConfig.GetStrategy());
        EXPECT_FALSE(defaultConfig.EnableLocalDeployManifestChecking());
    }
    {
        LifecycleConfig dynamicLifecycleConfig;
        string jsonStr = R"({"strategy":"dynamic", "statistic_type": "integer"})";
        FromJsonString(dynamicLifecycleConfig, jsonStr);
        EXPECT_EQ(LifecycleConfig::DYNAMIC_STRATEGY, dynamicLifecycleConfig.GetStrategy());
        EXPECT_TRUE(dynamicLifecycleConfig.EnableLocalDeployManifestChecking());
    }
    {
        LifecycleConfig dynamicLifecycleConfig;
        string jsonStr =
            R"({"strategy":"dynamic", "statistic_type": "integer", "enable_local_deploy_manifest": false})";
        ASSERT_NO_THROW(FromJsonString(dynamicLifecycleConfig, jsonStr));
    }
    {
        LifecycleConfig staticLifecycleConfig;
        string jsonStr = R"({"strategy":"static", "enable_local_deploy_manifest": true})";
        ASSERT_NO_THROW(FromJsonString(staticLifecycleConfig, jsonStr));
        EXPECT_TRUE(staticLifecycleConfig.EnableLocalDeployManifestChecking());
    }
    {
        LifecycleConfig dynamicLifecycleConfig;
        string jsonStr =
            R"({"strategy":"dynamic", "patterns":[{"statistic_type": "integer", "offset_base": "CURRENT_TIME"}]})";
        ASSERT_NO_THROW(FromJsonString(dynamicLifecycleConfig, jsonStr));
        ASSERT_TRUE(dynamicLifecycleConfig.InitOffsetBase({{"CURRENT_TIME", "33"}}));
        auto typedPattern = std::dynamic_pointer_cast<indexlib::file_system::IntegerLifecyclePattern>(
            dynamicLifecycleConfig.GetPatterns()[0]);
        ASSERT_EQ(33, typedPattern->baseValue);

        ASSERT_FALSE(dynamicLifecycleConfig.InitOffsetBase({{"some-non-exist-key", "33"}}));
    }
    {
        LifecycleConfig dynamicLifecycleConfig;
        string jsonStr = R"({"strategy":"dynamic","patterns":[{"statistic_type": "integer", "offset_base": "3300"}]})";
        ASSERT_NO_THROW(FromJsonString(dynamicLifecycleConfig, jsonStr));
        ASSERT_TRUE(dynamicLifecycleConfig.InitOffsetBase({{"CURRENT_TIME", "33"}}));
        auto typedPattern = std::dynamic_pointer_cast<indexlib::file_system::IntegerLifecyclePattern>(
            dynamicLifecycleConfig.GetPatterns()[0]);
        EXPECT_EQ(3300, typedPattern->baseValue);
    }
}

}} // namespace indexlibv2::config
