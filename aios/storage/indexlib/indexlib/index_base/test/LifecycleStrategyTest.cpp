#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/LifecycleConfig.h"
#include "indexlib/file_system/LifecycleTable.h"
#include "indexlib/framework/Version.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/lifecycle_strategy.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "unittest/unittest.h"

using namespace indexlib::file_system;

namespace indexlib::index_base {

class LifecycleStrategyTest : public TESTBASE
{
public:
    LifecycleStrategyTest() = default;
    ~LifecycleStrategyTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}

    std::shared_ptr<LifecycleTable> CreateLifecycleTable(const std::string& versionJsonStr,
                                                         const std::string& onlineConfigJsonStr)
    {
        indexlibv2::config::TabletOptions tabletOptions;
        FromJsonString(tabletOptions.TEST_GetOnlineConfig(), onlineConfigJsonStr);
        indexlibv2::framework::Version version;
        FromJsonString(version, versionJsonStr);
        auto lifecycleConfig = tabletOptions.GetOnlineConfig().GetLifecycleConfig();
        int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
        auto lifecycleStrategy = indexlib::index_base::LifecycleStrategyFactory::CreateStrategy(
            lifecycleConfig,
            {{indexlib::file_system::LifecyclePatternBase::CURRENT_TIME, std::to_string(currentTime)}});
        auto lifecycleTable = std::make_shared<indexlib::file_system::LifecycleTable>();
        auto segLifecycles = lifecycleStrategy->GetSegmentLifecycles(version.GetSegmentDescriptions());
        std::set<std::string> segmentsWithLifecycle;
        for (const auto& kv : segLifecycles) {
            std::string segmentDir = version.GetSegmentDirName(kv.first);
            lifecycleTable->AddDirectory(segmentDir, kv.second);
            segmentsWithLifecycle.insert(segmentDir);
        }
        for (auto it = version.begin(); it != version.end(); it++) {
            std::string segmentDir = version.GetSegmentDirName(it->segmentId);
            if (segmentsWithLifecycle.find(segmentDir) == segmentsWithLifecycle.end()) {
                lifecycleTable->AddDirectory(segmentDir, "");
            }
        }
        return lifecycleTable;
    }

    void CheckLifecycleTable(const std::shared_ptr<LifecycleTable>& lifecycleTable,
                             const std::map<std::string, std::string>& expectLifecycle) const
    {
        ASSERT_EQ(expectLifecycle.size(), lifecycleTable->Size());
        for (auto it : expectLifecycle) {
            EXPECT_EQ(it.second, lifecycleTable->GetLifecycle(it.first));
        }
    }
};

TEST_F(LifecycleStrategyTest, TestStaticStrategy)
{
    std::string versionJsonStr = R"({
        "versionid": 3,
        "segments": [1, 2, 3],
        "segment_descriptions": {
            "segment_statistics": [
                {
                    "segment_id": 1,
                    "string_stats":{
                        "segment_temperature": "cold",
                        "segment_temperature_detail": ""
                    }
                },
                {
                    "segment_id": 2,
                    "string_stats":{
                        "segment_temperature": "warm",
                        "segment_temperature_detail": ""
                    }
                },
                {
                    "segment_id": 3,
                    "string_stats":{
                        "segment_temperature": "hot",
                        "segment_temperature_detail": ""
                    }
                }
            ]
        }
    })";
    std::string onlineConfigJsonStr = R"({
        "load_config": [],
        "lifecycle_config": {
            "strategy" : "static",
            "patterns" : [
                {"statistic_field" : "segment_temperature","statistic_type" : "string", "lifecycle": "hot", "range" : ["hot"]},
                {"statistic_field" : "segment_temperature","statistic_type" : "string", "lifecycle": "warm", "range" : ["warm"]},
                {"statistic_field" : "segment_temperature","statistic_type" : "string", "lifecycle": "cold", "range" : ["cold"]}
            ]
        }
    })";
    auto lifecycleTable = CreateLifecycleTable(versionJsonStr, onlineConfigJsonStr);
    std::map<std::string, std::string> expectLifecycleTable;
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_1/", "cold"));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_2/", "warm"));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_3/", "hot"));
    CheckLifecycleTable(lifecycleTable, expectLifecycleTable);
}

TEST_F(LifecycleStrategyTest, TestDynamicStrategy)
{
    std::string versionJsonStr = R"({
        "versionid": 3,
        "segments": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "segment_descriptions": {
            "segment_statistics" : [
                {
                    "segment_id" : 1,
                    "integer_stats" : {
                        "event_time" : [-100, 0]
                    }
                },
                {
                    "segment_id" : 2,
                    "integer_stats" : {
                        "event_time" : [0, 101]
                    }
                },
                {
                    "segment_id" : 3,
                    "integer_stats" : {
                        "event_time" : [550, 650]
                    }
                },
                {
                    "segment_id" : 4,
                    "integer_stats" : {
                        "event_time" : [700, 1000]
                    }
                },
                {
                    "segment_id" : 5,
                    "integer_stats" : {
                        "event_time" : [0, 1000]
                    }
                },
                {
                    "segment_id" : 6,
                    "integer_stats" : {
                        "event_time" : [1000, 1200]
                    }
                },
                {
                    "segment_id" : 7,
                    "integer_stats" : {
                        "event_time" : [999, 1200]
                    }
                },
                {
                    "segment_id" : 8,
                    "integer_stats" : {
                        "event_time" : [999, 1200]
                    },
                    "string_stats" : {
                        "string_key1": "value1",
                        "string_key2": "value2"
                   }
                },
                {
                    "segment_id" : 9,
                    "integer_stats" : {
                        "event_time" : [999, 1200]
                    },
                    "string_stats" : {
                        "string_key1": "value10000",
                        "string_key2": "value2"
                   }
                },
                {
                    "segment_id" : 10,
                    "integer_stats" : {
                        "event_time" : [999, 1200]
                    },
                    "string_stats" : {
                        "string_key1": "aaaa",
                        "string_key2": "value2"
                   }
                }
            ]
        }
    })";
    std::string onlineConfigJsonStr = R"({
        "load_config": [],
        "lifecycle_config": {
            "strategy" : "dynamic",
            "patterns" : [
                {"statistic_field" : "string_key1","statistic_type" : "string", "lifecycle": "demo", "range" : ["value10000", "aaaa", "bbb"]},
                {"statistic_field" : "event_time","statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "hot", "range" : [100, 300], "is_offset": false}, 
                {"statistic_field" : "event_time","statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "warm", "range" : [500, 700]}, 
                {"statistic_field" : "event_time","statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "cold", "range" : [700, 900]},
                {"statistic_field" : "event_time","statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "warm", "range" : [0, 1000], "is_offset": false}
            ]
        }
    })";
    auto lifecycleTable = CreateLifecycleTable(versionJsonStr, onlineConfigJsonStr);
    std::map<std::string, std::string> expectLifecycleTable;
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_1/", ""));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_2/", "hot"));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_3/", "warm"));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_4/", "cold"));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_5/", "hot"));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_6/", ""));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_7/", "warm"));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_8/", "warm"));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_9/", "demo"));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_10/", "demo"));
    CheckLifecycleTable(lifecycleTable, expectLifecycleTable);
}

TEST_F(LifecycleStrategyTest, GetSegmentLifecycle)
{
    std::string versionJsonStr = R"({
        "versionid": 3,
        "segments": [1, 2, 3, 4, 5, 6, 7, 10],
        "segment_statistics" : [
            {
                "segment_id" : 1,
                "integer_stats" : {
                    "event_time" : [0, 100]
                }
            },
            {
                "segment_id" : 2,
                "integer_stats" : {
                    "event_time" : [0, 101]
                }
            },
            {
                "segment_id" : 3,
                "integer_stats" : {
                    "event_time" : [550, 650]
                }
            },
            {
                "segment_id" : 4,
                "integer_stats" : {
                    "event_time" : [700, 1000]
                }
            },
            {
                "segment_id" : 5,
                "integer_stats" : {
                    "event_time" : [0, 1000]
                }
            },
            {
                "segment_id" : 7,
                "integer_stats" : {
                    "event_time" : [999, 1200]
                }
            },
            {
                "segment_id" : 10,
                "integer_stats" : {
                    "event_time" : [1200, 1400]
                }
            }
        ]
    })";
    std::string onlineConfigJsonStr = R"({
        "load_config": [],
        "lifecycle_config": {
            "strategy" : "dynamic",
            "patterns" : [
                {"statistic_field" : "event_time", "statistic_type" : "integer","lifecycle": "hot", "range" : [100, 300], "is_offset": false}, 
                {"statistic_field" : "event_time", "statistic_type" : "integer","lifecycle": "warm", "range" : [500, 700]}, 
                {"statistic_field" : "event_time", "statistic_type" : "integer","lifecycle": "cold", "range" : [700, 900]},
                {"statistic_field" : "event_time", "statistic_type" : "integer","lifecycle": "warm", "range" : [0, 1000], "is_offset": false}
            ]
        }
    })";

    index_base::Version version;
    FromJsonString(version, versionJsonStr);
    indexlibv2::config::TabletOptions tabletOptions;
    FromJsonString(tabletOptions.TEST_GetOnlineConfig(), onlineConfigJsonStr);
    auto lifecycleConfig = tabletOptions.GetOnlineConfig().GetLifecycleConfig();
    auto lifecycleStrategy = LifecycleStrategyFactory::CreateStrategy(lifecycleConfig, {});
    ASSERT_TRUE(lifecycleStrategy != nullptr);

    auto CheckSegmentLifecycle = [&version, &lifecycleStrategy](segmentid_t segId,
                                                                const std::string& expectedLifecycle) {
        SegmentData segData;
        segData.SetSegmentId(segId);
        EXPECT_EQ(expectedLifecycle, lifecycleStrategy->GetSegmentLifecycle(version, &segData));
    };

    CheckSegmentLifecycle(0, "");
    CheckSegmentLifecycle(1, "warm");
    CheckSegmentLifecycle(2, "hot");
    CheckSegmentLifecycle(3, "warm");
    CheckSegmentLifecycle(4, "cold");
    CheckSegmentLifecycle(5, "hot");
    CheckSegmentLifecycle(6, "");
    CheckSegmentLifecycle(7, "warm");
    CheckSegmentLifecycle(8, "");
    CheckSegmentLifecycle(10, "");
}

TEST_F(LifecycleStrategyTest, TestCornerCase)
{
    // 1. segment(s) has no SegmentStatistics in SegmentDescription
    // 2. segment(s) has no statistic_field in SegmentDescription
    std::string versionJsonStr = R"({
        "versionid": 3,
        "segments": [1],
        "segment_descriptions": {
         }
    })";
    std::string onlineConfigJsonStr = R"({
        "load_config": [],
        "lifecycle_config": {
            "strategy" : "dynamic",
            "patterns" : [
                {"statistic_field" : "event_time", "statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "hot", "range" : [100, 300], "is_offset": false}, 
                {"statistic_field" : "event_time", "statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "warm", "range" : [500, 700]}, 
                {"statistic_field" : "event_time", "statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "cold", "range" : [700, 900]},
                {"statistic_field" : "event_time", "statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "warm", "range" : [0, 1000], "is_offset": false}
            ]
        },
        "need_read_remote_index": true
    })";
    std::map<std::string, std::string> expectLifecycleTable;
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_1/", ""));
    auto lifecycleTable = CreateLifecycleTable(versionJsonStr, onlineConfigJsonStr);
    CheckLifecycleTable(lifecycleTable, expectLifecycleTable);

    versionJsonStr = R"({
        "versionid": 3,
        "segments": [1]
    })";
    expectLifecycleTable.clear();
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_1/", ""));
    lifecycleTable = CreateLifecycleTable(versionJsonStr, onlineConfigJsonStr);
    CheckLifecycleTable(lifecycleTable, expectLifecycleTable);

    versionJsonStr = R"({
        "versionid": 3,
        "segments": [1, 2, 3, 4, 5],
        "segment_descriptions": {
            "segment_statistics" : [
                {
                    "segment_id" : 1,
                    "integer_stats" : {
                        "field_1" : [100, 200],
                        "event_time" : [-100, 1]
                    }
                },
                {
                    "segment_id" : 2,
                    "integer_stats" : {
                        "field_1" : [100, 200]
                    }
                }
            ]    
        }
    })";
    lifecycleTable = CreateLifecycleTable(versionJsonStr, onlineConfigJsonStr);
    expectLifecycleTable.clear();
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_1/", "warm"));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_2/", ""));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_3/", ""));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_4/", ""));
    expectLifecycleTable.insert(std::pair<std::string, std::string>("segment_5/", ""));
    CheckLifecycleTable(lifecycleTable, expectLifecycleTable);
}

TEST_F(LifecycleStrategyTest, TestDynamicCreateSegmentTemperatureList)
{
    std::string versionJsonStr = R"({
        "versionid": 3,
        "segments": [1, 2, 3],
        "segment_descriptions": {
            "segment_statistics": [
                {
                    "segment_id": 1,
                    "integer_stats": {
                        "event_time": [700, 800]
                    }
                },
                {
                    "segment_id": 2,
                    "integer_stats": {
                        "event_time": [300, 400]
                    }
                },
                {
                    "segment_id": 3,
                    "integer_stats": {
                        "event_time": [50, 150]
                    }
                }
            ]
        }
    })";
    std::string lifecycleConfigJsonStr = R"({
            "strategy" : "dynamic",
            "patterns" : [
                {"statistic_field" : "event_time", "statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "hot", "range" : [100, 300], "is_offset": false},
                {"statistic_field" : "event_time", "statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "warm", "range" : [500, 700]},
                {"statistic_field" : "event_time", "statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "cold", "range" : [700, 900]},
                {"statistic_field" : "event_time", "statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "warm", "range" : [0, 1000], "is_offset": false}
            ]
    })";
    indexlib::file_system::LifecycleConfig lifecycleConfig;
    FromJsonString(lifecycleConfig, lifecycleConfigJsonStr);
    auto lifecycleStrategy = LifecycleStrategyFactory::CreateStrategy(lifecycleConfig, {{"CURRENT_TIME", "1000000"}});
    indexlibv2::framework::Version version;
    FromJsonString(version, versionJsonStr);
    auto segmentLifecycles = lifecycleStrategy->GetSegmentLifecycles(version.GetSegmentDescriptions());
    EXPECT_EQ(3, segmentLifecycles.size());
    EXPECT_EQ(1, segmentLifecycles[0].first);
    EXPECT_EQ("cold", segmentLifecycles[0].second);
    EXPECT_EQ(2, segmentLifecycles[1].first);
    EXPECT_EQ("warm", segmentLifecycles[1].second);
    EXPECT_EQ(3, segmentLifecycles[2].first);
    EXPECT_EQ("hot", segmentLifecycles[2].second);
}

} // namespace indexlib::index_base
