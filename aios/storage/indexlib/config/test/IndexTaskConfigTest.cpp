#include "indexlib/config/IndexTaskConfig.h"

#include "indexlib/config/MergeConfig.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {
class IndexTaskConfigTest : public TESTBASE
{
public:
    IndexTaskConfigTest() = default;
    ~IndexTaskConfigTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void IndexTaskConfigTest::setUp() {}

void IndexTaskConfigTest::tearDown() {}

TEST_F(IndexTaskConfigTest, testRewriteWithDefaultMergeConfig)
{
    IndexTaskConfig config;
    std::string jsonStr = R"(
        {
            "task_type" : "merge",
            "task_name" : "merge_inc",
            "trigger" : "auto",
            "settings" : {
                "parallel_num" :10,
                "merge_config": {
                    "merge_strategy" : "optimize",
                    "merge_strategy_param" : ""
                }
            }
        })";
    FromJsonString(config, jsonStr);
    ASSERT_EQ("merge", config.GetTaskType());
    ASSERT_EQ("merge_inc", config.GetTaskName());
    MergeConfig mergeConfig;
    mergeConfig.TEST_SetMergeThreadCount(2);
    mergeConfig.TEST_SetEnablePackageFile(true);
    config.RewriteWithDefaultMergeConfig(mergeConfig);
    auto [status, taskMergeConfig] = config.GetSetting<indexlibv2::config::MergeConfig>("merge_config");
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(taskMergeConfig.IsPackageFileEnabled());
    uint32_t num = 0;
    std::tie(status, num) = config.GetSetting<uint32_t>("thread_num");
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(2u, num);
}

TEST_F(IndexTaskConfigTest, testUsage)
{
    IndexTaskConfig config;
    std::string jsonStr = R"(
        {
            "task_type" : "merge",
            "task_name" : "merge_inc",
            "trigger" : "auto",
            "settings" : {
                "parallel_num" :10,
                "thread_num" :11,
                "merge_config": {
                    "merge_strategy" : "optimize",
                    "merge_strategy_param" : ""
                }
            }
        })";
    FromJsonString(config, jsonStr);
    ASSERT_EQ("merge", config.GetTaskType());
    ASSERT_EQ("merge_inc", config.GetTaskName());

    const auto& trigger = config.GetTrigger();
    ASSERT_EQ(Trigger::TriggerType::AUTO, trigger.GetTriggerType());
    ASSERT_EQ(std::nullopt, trigger.GetTriggerPeriod());
    ASSERT_EQ(std::nullopt, trigger.GetTriggerDayTime());
    ASSERT_EQ("auto", trigger.GetTriggerStr());

    auto [status, mergeConfig] = config.GetSetting<MergeConfig>("merge_config");
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ("optimize", mergeConfig.GetMergeStrategyStr());

    auto [status2, _] = config.GetSetting<bool>("not-existed");
    ASSERT_TRUE(status2.IsNotFound());

    uint32_t num;
    std::tie(status, num) = config.GetSetting<uint32_t>("parallel_num");
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(10u, num);
    std::tie(status, num) = config.GetSetting<uint32_t>("thread_num");
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(11u, num);
}

TEST_F(IndexTaskConfigTest, testDayTime)
{
    IndexTaskConfig config;
    std::string jsonStr = R"(
        {
            "task_type" : "lambda",
            "task_name" : "",
            "trigger" : "daytime=12:13"
        })";
    FromJsonString(config, jsonStr);
    ASSERT_EQ("lambda", config.GetTaskType());
    ASSERT_EQ("", config.GetTaskName());

    const auto& trigger = config.GetTrigger();
    ASSERT_EQ(Trigger::TriggerType::DATE_TIME, trigger.GetTriggerType());
    ASSERT_EQ(std::nullopt, trigger.GetTriggerPeriod());

    struct tm dayTime = trigger.GetTriggerDayTime().value();
    dayTime.tm_hour = 12;
    dayTime.tm_min = 13;
    ASSERT_EQ(12, dayTime.tm_hour);
    ASSERT_EQ(13, dayTime.tm_min);

    ASSERT_EQ("daytime=12:13", trigger.GetTriggerStr());
}

TEST_F(IndexTaskConfigTest, testPeriod)
{
    IndexTaskConfig config;
    std::string jsonStr = R"(
        {
            "task_type" : "lambda",
            "task_name" : "",
            "trigger" : "period=3600"
        })";
    FromJsonString(config, jsonStr);
    ASSERT_EQ("lambda", config.GetTaskType());
    ASSERT_EQ("", config.GetTaskName());

    const auto& trigger = config.GetTrigger();
    ASSERT_EQ(Trigger::TriggerType::PERIOD, trigger.GetTriggerType());
    ASSERT_EQ(3600, trigger.GetTriggerPeriod().value());
    ASSERT_EQ(std::nullopt, trigger.GetTriggerDayTime());
    ASSERT_EQ("period=3600", trigger.GetTriggerStr());
}

TEST_F(IndexTaskConfigTest, testManual)
{
    IndexTaskConfig config;
    std::string jsonStr = R"(
        {
            "task_type" : "lambda",
            "task_name" : "",
            "trigger" : "manual"
        })";
    FromJsonString(config, jsonStr);
    ASSERT_EQ("lambda", config.GetTaskType());
    ASSERT_EQ("", config.GetTaskName());

    const auto& trigger = config.GetTrigger();
    ASSERT_EQ(Trigger::TriggerType::MANUAL, trigger.GetTriggerType());
    ASSERT_EQ(std::nullopt, trigger.GetTriggerPeriod());
    ASSERT_EQ(std::nullopt, trigger.GetTriggerDayTime());
    ASSERT_EQ("manual", trigger.GetTriggerStr());
}

TEST_F(IndexTaskConfigTest, testError)
{
    {
        IndexTaskConfig config;
        std::string jsonStr = R"(
        {
            "task_type" : "lambda",
            "task_name" : "",
            "trigger" : "hello"
        })";
        FromJsonString(config, jsonStr);
        ASSERT_EQ(Trigger::TriggerType::ERROR, config.GetTrigger().GetTriggerType());
    }
    {
        IndexTaskConfig config;
        std::string jsonStr = R"(
        {
            "task_type" : "lambda",
            "task_name" : "",
            "trigger" : "123"
        })";
        FromJsonString(config, jsonStr);
        ASSERT_EQ(Trigger::TriggerType::ERROR, config.GetTrigger().GetTriggerType());
    }
    {
        IndexTaskConfig config;
        std::string jsonStr = R"(
        {
            "task_type" : "lambda",
            "task_name" : "",
            "trigger" : "daytime"
        })";
        FromJsonString(config, jsonStr);
        ASSERT_EQ(Trigger::TriggerType::ERROR, config.GetTrigger().GetTriggerType());
    }
    {
        IndexTaskConfig config;
        std::string jsonStr = R"(
        {
            "task_type" : "lambda",
            "task_name" : "",
            "trigger" : "daytime=12:123"
        })";
        FromJsonString(config, jsonStr);
        ASSERT_EQ(Trigger::TriggerType::ERROR, config.GetTrigger().GetTriggerType());
    }
    {
        IndexTaskConfig config;
        std::string jsonStr = R"(
        {
            "task_type" : "lambda",
            "task_name" : "",
            "trigger" : "period=12:10:12"
        })";
        FromJsonString(config, jsonStr);
        ASSERT_EQ(Trigger::TriggerType::ERROR, config.GetTrigger().GetTriggerType());
    }
}
} // namespace indexlibv2::config
