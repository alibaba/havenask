#include "indexlib/table/normal_table/index_task/NormalTableTaskPlanCreator.h"

#include "autil/StringTokenizer.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/config/SegmentGroupConfig.h"
#include "indexlib/table/normal_table/index_task/NormalTableReclaimPlanCreator.h"
#include "indexlib/table/normal_table/index_task/NormalTableSplitPlanCreator.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "unittest/unittest.h"
namespace indexlibv2::table {

class NormalTableTaskPlanCreatorTest : public TESTBASE
{
public:
    NormalTableTaskPlanCreatorTest() = default;
    ~NormalTableTaskPlanCreatorTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    struct tm _defaultSplitTm;
    int64_t _defalutTTLInterval;
};

void NormalTableTaskPlanCreatorTest::setUp()
{
    autil::StringTokenizer st1(NormalTableSplitPlanCreator::DEFAULT_TASK_PERIOD.c_str(), "=",
                               autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    assert(st1.getNumTokens() == 2);
    strptime(st1[1].c_str(), "%H:%M", &_defaultSplitTm);

    autil::StringTokenizer st2(NormalTableReclaimPlanCreator::DEFAULT_TASK_PERIOD, "=",
                               autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    assert(st2.getNumTokens() == 2);
    autil::StringUtil::fromString(st2[1], _defalutTTLInterval);
}

void NormalTableTaskPlanCreatorTest::tearDown() {}

TEST_F(NormalTableTaskPlanCreatorTest, TestSelectTaskConfigs)
{
    auto GetTaskConfigs = [](bool hasSplitConfig, bool hasGroupConfig, bool enableTTL, bool hasTTLTaskConfig,
                             std::vector<config::IndexTaskConfig>* configs) {
        NormalTableTaskPlanCreator creator;
        auto options = std::make_shared<config::TabletOptions>();
        auto& offlineConfig = options->TEST_GetOfflineConfig();
        auto& indexTaskConfigs = offlineConfig.TEST_GetIndexTaskConfigs();
        if (hasSplitConfig) {
            indexTaskConfigs.emplace_back("simple", NormalTableSplitPlanCreator::TASK_TYPE, "period=300");
        }

        if (hasTTLTaskConfig) {
            indexTaskConfigs.emplace_back("ttl", NormalTableReclaimPlanCreator::TASK_TYPE, "period=300");
        }

        std::string groupConfigStr = R"({"groups":["cheap", "expensive"] })";
        SegmentGroupConfig groupConfig;
        ASSERT_NO_THROW(autil::legacy::FromJsonString(groupConfig, groupConfigStr));

        std::string field = "string1:string;price:uint32";
        std::string index = "pk:primarykey64:string1";
        std::string attribute = "price";
        auto tabletSchema = NormalTabletSchemaMaker::Make(field, index, attribute, "");
        auto unresolvedSchema = tabletSchema->TEST_GetImpl();
        if (hasGroupConfig) {
            ASSERT_TRUE(unresolvedSchema->SetRuntimeSetting(NORMAL_TABLE_GROUP_CONFIG_KEY, ToJson(groupConfig), true));
        }
        if (enableTTL) {
            unresolvedSchema->SetRuntimeSetting("enable_ttl", true, true);
        }

        framework::IndexTaskContext context;
        context.SetTabletOptions(options);
        context.TEST_SetTabletSchema(tabletSchema);
        ASSERT_TRUE(creator.SelectTaskConfigs(&context, configs).IsOK());
    };
    {
        std::vector<config::IndexTaskConfig> configs;
        GetTaskConfigs(/*hasSplitConfig*/ false, /*hasGroupConfig*/ true, /*enableTTL*/ false, /*hasTTLConfig*/ false,
                       &configs);
        ASSERT_EQ(1, configs.size());
        ASSERT_EQ(NormalTableSplitPlanCreator::DEFAULT_TASK_NAME, configs[0].GetTaskName());
        ASSERT_EQ(NormalTableSplitPlanCreator::TASK_TYPE, configs[0].GetTaskType());
        auto dayTime = configs[0].GetTrigger().GetTriggerDayTime();
        ASSERT_EQ(_defaultSplitTm.tm_min, dayTime.value().tm_min);
        ASSERT_EQ(_defaultSplitTm.tm_hour, dayTime.value().tm_hour);
    }
    {
        std::vector<config::IndexTaskConfig> configs;
        GetTaskConfigs(/*hasSplitConfig*/ true, /*hasGroupConfig*/ true, /*enableTTL*/ false, /*hasTTLConfig*/ false,
                       &configs);
        ASSERT_EQ(1, configs.size());
        ASSERT_EQ("simple", configs[0].GetTaskName());
        ASSERT_EQ(NormalTableSplitPlanCreator::TASK_TYPE, configs[0].GetTaskType());
        auto period = configs[0].GetTrigger().GetTriggerPeriod();
        ASSERT_EQ(300, period.value());
    }
    {
        std::vector<config::IndexTaskConfig> configs;
        GetTaskConfigs(/*hasSplitConfig*/ false, /*hasGroupConfig*/ false, /*enableTTL*/ false, /*hasTTLConfig*/ false,
                       &configs);
        ASSERT_EQ(0, configs.size());
    }
    {
        std::vector<config::IndexTaskConfig> configs;
        GetTaskConfigs(/*hasSplitConfig*/ true, /*hasGroupConfig*/ false, /*enableTTL*/ false, /*hasTTLConfig*/ false,
                       &configs);
        ASSERT_EQ(1, configs.size());
    }

    // test for ttl defalut config
    {
        std::vector<config::IndexTaskConfig> configs;
        GetTaskConfigs(/*hasSplitConfig*/ false, /*hasGroupConfig*/ false, /*enableTTL*/ true, /*hasTTLConfig*/ false,
                       &configs);
        ASSERT_EQ(1, configs.size());
        ASSERT_EQ(NormalTableReclaimPlanCreator::DEFAULT_TASK_NAME, configs[0].GetTaskName());
        ASSERT_EQ(NormalTableReclaimPlanCreator::TASK_TYPE, configs[0].GetTaskType());
        auto period = configs[0].GetTrigger().GetTriggerPeriod();
        ASSERT_EQ(_defalutTTLInterval, period.value());
    }

    {
        std::vector<config::IndexTaskConfig> configs;
        GetTaskConfigs(/*hasSplitConfig*/ false, /*hasGroupConfig*/ false, /*enableTTL*/ true, /*hasTTLConfig*/ true,
                       &configs);
        ASSERT_EQ(1, configs.size());
        ASSERT_EQ("ttl", configs[0].GetTaskName());
        ASSERT_EQ(NormalTableReclaimPlanCreator::TASK_TYPE, configs[0].GetTaskType());
        auto period = configs[0].GetTrigger().GetTriggerPeriod();
        ASSERT_EQ(300, period.value());
    }

    {
        std::vector<config::IndexTaskConfig> configs;
        GetTaskConfigs(/*hasSplitConfig*/ false, /*hasGroupConfig*/ true, /*enableTTL*/ true, /*hasTTLConfig*/ false,
                       &configs);
        ASSERT_EQ(2, configs.size());
        ASSERT_EQ(NormalTableSplitPlanCreator::DEFAULT_TASK_NAME, configs[0].GetTaskName());
        ASSERT_EQ(NormalTableSplitPlanCreator::TASK_TYPE, configs[0].GetTaskType());

        auto dayTime = configs[0].GetTrigger().GetTriggerDayTime();
        ASSERT_EQ(_defaultSplitTm.tm_min, dayTime.value().tm_min);
        ASSERT_EQ(_defaultSplitTm.tm_hour, dayTime.value().tm_hour);

        ASSERT_EQ(NormalTableReclaimPlanCreator::DEFAULT_TASK_NAME, configs[1].GetTaskName());
        ASSERT_EQ(NormalTableReclaimPlanCreator::TASK_TYPE, configs[1].GetTaskType());
        auto period = configs[1].GetTrigger().GetTriggerPeriod();
        ASSERT_EQ(_defalutTTLInterval, period.value());
    }
}

} // namespace indexlibv2::table
