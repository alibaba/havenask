#include "build_service/config/ProcessorRuleConfig.h"

#include <iosfwd>
#include <string>

#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/ProcessorAdaptiveScalingConfig.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace config {

class ProcessorRuleConfigTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void ProcessorRuleConfigTest::setUp() {}

void ProcessorRuleConfigTest::tearDown() {}

TEST_F(ProcessorRuleConfigTest, testSimple)
{
    std::string configStr = R"({
       "partition_count" : 1 ,
       "adaptive_scaling_config" : {
          "enable_adaptive_scaling" : true,
          "max_count" : 128
       }
    })";
    ProcessorRuleConfig config;
    FromJsonString(config, configStr);
    ASSERT_EQ(128, config.adaptiveScalingConfig.maxCount);
    ASSERT_TRUE(config.adaptiveScalingConfig.enableAdaptiveScaling);
    ASSERT_TRUE(config.adaptiveScalingConfig.cpuStrategy.enabled);
    ASSERT_EQ(3600, config.adaptiveScalingConfig.cpuStrategy.sampleWindow);
    ASSERT_EQ(80, config.adaptiveScalingConfig.cpuStrategy.cpuMaxRatio);
    ASSERT_EQ(40, config.adaptiveScalingConfig.cpuStrategy.cpuMinRatio);

    ASSERT_TRUE(config.adaptiveScalingConfig.latencyStrategy.enabled);
    ASSERT_EQ(600, config.adaptiveScalingConfig.latencyStrategy.latencyGap);
    ASSERT_EQ(30, config.adaptiveScalingConfig.latencyStrategy.delayedWorkerPercent);
    ASSERT_TRUE(config.validate());
}

TEST_F(ProcessorRuleConfigTest, validConfig)
{
    {
        std::string configStr = R"({
       "partition_count" : 1 ,
       "adaptive_scaling_config" : {
          "enable_adaptive_scaling" : true,
          "max_count" : 128,
          "cpu_adjust_strategy" : {
              "expect_max_ratio" : 120
           }
       }
    })";
        ProcessorRuleConfig config;
        FromJsonString(config, configStr);
        ASSERT_FALSE(config.validate());
    }
    {
        std::string configStr = R"({
       "partition_count" : 1 ,
       "adaptive_scaling_config" : {
          "enable_adaptive_scaling" : true,
          "max_count" : 128,
          "latency_adjust_strategy" : {
             "delayed_worker_percent" : 120
           }
       }
    })";
        ProcessorRuleConfig config;
        FromJsonString(config, configStr);
        ASSERT_FALSE(config.validate());
    }
}

}} // namespace build_service::config
