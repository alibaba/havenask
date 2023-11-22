#include "build_service/config/AgentGroupConfig.h"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace config {

class AgentGroupConfigTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void AgentGroupConfigTest::setUp() {}

void AgentGroupConfigTest::tearDown() {}

TEST_F(AgentGroupConfigTest, testSimple)
{
    string jsonStr = R"( {
        "agent_node_groups" :
        [
            {
                "id" : "processor",
                "role_pattern" : "*processor*",
                "node_count" : 2,
                "in_blacklist_timeout" : 1800,
                "resource_limits" : [
                    {
                        "resource" : "cpu",
                        "oversell_factor" : 1.2
                    }
                ]
            },
            {
                "id" : "default",
                "role_pattern" : "*",
                "node_count" : 1
            }
        ] 
    } )";

    AgentGroupConfig config;
    autil::legacy::FromJsonString(config, jsonStr);
    ASSERT_TRUE(config.check());

    ASSERT_EQ(2, config.getAgentConfigs().size());
    ASSERT_EQ(string("processor"), config.getAgentConfigs()[0].identifier);
    ASSERT_EQ(string("*processor*"), config.getAgentConfigs()[0].rolePattern);
    ASSERT_EQ(2, config.getAgentConfigs()[0].agentNodeCount);
    ASSERT_EQ(1, config.getAgentConfigs()[0].resourceLimitParams.size());
    ASSERT_EQ(string("cpu"), config.getAgentConfigs()[0].resourceLimitParams[0].resourceName);
    ASSERT_EQ(1.2, config.getAgentConfigs()[0].resourceLimitParams[0].oversellFactor);

    // over 1800 seconds, role in black list will release to normal
    ASSERT_EQ(1800, config.getAgentConfigs()[0].inBlackListTimeout);

    ASSERT_EQ(string("default"), config.getAgentConfigs()[1].identifier);
    ASSERT_EQ(string("*"), config.getAgentConfigs()[1].rolePattern);
    ASSERT_EQ(1, config.getAgentConfigs()[1].agentNodeCount);
    ASSERT_EQ(AgentConfig::DEFAULT_IN_BLACKLIST_TIMEOUT, config.getAgentConfigs()[1].inBlackListTimeout);
}

TEST_F(AgentGroupConfigTest, testCheck)
{
    string jsonStr = R"( {
        "agent_node_groups" :
        [
            {
                "id" : "processor",
                "role_pattern" : "*processor*",
                "node_count" : 0
            }
        ] 
    } )";

    AgentGroupConfig config;
    autil::legacy::FromJsonString(config, jsonStr);
    ASSERT_FALSE(config.check());
}

TEST_F(AgentGroupConfigTest, testMatch)
{
    string jsonStr = R"( {
        "agent_node_groups" :
        [
            {
                "id" : "processor",
                "role_pattern" : ".*processor.*",
                "node_count" : 2
            },
            {
                "id" : "default",
                "role_pattern" : ".*builder.*",
                "node_count" : 1
            }
        ] 
    } )";

    AgentGroupConfig config;
    autil::legacy::FromJsonString(config, jsonStr);
    ASSERT_TRUE(config.check());

    size_t idx;
    ASSERT_TRUE(config.match("abc.123.processor.full.0.65535", idx));
    ASSERT_EQ(0, idx);
    ASSERT_TRUE(config.match("abc.123.builder.full.0.65535", idx));
    ASSERT_EQ(1, idx);
    ASSERT_FALSE(config.match("abc.123.task.full.0.65535", idx));
}

}} // namespace build_service::config
