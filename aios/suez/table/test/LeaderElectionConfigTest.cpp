#include "suez/table/LeaderElectionConfig.h"

#include "autil/EnvUtil.h"
#include "unittest/unittest.h"

using namespace testing;
using namespace autil;

namespace suez {

class LeaderElectionConfigTest : public TESTBASE {};

TEST_F(LeaderElectionConfigTest, testEmpty) {
    LeaderElectionConfig config;
    ASSERT_FALSE(config.initFromEnv());
}

TEST_F(LeaderElectionConfigTest, testZkRootSet) {
    {
        EnvGuard guard("zk_root", "zfs://path/to/some/zk");
        EnvGuard guard2("leader_election_strategy_type", "range");
        LeaderElectionConfig config;
        ASSERT_TRUE(config.initFromEnv());
        ASSERT_EQ("zfs://path/to/some/zk", config.zkRoot);
        ASSERT_EQ(LeaderElectionConfig::DEFAULT_ZK_TIMEOUT_IN_MS, config.zkTimeoutInMs);
        ASSERT_EQ(LeaderElectionConfig::DEFAULT_LEASE_IN_MS, config.leaseInMs);
        ASSERT_EQ(LeaderElectionConfig::DEFAULT_LOOP_INTERVAL_IN_MS, config.loopIntervalInMs);
        ASSERT_EQ(LeaderElectionConfig::DEFAULT_FORCE_FOLLOWER_TIME_IN_MS, config.forceFollowerTimeInMs);
        ASSERT_TRUE(config.campaignNow);
        ASSERT_EQ("range", config.strategyType);
        ASSERT_EQ(CLT_NORMAL, config.campaginLeaderType);
    }
    {
        EnvGuard guard("zk_root", "");
        LeaderElectionConfig config;
        ASSERT_FALSE(config.initFromEnv());
    }
}

TEST_F(LeaderElectionConfigTest, testLeaderElectionConfigSet) {
    {
        EnvGuard guard("leader_election_config", "");
        LeaderElectionConfig config;
        ASSERT_FALSE(config.initFromEnv());
    }
    {
        EnvGuard guard("leader_election_config", "not a json map");
        LeaderElectionConfig config;
        ASSERT_FALSE(config.initFromEnv());
    }
    {
        std::string jsonStr = R"doc({
"zk_root" : "",
"lease_in_ms" : 100
})doc";
        EnvGuard guard("leader_election_config", jsonStr);
        LeaderElectionConfig config;
        ASSERT_FALSE(config.initFromEnv());
    }
    // test default value
    {
        std::string jsonStr = R"doc({
"zk_root" : "zfs://path/to/zk"
})doc";
        EnvGuard guard("leader_election_config", jsonStr);
        LeaderElectionConfig config;
        ASSERT_TRUE(config.initFromEnv());
        ASSERT_EQ("zfs://path/to/zk", config.zkRoot);
        ASSERT_EQ(60 * 1000, config.leaseInMs);
        ASSERT_EQ(10 * 1000, config.zkTimeoutInMs);
        ASSERT_EQ(10 * 1000, config.loopIntervalInMs);
        ASSERT_EQ(10 * 1000, config.forceFollowerTimeInMs);
        ASSERT_EQ("", config.strategyType);
        ASSERT_TRUE(config.campaignNow);
        ASSERT_TRUE(config.needZoneName);
        ASSERT_FALSE(config.extraPublishRawTable);
        ASSERT_EQ(CLT_NORMAL, config.campaginLeaderType);
    }
    {
        std::string jsonStr = R"doc({
"zk_root" : "zfs://path/to/zk",
"zk_timeout_in_ms" : 700,
"lease_in_ms" : 100,
"loop_interval_in_ms" : 600,
"force_follower_time_in_ms":100,
"strategy_type" : "worker",
"campaign_now" : false,
"need_zone_name": false,
"extra_publish_raw_table": true,
"campagin_leader_type": "by_indication"
})doc";
        EnvGuard guard("leader_election_config", jsonStr);
        LeaderElectionConfig config;
        ASSERT_TRUE(config.initFromEnv());
        ASSERT_EQ("zfs://path/to/zk", config.zkRoot);
        ASSERT_EQ(100, config.leaseInMs);
        ASSERT_EQ(700, config.zkTimeoutInMs);
        ASSERT_EQ(600, config.loopIntervalInMs);
        ASSERT_EQ(100, config.forceFollowerTimeInMs);
        ASSERT_EQ("worker", config.strategyType);
        ASSERT_FALSE(config.campaignNow);
        ASSERT_FALSE(config.needZoneName);
        ASSERT_TRUE(config.extraPublishRawTable);
        ASSERT_EQ(CLT_BY_INDICATION, config.campaginLeaderType);
    }
}

} // namespace suez
