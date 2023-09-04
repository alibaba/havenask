#include "autil/EnvUtil.h"
#include "suez/table/VersionSynchronizer.h"
#include "unittest/unittest.h"

using namespace testing;
using namespace autil;

namespace suez {

class VersionSynchronizerConfigTest : public TESTBASE {};

TEST_F(VersionSynchronizerConfigTest, testEmpty) {
    VersionSynchronizerConfig config;
    ASSERT_FALSE(config.initFromEnv());
}

TEST_F(VersionSynchronizerConfigTest, testZkRootSet) {
    {
        EnvGuard guard("zk_root", "zfs://path/to/some/zk");
        VersionSynchronizerConfig config;
        ASSERT_TRUE(config.initFromEnv());
        ASSERT_EQ("zfs://path/to/some/zk/versions", config.zkRoot);
        ASSERT_EQ(VersionSynchronizerConfig::DEFAULT_ZK_TIMEOUT_IN_MS, config.zkTimeoutInMs);
    }
    {
        EnvGuard guard("zk_root", "");
        VersionSynchronizerConfig config;
        ASSERT_FALSE(config.initFromEnv());
    }
}

TEST_F(VersionSynchronizerConfigTest, testLocalModeSet) {
    {
        EnvGuard gurad("localMode", "true");
        VersionSynchronizerConfig config;
        ASSERT_TRUE(config.initFromEnv());
        ASSERT_EQ("LOCAL", config.zkRoot);
        ASSERT_EQ(VersionSynchronizerConfig::DEFAULT_ZK_TIMEOUT_IN_MS, config.zkTimeoutInMs);
    }
    {
        EnvGuard gurad("localMode", "false");
        VersionSynchronizerConfig config;
        ASSERT_FALSE(config.initFromEnv());
    }
}

TEST_F(VersionSynchronizerConfigTest, testVersionSyncConfigSet) {
    {
        EnvGuard guard("version_sync_config", "");
        VersionSynchronizerConfig config;
        ASSERT_FALSE(config.initFromEnv());
    }
    {
        EnvGuard guard("version_sync_config", "not a json map");
        VersionSynchronizerConfig config;
        ASSERT_FALSE(config.initFromEnv());
    }
    {
        std::string jsonStr = R"doc({
"zk_root" : "",
"zk_timeout_in_ms" : 100
})doc";
        EnvGuard guard("version_sync_config", jsonStr);
        VersionSynchronizerConfig config;
        ASSERT_FALSE(config.initFromEnv());
    }
    {
        std::string jsonStr = R"doc({
"zk_root" : "zfs://path/to/zk",
"zk_timeout_in_ms" : 100
})doc";
        EnvGuard guard("version_sync_config", jsonStr);
        VersionSynchronizerConfig config;
        ASSERT_TRUE(config.initFromEnv());
        ASSERT_EQ("zfs://path/to/zk/versions", config.zkRoot);
        ASSERT_EQ(100, config.zkTimeoutInMs);
    }
}

} // namespace suez
