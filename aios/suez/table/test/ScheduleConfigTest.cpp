#include "suez/table/ScheduleConfig.h"

#include "autil/EnvUtil.h"
#include "unittest/unittest.h"

using namespace testing;
using namespace autil;

namespace suez {

class ScheduleConfigTest : public TESTBASE {};

TEST_F(ScheduleConfigTest, testDefault) {
    ScheduleConfig defaultConfig;
    ASSERT_TRUE(defaultConfig.allowForceLoad);
    ASSERT_FALSE(defaultConfig.allowReloadByConfig);
    ASSERT_FALSE(defaultConfig.allowReloadByIndexRoot);
    ASSERT_FALSE(defaultConfig.allowPreload);
    ASSERT_TRUE(defaultConfig.allowFastCleanIncVersion);
    ASSERT_EQ(60, defaultConfig.fastCleanIncVersionIntervalInSec);
    ASSERT_FALSE(defaultConfig.allowFollowerWrite);

    defaultConfig.initFromEnv();
    ASSERT_TRUE(defaultConfig.allowForceLoad);
    ASSERT_FALSE(defaultConfig.allowReloadByConfig);
    ASSERT_FALSE(defaultConfig.allowReloadByIndexRoot);
    ASSERT_FALSE(defaultConfig.allowPreload);
    ASSERT_TRUE(defaultConfig.allowFastCleanIncVersion);
    ASSERT_EQ(60, defaultConfig.fastCleanIncVersionIntervalInSec);
    ASSERT_FALSE(defaultConfig.allowFollowerWrite);
    ASSERT_TRUE(defaultConfig.allowLoadUtilRtRecovered);
}

TEST_F(ScheduleConfigTest, testAllowReloadByIndexRoot) {
    EnvGuard guard("RS_ALLOW_RELOAD_BY_INDEX_ROOT", "true");
    ScheduleConfig config;
    ASSERT_FALSE(config.allowReloadByIndexRoot);
    config.initFromEnv();
    ASSERT_TRUE(config.allowReloadByIndexRoot);
}

TEST_F(ScheduleConfigTest, testAllowReloadByConfig) {
    EnvGuard guard("RS_ALLOW_RELOAD_BY_CONFIG", "true");
    ScheduleConfig config;
    ASSERT_FALSE(config.allowReloadByConfig);
    config.initFromEnv();
    ASSERT_TRUE(config.allowReloadByConfig);
}

TEST_F(ScheduleConfigTest, testDisableForceLoad) {
    EnvGuard guard("RS_ALLOW_FORCELOAD", "false");
    ScheduleConfig config;
    ASSERT_TRUE(config.allowForceLoad);
    config.initFromEnv();
    ASSERT_FALSE(config.allowForceLoad);
}

TEST_F(ScheduleConfigTest, testAllowPreload) {
    EnvGuard guard("ALLOW_PRELOAD", "true");
    ScheduleConfig config;
    ASSERT_FALSE(config.allowPreload);
    config.initFromEnv();
    ASSERT_TRUE(config.allowPreload);
}

TEST_F(ScheduleConfigTest, testAllowFastCleanIncVersion) {
    EnvGuard guard("FAST_CLEAN_INC_VERSION", "true");
    ScheduleConfig config;
    config.initFromEnv();
    ASSERT_TRUE(config.allowFastCleanIncVersion);
}

TEST_F(ScheduleConfigTest, testFastCleanIncVersionInterval) {
    EnvGuard guard("FAST_CLEAN_INC_VERSION_INTERVAL_IN_SEC", "100");
    ScheduleConfig config;
    config.initFromEnv();
    ASSERT_EQ(100, config.fastCleanIncVersionIntervalInSec);
}

TEST_F(ScheduleConfigTest, testAllowFollowerWrite) {
    EnvGuard guard("ALLOW_FOLLOWER_WRITE", "true");
    ScheduleConfig config;
    config.initFromEnv();
    ASSERT_TRUE(config.allowFollowerWrite);
}

TEST_F(ScheduleConfigTest, testAllowLoadUtilRtRecovered) {
    EnvGuard guard("ALLOW_LOAD_UTIL_RT_RECOVERED", "false");
    ScheduleConfig config;
    config.initFromEnv();
    ASSERT_FALSE(config.allowLoadUtilRtRecovered);
}

} // namespace suez
