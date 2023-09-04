#include "iquan/config/ExecConfig.h"

#include <algorithm>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class ExecConfigTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, ExecConfigTest);

TEST_F(ExecConfigTest, testParallelConfig) {
    ParallelConfig config;
    ASSERT_TRUE(config.isValid());

    config.parallelNum = 0;
    ASSERT_FALSE(config.isValid());

    config.parallelNum = 10;
    ASSERT_TRUE(config.isValid());
    ASSERT_EQ(10, config.parallelNum);
    config.parallelTables.emplace_back("a");
    ASSERT_TRUE(config.isValid());
    ASSERT_EQ(1, config.parallelTables.size());
    ASSERT_EQ("a", config.parallelTables[0]);

    ParallelConfig config2 = config;
    ASSERT_TRUE(config2.isValid());
    ASSERT_EQ(10, config2.parallelNum);
    ASSERT_EQ(1, config2.parallelTables.size());
    ASSERT_EQ("a", config2.parallelTables[0]);
}

TEST_F(ExecConfigTest, testRuntimeConfig) {
    RuntimeConfig config;
    ASSERT_TRUE(config.isValid());

    config.threadLimit = 0;
    config.timeout = 0;
    ASSERT_FALSE(config.isValid());

    config.threadLimit = 10;
    ASSERT_FALSE(config.isValid());

    config.timeout = 20;
    ASSERT_TRUE(config.isValid());

    config.traceLevel = "test_level";
    ASSERT_TRUE(config.isValid());

    RuntimeConfig config2 = config;
    ASSERT_EQ(10, config2.threadLimit);
    ASSERT_EQ(20, config2.timeout);
    ASSERT_EQ("test_level", config2.traceLevel);
}

TEST_F(ExecConfigTest, testNaviRunGraphConfig) {
    NaviRunGraphConfig config;
    ASSERT_TRUE(config.isValid());

    RuntimeConfig runtimeConfig;
    ASSERT_TRUE(runtimeConfig.isValid());
    runtimeConfig.threadLimit = 100;
    runtimeConfig.timeout = 100;
    runtimeConfig.traceLevel = "test_level";
    ASSERT_TRUE(runtimeConfig.isValid());

    config.runtimeConfig = runtimeConfig;
    ASSERT_TRUE(config.isValid());

    NaviRunGraphConfig config2 = config;
    ASSERT_EQ(100, config2.runtimeConfig.threadLimit);
    ASSERT_EQ(100, config2.runtimeConfig.timeout);
    ASSERT_EQ("test_level", config2.runtimeConfig.traceLevel);
    ASSERT_TRUE(config2.isValid());
}

TEST_F(ExecConfigTest, testExecConfig) {
    ExecConfig config;
    ASSERT_TRUE(config.isValid());

    config.parallelConfig.parallelNum = 100;
    config.parallelConfig.parallelTables.emplace_back("abc");
    config.naviConfig.runtimeConfig.threadLimit = 50;
    config.naviConfig.runtimeConfig.timeout = 50;
    config.naviConfig.runtimeConfig.traceLevel = "test_level";
    ASSERT_TRUE(config.isValid());

    ExecConfig config2 = config;
    ASSERT_EQ(100, config2.parallelConfig.parallelNum);
    ASSERT_EQ("abc", config2.parallelConfig.parallelTables[0]);
    ASSERT_EQ(50, config2.naviConfig.runtimeConfig.threadLimit);
    ASSERT_EQ(50, config2.naviConfig.runtimeConfig.timeout);
    ASSERT_EQ("test_level", config2.naviConfig.runtimeConfig.traceLevel);
}

} // namespace iquan
