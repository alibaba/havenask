#include "iquan/config/JniConfig.h"

#include <string>

#include "autil/Log.h"
#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class JniConfigTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, JniConfigTest);

TEST_F(JniConfigTest, testTableConfig) {
    TableConfig config;
    ASSERT_TRUE(config.isValid());
    ASSERT_EQ("_summary_", config.summaryTableSuffix);

    config.summaryTableSuffix = "";
    ASSERT_FALSE(config.isValid());
    ASSERT_EQ("", config.summaryTableSuffix);

    config.summaryTableSuffix = "test";
    ASSERT_TRUE(config.isValid());
    ASSERT_EQ("test", config.summaryTableSuffix);

    TableConfig config2 = config;
    ASSERT_TRUE(config2.isValid());
    ASSERT_EQ("test", config2.summaryTableSuffix);
}

TEST_F(JniConfigTest, testJniConfig) {
    JniConfig config;
    ASSERT_TRUE(config.isValid());
    ASSERT_EQ("_summary_", config.tableConfig.summaryTableSuffix);

    TableConfig tableConfig;
    ASSERT_TRUE(tableConfig.isValid());
    tableConfig.summaryTableSuffix = "test";
    config.tableConfig = tableConfig;
    ASSERT_TRUE(config.isValid());
    ASSERT_EQ("test", config.tableConfig.summaryTableSuffix);

    JniConfig config2 = config;
    ASSERT_TRUE(config2.isValid());
    ASSERT_EQ("test", config2.tableConfig.summaryTableSuffix);
}

} // namespace iquan
