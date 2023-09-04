#include "suez/table/PartitionProperties.h"

#include "autil/StringUtil.h"
#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

namespace suez {

class PartitionPropertiesTest : public TESTBASE {};

TEST_F(PartitionPropertiesTest, testSimple) {
    auto pid = TableMetaUtil::makePid("t", 1);
    auto target = TableMetaUtil::makeTarget(1, "/path/to/index/t", "/path/to/config/t/12345");

    PartitionProperties properties(pid);

    TableConfig tableConfig;
    tableConfig._realtime = true;
    ASSERT_TRUE(properties.init(target, tableConfig));
    ASSERT_TRUE(properties.hasRt);
    ASSERT_FALSE(properties.needReadRemoteIndex());
    ASSERT_EQ("/path/to/index/t/t/generation_1/partition_0_65535", properties.secondaryIndexRoot);
    ASSERT_TRUE(
        autil::StringUtil::endsWith(properties.primaryIndexRoot, "runtimedata/t/generation_1/partition_0_65535"));
    ASSERT_EQ(properties.primaryIndexRoot, properties.schemaRoot);
}

TEST_F(PartitionPropertiesTest, testLoadTableConfig) {
    TableConfig tableConfig;
    ASSERT_TRUE(
        tableConfig.loadFromFile(GET_TEST_DATA_PATH() + "/table_test/table_config", "table_cluster.json", false));
    ASSERT_EQ(std::string("normal"), tableConfig.GetDataLinkMode());
}

} // namespace suez
