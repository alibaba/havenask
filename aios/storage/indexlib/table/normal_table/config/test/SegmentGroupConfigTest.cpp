#include "indexlib/table/normal_table/config/SegmentGroupConfig.h"

#include "indexlib/config/MutableJson.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/attribute/expression/FunctionConfig.h"
#include "indexlib/table/normal_table/Common.h"
#include "unittest/unittest.h"

namespace indexlibv2::table {

class SegmentGroupConfigTest : public TESTBASE
{
public:
    SegmentGroupConfigTest() = default;
    ~SegmentGroupConfigTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void SegmentGroupConfigTest::setUp() {}

void SegmentGroupConfigTest::tearDown() {}

TEST_F(SegmentGroupConfigTest, testJsonize)
{
    std::string content = R"({
                "groups":["hot_orders", "cold_orders"]
        })";
    SegmentGroupConfig config;
    ASSERT_NO_THROW(FromJsonString(config, content));
    ASSERT_EQ(std::vector<std::string>({"hot_orders", "cold_orders", "default_group"}), config.GetGroups());
    ASSERT_EQ(std::vector<std::string>(
                  {"hot_orders", "cold_orders", index::AttributeFunctionConfig::ALWAYS_TRUE_FUNCTION_NAME}),
              config.GetFunctionNames());
}

TEST_F(SegmentGroupConfigTest, testLoadFromSchema)
{
    auto schema = framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema.json");
    ASSERT_TRUE(schema);
    auto [status, segmentGroupConfig] =
        schema->GetRuntimeSettings().GetValue<SegmentGroupConfig>(NORMAL_TABLE_GROUP_CONFIG_KEY);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(std::vector<std::string>({"hot_orders", "cold_orders", "default_group"}), segmentGroupConfig.GetGroups());
}

TEST_F(SegmentGroupConfigTest, testIsGroupEnabled)
{
    {
        SegmentGroupConfig config;
        ASSERT_FALSE(config.IsGroupEnabled());
        ASSERT_EQ(0, config.GetGroups().size());
        ASSERT_EQ(0, config.GetFunctionNames().size());
    }
    {
        SegmentGroupConfig config;
        config.TEST_SetGroups({"group1"});
        ASSERT_TRUE(config.IsGroupEnabled());
        ASSERT_EQ(2, config.GetGroups().size());
        ASSERT_EQ(2, config.GetFunctionNames().size());
    }
    {
        SegmentGroupConfig config;
        config.TEST_SetGroups({"group1", "group2"});
        ASSERT_TRUE(config.IsGroupEnabled());
        ASSERT_EQ(3, config.GetGroups().size());
        ASSERT_EQ(3, config.GetFunctionNames().size());
    }
}

} // namespace indexlibv2::table
