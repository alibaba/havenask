#include "indexlib/index/attribute/expression/FunctionConfig.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/attribute/Common.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class FunctionConfigTest : public TESTBASE
{
public:
    FunctionConfigTest() = default;
    ~FunctionConfigTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void FunctionConfigTest::setUp() {}

void FunctionConfigTest::tearDown() {}

TEST_F(FunctionConfigTest, testJsonize)
{
    std::string expectExpression =
        R"($currentTime - ops_user__gmt_modified >= 0 AND $currentTime - ops_user__gmt_modified <= 25920000)";
    std::string content = R"({
              "name": "hot_order",
              "expression": "$currentTime - ops_user__gmt_modified >= 0 AND $currentTime - ops_user__gmt_modified <= 25920000"
})";
    std::shared_ptr<FunctionConfig> config;
    ASSERT_NO_THROW(FromJsonString(config, content));
    ASSERT_EQ("hot_order", config->GetName());
    ASSERT_EQ(expectExpression, config->GetExpression());
}

TEST_F(FunctionConfigTest, testReadFromSchema)
{
    auto schema = framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema.json");
    ASSERT_TRUE(schema);
    auto [status, functionConfigs] =
        schema->GetRuntimeSettings().GetValue<AttributeFunctionConfig>(ATTRIBUTE_FUNCTION_CONFIG_KEY);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(2, functionConfigs.size());
    ASSERT_EQ("hot_order", functionConfigs[0]->GetName());
    ASSERT_EQ("cold_order", functionConfigs[1]->GetName());
    ASSERT_EQ(
        R"(($currentTime - ops_user__gmt_modified >= 0 AND $currentTime - ops_user__gmt_modified <= 25920000) AND (ops_user__order_status = 0))",
        functionConfigs[0]->GetExpression());
    ASSERT_EQ(R"($currentTime - ops_user__gmt_modified > 25920000)", functionConfigs[1]->GetExpression());
}

} // namespace indexlibv2::index
