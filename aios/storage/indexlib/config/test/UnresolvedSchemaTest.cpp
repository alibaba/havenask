#include "indexlib/config/UnresolvedSchema.h"

#include <tuple>

#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class UnresolvedSchemaTest : public TESTBASE
{
};

TEST_F(UnresolvedSchemaTest, TestSettings)
{
    UnresolvedSchema schema;
    // test simple type
    {
        ASSERT_FALSE(schema.GetRuntimeSettings().GetValue<int>("a.b").first.IsOK());
        // don't know testIntValue, it's random

        ASSERT_TRUE(schema.SetRuntimeSetting("a.b", 100, false));
        ASSERT_EQ(100, schema.GetRuntimeSettings().GetValue<int>("a.b").second);
    }

    // test exception case
    {
        ASSERT_TRUE(schema.SetRuntimeSetting("a.c.e", "not_a_long", false));
        ASSERT_FALSE(schema.GetRuntimeSettings().GetValue<long>("a.c.e").first.IsOK());

        ASSERT_TRUE(schema.SetRuntimeSetting("a.c.e", 1, true));
        ASSERT_EQ(1, schema.GetRuntimeSettings().GetValue<long>("a.c.e").second);
    }
}

TEST_F(UnresolvedSchemaTest, TestSettingsWithDefaultValue)
{
    UnresolvedSchema schema;
    // test simple type
    {
        int testIntValue = -9999;
        auto status = schema.GetRuntimeSetting("a.b", testIntValue, 123);
        ASSERT_TRUE(status.IsOK());
        // should not change value when not found
        ASSERT_EQ(testIntValue, 123);

        ASSERT_TRUE(schema.SetRuntimeSetting("a.b", 100, false));
        status = schema.GetRuntimeSetting("a.b", testIntValue, 456);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(100, testIntValue);
    }

    // test exception case
    {
        ASSERT_TRUE(schema.SetRuntimeSetting("a.c.e", "not_a_long", false));
        long testLongValue = -9999;
        auto status = schema.GetRuntimeSetting("a.c.e", testLongValue, 456L);
        ASSERT_FALSE(status.IsOK());
        ASSERT_EQ(testLongValue, -9999);

        ASSERT_TRUE(schema.SetRuntimeSetting("a.c.e", 1, true));
        status = schema.GetRuntimeSetting("a.c.e", testLongValue, 789L);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(testLongValue, 1);
    }
}

TEST_F(UnresolvedSchemaTest, TestDeserializeLegacySchema)
{
    std::string legacySchemaStr;
    //老schema，动态减字段再加一个同名字段的schema
    ASSERT_EQ(fslib::ErrorCode::EC_OK,
              fslib::fs::FileSystem::readFile(GET_PRIVATE_TEST_DATA_PATH() + "/legacy_schema_1.json", legacySchemaStr));
    UnresolvedSchema schema;
    ASSERT_TRUE(schema.Deserialize(legacySchemaStr));
}

} // namespace indexlibv2::config
