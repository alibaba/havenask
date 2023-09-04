#include "indexlib/config/TabletSchema.h"

#include <tuple>

#include "fslib/fs/FileSystem.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class TabletSchemaTest : public TESTBASE
{
protected:
    std::shared_ptr<TabletSchema> LoadSchema(const std::string& fileName) const
    {
        std::string jsonStr;
        auto ec = fslib::fs::FileSystem::readFile(GET_PRIVATE_TEST_DATA_PATH() + "/" + fileName, jsonStr);
        if (ec != fslib::EC_OK) {
            return nullptr;
        }
        auto tabletSchema = std::make_shared<TabletSchema>();
        if (!tabletSchema->Deserialize(jsonStr)) {
            return nullptr;
        }
        return tabletSchema;
    }
};

TEST_F(TabletSchemaTest, TestJson)
{
    std::string jsonStr = R"( {
        "table_type": "mock",
        "table_name": "table",
        "fields": [{"field_name": "long", "field_type": "LONG"}],
        "user_defined_param": {"vec": ["v1", "v2"], "str": "abc"},
        "settings": {"ttl_field_name": "ttl"}
    } )";
    TabletSchema schema;
    ASSERT_TRUE(schema.Deserialize(jsonStr));
    ASSERT_EQ("table", schema.GetTableName());
    ASSERT_EQ("mock", schema.GetTableType());
    ASSERT_EQ(1, schema.GetFieldCount());
    ASSERT_FALSE(schema.GetUserDefinedParam().Value<std::string>("vec").IsOK());
    ASSERT_EQ("abc", schema.GetUserDefinedParam().Value<std::string>("str").get());
    ASSERT_FALSE(schema.GetRuntimeSetting<std::string>("str").first.IsOK());
    ASSERT_EQ("ttl", schema.GetRuntimeSetting<std::string>("ttl_field_name").second);

    std::string jsonStr2;
    ASSERT_TRUE(schema.Serialize(true, &jsonStr2));
    TabletSchema schema2;
    ASSERT_TRUE(schema2.Deserialize(jsonStr2));
    ASSERT_EQ(schema.GetTableName(), schema2.GetTableName());
    ASSERT_EQ(schema.GetTableType(), schema2.GetTableType());
    ASSERT_EQ(schema.GetFieldCount(), schema2.GetFieldCount());
    ASSERT_FALSE(schema2.GetUserDefinedParam().Value<std::string>("vec").IsOK());
    ASSERT_EQ("abc", schema2.GetUserDefinedParam().Value<std::string>("str").steal_value());
    ASSERT_EQ("ttl", schema2.GetRuntimeSetting<std::string>("ttl_field_name").second);
}

TEST_F(TabletSchemaTest, TestRuntimeSettings)
{
    TabletSchema schema;
    auto [status, testValue] = schema.GetRuntimeSetting<int>("a.b");
    ASSERT_TRUE(status.IsNotFound());
    ASSERT_TRUE(schema.TEST_GetImpl()->SetRuntimeSetting("a.b", 100, true));
    std::tie(status, testValue) = schema.GetRuntimeSetting<int>("a.b");
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(100, testValue);

    struct InnerStruct : public autil::legacy::Jsonizable {
        int value = 0;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) final override
        {
            json.Jsonize("value", value, value);
        }
    };

    struct TestStruct : public autil::legacy::Jsonizable {
        int value = 0;
        InnerStruct complexValue;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) final override
        {
            json.Jsonize("value", value, value);
            json.Jsonize("complex_value", complexValue, complexValue);
        }
    };

    TestStruct structValue;
    structValue.value = -100;
    structValue.complexValue.value = -200;
    ASSERT_TRUE(schema.TEST_GetImpl()->SetRuntimeSetting("a.c", structValue, true));
    std::tie(status, testValue) = schema.GetRuntimeSetting<int>("a.b");
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(100, testValue);

    auto [status1, value1] = schema.GetRuntimeSetting<TestStruct>("a.c");
    ASSERT_TRUE(status1.IsOK());
    ASSERT_EQ(structValue.value, value1.value);

    auto [status2, value2] = schema.GetRuntimeSetting<int>("a.c.value");
    ASSERT_TRUE(status2.IsOK());
    ASSERT_EQ(structValue.value, value2);

    auto [status3, value3] = schema.GetRuntimeSetting<int>("a.c.complex_value.value");
    ASSERT_TRUE(status3.IsOK());
    ASSERT_EQ(structValue.complexValue.value, value3);

    ASSERT_TRUE(schema.TEST_GetImpl()->SetRuntimeSetting("a.c.complex_value.value", -999, true));
    auto [status4, value4] = schema.GetRuntimeSetting<int>("a.c.complex_value.value");
    ASSERT_TRUE(status4.IsOK());
    ASSERT_EQ(-999, value4);

    auto [status5, value5] = schema.GetRuntimeSetting<int>("a.c.value");
    ASSERT_TRUE(status5.IsOK());
    ASSERT_EQ(structValue.value, value5);
}

} // namespace indexlibv2::config
