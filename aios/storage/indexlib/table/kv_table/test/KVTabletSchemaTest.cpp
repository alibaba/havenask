#include "fslib/fs/FileSystem.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/table/BuiltinDefine.h"
#include "unittest/unittest.h"

namespace indexlibv2::table {

class KVTabletSchemaTest : public TESTBASE
{
protected:
    std::shared_ptr<config::ITabletSchema> LoadSchema(const std::string& fileName) const
    {
        std::shared_ptr<config::ITabletSchema> tabletSchema =
            framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), fileName);
        if (!tabletSchema) {
            return nullptr;
        }
        auto status = framework::TabletSchemaLoader::ResolveSchema(nullptr, "", tabletSchema.get());
        if (status.IsOK()) {
            return tabletSchema;
        }
        return nullptr;
    }
};

TEST_F(KVTabletSchemaTest, testKVLoad)
{
    {
        auto schema = LoadSchema("kv_schema.json");
        ASSERT_TRUE(schema != nullptr);
        ASSERT_EQ(indexlib::table::TABLE_TYPE_KV, schema->GetTableType());

        auto config = schema->GetIndexConfig(index::KV_INDEX_TYPE_STR, "member_id_sim_member_id");
        ASSERT_TRUE(config);
        auto kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(config);
        ASSERT_TRUE(kvConfig);
        auto valueConfig = kvConfig->GetValueConfig();
        ASSERT_EQ(4u, valueConfig->GetAttributeCount());

        auto [status, packAttributeConfig] = valueConfig->CreatePackAttributeConfig();
        ASSERT_TRUE(status.IsOK());
        std::vector<std::string> fieldNames;
        packAttributeConfig->GetSubAttributeNames(fieldNames);
        std::vector<std::string> expected = {"seller_id", "sim_seller_id", "sim_member_id", "score"};
        std::sort(fieldNames.begin(), fieldNames.end());
        std::sort(expected.begin(), expected.end());
        ASSERT_EQ(indexlib::ipt_perf, kvConfig->GetIndexPreference().GetType());
    }
    {
        auto schema = LoadSchema("kv_schema_tablet.json");
        ASSERT_TRUE(schema != nullptr);
        ASSERT_EQ(indexlib::table::TABLE_TYPE_KV, schema->GetTableType());

        auto config = schema->GetIndexConfig(index::KV_INDEX_TYPE_STR, "member_id_sim_member_id");
        ASSERT_TRUE(config);
        auto kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(config);
        ASSERT_TRUE(kvConfig);
        auto valueConfig = kvConfig->GetValueConfig();
        ASSERT_EQ(4u, valueConfig->GetAttributeCount());

        auto [status, packAttributeConfig] = valueConfig->CreatePackAttributeConfig();
        ASSERT_TRUE(status.IsOK());
        std::vector<std::string> fieldNames;
        packAttributeConfig->GetSubAttributeNames(fieldNames);
        std::vector<std::string> expected = {"seller_id", "sim_seller_id", "sim_member_id", "score"};
        std::sort(fieldNames.begin(), fieldNames.end());
        std::sort(expected.begin(), expected.end());
        ASSERT_EQ(indexlib::ipt_perf, kvConfig->GetIndexPreference().GetType());
    }
}

TEST_F(KVTabletSchemaTest, testException)
{
    ASSERT_FALSE(LoadSchema("kv_schema_exception.json"));  // 压缩buffer大小不当
    ASSERT_FALSE(LoadSchema("kv_schema_exception1.json")); // pk字段不能是多值
    ASSERT_FALSE(LoadSchema("kv_schema_exception2.json")); // 类型问题
    ASSERT_FALSE(LoadSchema("kv_schema_without_attributes.json"));
    ASSERT_FALSE(LoadSchema("kv_schema_without_fields.json"));
    ASSERT_FALSE(LoadSchema("kv_schema_with_suppport_null_field.json"));

    // ASSERT_FALSE(LoadSchema("kv_schema_multi_region.json")); //新架构不支持多region
    // ASSERT_FALSE(LoadSchema("kv_schema_multi_region_field.json)); //新架构不支持多region

    // test tablet schema
    ASSERT_FALSE(LoadSchema("kv_schema_exception_tablet.json"));
    ASSERT_FALSE(LoadSchema("kv_schema_exception1_tablet.json"));
    ASSERT_TRUE(LoadSchema("kv_schema_exception2_tablet.json")); //新 schema 没有pk index type
    ASSERT_FALSE(LoadSchema("kv_schema_without_attributes_tablet.json"));
    ASSERT_FALSE(LoadSchema("kv_schema_without_fields_tablet.json"));
    ASSERT_FALSE(LoadSchema("kv_schema_with_suppport_null_field_tablet.json"));
}

TEST_F(KVTabletSchemaTest, testMultiKVLoad)
{
    using indexlibv2::config::KVIndexConfig;
    auto schema = LoadSchema("multi_kv_schema.json");
    ASSERT_TRUE(schema != nullptr);
    ASSERT_EQ(indexlib::table::TABLE_TYPE_KV, schema->GetTableType());

    auto config1 = std::dynamic_pointer_cast<KVIndexConfig>(schema->GetIndexConfig("kv", "index1"));
    ASSERT_TRUE(config1);
    ASSERT_EQ("index1", config1->GetIndexName());
    ASSERT_TRUE(config1->GetFieldConfig());
    ASSERT_TRUE(config1->GetValueConfig());

    auto config2 = std::dynamic_pointer_cast<KVIndexConfig>(schema->GetIndexConfig("kv", "index2"));
    ASSERT_TRUE(config2);
    ASSERT_EQ("index2", config2->GetIndexName());
    ASSERT_TRUE(config2->GetFieldConfig());
    ASSERT_TRUE(config2->GetValueConfig());
}

TEST_F(KVTabletSchemaTest, testPKValueStore)
{
    {
        auto schema = LoadSchema("pack_kv_schema.json");
        ASSERT_TRUE(schema != nullptr);
        ASSERT_EQ(indexlib::table::TABLE_TYPE_KV, schema->GetTableType());
        ASSERT_EQ(size_t(2), schema->GetIndexConfigs().size());

        auto config1 =
            std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(schema->GetIndexConfig("kv", "index1"));
        ASSERT_TRUE(config1);
        ASSERT_EQ("index1", config1->GetIndexName());
        ASSERT_TRUE(config1->GetFieldConfig());
        ASSERT_TRUE(config1->GetValueConfig());

        auto config2 =
            std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(schema->GetIndexConfig("kv", "raw_key"));
        ASSERT_TRUE(config2);
        ASSERT_EQ("raw_key", config2->GetIndexName());
        ASSERT_TRUE(config2->GetFieldConfig());
        auto fieldConfigs = config2->GetFieldConfigs();
        ASSERT_EQ(size_t(1), fieldConfigs.size());
        ASSERT_EQ("key", fieldConfigs[0]->GetFieldName());
        ASSERT_TRUE(config2->GetValueConfig());
    }
    {
        auto schema = LoadSchema("kv_schema.json");
        ASSERT_TRUE(schema != nullptr);
        ASSERT_EQ(indexlib::table::TABLE_TYPE_KV, schema->GetTableType());
        ASSERT_EQ(size_t(1), schema->GetIndexConfigs().size());

        auto kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(
            schema->GetIndexConfig("kv", "member_id_sim_member_id"));
        ASSERT_TRUE(kvConfig);
    }
}

TEST_F(KVTabletSchemaTest, TestTTL)
{
    {
        std::string schemaStr;
        {
            auto tabletSchema = LoadSchema("kv_schema_default_ttl.json");
            ASSERT_TRUE(tabletSchema);
            auto [status, enableTTL] = tabletSchema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(enableTTL);

            auto [status2, defaultTTL] = tabletSchema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
            ASSERT_TRUE(status2.IsOK());
            ASSERT_EQ(20, defaultTTL);
            ASSERT_TRUE(tabletSchema->Serialize(false, &schemaStr));
        }
        {
            auto tabletSchema = framework::TabletSchemaLoader::LoadSchema(schemaStr);
            ASSERT_TRUE(
                framework::TabletSchemaLoader::ResolveSchema(nullptr, GET_TEMP_DATA_PATH(), tabletSchema.get()).IsOK());
            ASSERT_TRUE(tabletSchema);
            auto [status, enableTTL] = tabletSchema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(enableTTL);

            auto [status2, defaultTTL] = tabletSchema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
            ASSERT_TRUE(status2.IsOK());
            ASSERT_EQ(20, defaultTTL);
        }
    }

    {
        std::string schemaStr;
        {
            auto tabletSchema = LoadSchema("kv_schema_enable_ttl.json");
            ASSERT_TRUE(tabletSchema);
            auto [status, enableTTL] = tabletSchema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(enableTTL);

            auto [status2, defaultTTL] = tabletSchema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
            ASSERT_TRUE(status2.IsOK());
            ASSERT_EQ(indexlib::DEFAULT_TIME_TO_LIVE, defaultTTL);
            ASSERT_TRUE(tabletSchema->Serialize(false, &schemaStr));
        }
        {
            auto tabletSchema = framework::TabletSchemaLoader::LoadSchema(schemaStr);
            ASSERT_TRUE(
                framework::TabletSchemaLoader::ResolveSchema(nullptr, GET_TEMP_DATA_PATH(), tabletSchema.get()).IsOK());
            ASSERT_TRUE(tabletSchema);
            auto [status, enableTTL] = tabletSchema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(enableTTL);

            auto [status2, defaultTTL] = tabletSchema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
            ASSERT_TRUE(status2.IsOK());
            ASSERT_EQ(indexlib::DEFAULT_TIME_TO_LIVE, defaultTTL);
        }
    }

    {
        std::string schemaStr;
        {
            auto tabletSchema = LoadSchema("kv_schema_both_1_ttl.json");
            ASSERT_TRUE(tabletSchema);
            auto [status, enableTTL] = tabletSchema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(enableTTL);

            auto [status2, defaultTTL] = tabletSchema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
            ASSERT_TRUE(status2.IsOK());
            ASSERT_EQ(30, defaultTTL);
            ASSERT_TRUE(tabletSchema->Serialize(false, &schemaStr));
        }
        {
            auto tabletSchema = framework::TabletSchemaLoader::LoadSchema(schemaStr);
            ASSERT_TRUE(
                framework::TabletSchemaLoader::ResolveSchema(nullptr, GET_TEMP_DATA_PATH(), tabletSchema.get()).IsOK());
            ASSERT_TRUE(tabletSchema);
            auto [status, enableTTL] = tabletSchema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(enableTTL);

            auto [status2, defaultTTL] = tabletSchema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
            ASSERT_TRUE(status2.IsOK());
            ASSERT_EQ(30, defaultTTL);
        }
    }

    {
        std::string schemaStr;
        {
            auto tabletSchema = LoadSchema("kv_schema_both_2_ttl.json");
            ASSERT_TRUE(tabletSchema);
            auto [status, enableTTL] = tabletSchema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
            ASSERT_FALSE(status.IsOK());
            auto [status2, defaultTTL] = tabletSchema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
            ASSERT_FALSE(status2.IsOK());
            ASSERT_TRUE(tabletSchema->Serialize(false, &schemaStr));
        }
        {
            auto tabletSchema = framework::TabletSchemaLoader::LoadSchema(schemaStr);
            ASSERT_TRUE(
                framework::TabletSchemaLoader::ResolveSchema(nullptr, GET_TEMP_DATA_PATH(), tabletSchema.get()).IsOK());
            ASSERT_TRUE(tabletSchema);
            auto [status, enableTTL] = tabletSchema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
            ASSERT_FALSE(status.IsOK());
            auto [status2, defaultTTL] = tabletSchema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
            ASSERT_FALSE(status2.IsOK());
        }
    }
    {
        std::string schemaStr;
        {
            auto tabletSchema = LoadSchema("kv_schema_both_3_ttl.json");
            ASSERT_TRUE(tabletSchema);
            auto [status, enableTTL] = tabletSchema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(enableTTL);

            auto [status2, defaultTTL] = tabletSchema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
            ASSERT_TRUE(status2.IsOK());
            ASSERT_EQ(indexlib::DEFAULT_TIME_TO_LIVE, defaultTTL);
            ASSERT_TRUE(tabletSchema->Serialize(false, &schemaStr));
        }
        {
            auto tabletSchema = framework::TabletSchemaLoader::LoadSchema(schemaStr);
            ASSERT_TRUE(
                framework::TabletSchemaLoader::ResolveSchema(nullptr, GET_TEMP_DATA_PATH(), tabletSchema.get()).IsOK());
            ASSERT_TRUE(tabletSchema);
            auto [status, enableTTL] = tabletSchema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(enableTTL);

            auto [status2, defaultTTL] = tabletSchema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
            ASSERT_TRUE(status2.IsOK());
            ASSERT_EQ(indexlib::DEFAULT_TIME_TO_LIVE, defaultTTL);
        }
    }
    {
        std::string schemaStr;
        {
            auto tabletSchema = LoadSchema("kv_schema_nottl.json");
            ASSERT_TRUE(tabletSchema);
            auto [status, enableTTL] = tabletSchema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
            ASSERT_FALSE(status.IsOK());
            auto [status2, defaultTTL] = tabletSchema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
            ASSERT_FALSE(status2.IsOK());
            ASSERT_TRUE(tabletSchema->Serialize(false, &schemaStr));
        }
        {
            auto tabletSchema = framework::TabletSchemaLoader::LoadSchema(schemaStr);
            ASSERT_TRUE(
                framework::TabletSchemaLoader::ResolveSchema(nullptr, GET_TEMP_DATA_PATH(), tabletSchema.get()).IsOK());
            ASSERT_TRUE(tabletSchema);
            auto [status, enableTTL] = tabletSchema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
            ASSERT_FALSE(status.IsOK());
            auto [status2, defaultTTL] = tabletSchema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
            ASSERT_FALSE(status2.IsOK());
        }
    }
}

} // namespace indexlibv2::table
