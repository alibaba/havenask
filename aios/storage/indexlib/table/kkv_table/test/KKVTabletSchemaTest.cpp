#include "fslib/fs/FileSystem.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/table/BuiltinDefine.h"
#include "unittest/unittest.h"

namespace indexlibv2::table {

class KKVTabletSchemaTest : public TESTBASE
{
private:
    std::shared_ptr<config::ITabletSchema> LoadSchema(const std::string& fileName) const
    {
        std::shared_ptr<config::TabletSchema> tabletSchema =
            framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), fileName);
        if (!tabletSchema) {
            return nullptr;
        }
        return tabletSchema;
    }
};

TEST_F(KKVTabletSchemaTest, TestLoadV1Schema)
{
    // we can read legacy schema use tablet config loader
    {
        auto schema = LoadSchema("kkv_schema_v1.json");
        ASSERT_TRUE(schema != nullptr);
        ASSERT_EQ("kkv", schema->GetTableType());

        auto configs = schema->GetIndexConfigs("orc");
        ASSERT_TRUE(configs.empty());

        configs = schema->GetIndexConfigs("kkv");
        ASSERT_EQ(configs.size(), 1);

        auto indexConfig = schema->GetIndexConfig("kkv", "pkey");
        ASSERT_TRUE(indexConfig);
        auto kkvIndexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
        ASSERT_TRUE(kkvIndexConfig);
        ASSERT_EQ(kkvIndexConfig->GetValueConfig()->GetAttributeCount(), 1);
        ASSERT_TRUE(kkvIndexConfig->OptimizedStoreSKey());
        ASSERT_TRUE(kkvIndexConfig->NeedSuffixKeyTruncate());
        ASSERT_EQ((uint32_t)5000, kkvIndexConfig->GetSuffixKeyTruncateLimits());

        const auto& truncSortParam = kkvIndexConfig->GetSuffixKeyTruncateParams();
        ASSERT_EQ((size_t)1, truncSortParam.size());
        ASSERT_EQ("$TIME_STAMP", truncSortParam[0].GetSortField());
        ASSERT_TRUE(kkvIndexConfig->OptimizedStoreSKey());

        auto [status, valueConfig] = kkvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(valueConfig);
        std::vector<std::string> attrNames;
        valueConfig->GetSubAttributeNames(attrNames);
        std::vector<std::string> expected = {"value"};
        ASSERT_EQ(expected, attrNames);

        const auto& indexPreference = kkvIndexConfig->GetIndexPreference();
        ASSERT_EQ(indexlib::ipt_perf, indexPreference.GetType());
        const auto& skeyParam = indexPreference.GetSkeyParam();
        const auto& valueParam = indexPreference.GetValueParam();
        ASSERT_FALSE(skeyParam.IsEncode());
        ASSERT_TRUE(valueParam.IsEncode());
        ASSERT_EQ("", skeyParam.GetFileCompressType());
        ASSERT_EQ("lz4", valueParam.GetFileCompressType());
    }
}

TEST_F(KKVTabletSchemaTest, TestException)
{
    // pkey set limit_count
    ASSERT_FALSE(LoadSchema("kkv_schema_exception1.json"));

    // skey trunc_sort_param set no_exist field
    ASSERT_FALSE(LoadSchema("kkv_schema_exception2.json"));

    // skey trunc_sort_param use multi value field
    ASSERT_FALSE(LoadSchema("kkv_schema_exception3.json"));

    // unsupported compress type
    ASSERT_FALSE(LoadSchema("kkv_schema_exception4.json"));

    // key use multi value field
    ASSERT_FALSE(LoadSchema("kkv_schema_exception5.json"));

    // skey use multi value field
    ASSERT_FALSE(LoadSchema("kkv_schema_exception6.json"));

    // unsupported null field
    ASSERT_FALSE(LoadSchema("kkv_schema_with_null_field.json"));

    // 新架构不支持多region
    // ASSERT_FALSE(LoadSchema("kkv_schema_multi_region.json"));

    // test tablet schema
    ASSERT_FALSE(LoadSchema("kkv_schema_exception1_tablet.json"));
    ASSERT_FALSE(LoadSchema("kkv_schema_exception2_tablet.json"));
    ASSERT_FALSE(LoadSchema("kkv_schema_exception3_tablet.json"));
    ASSERT_FALSE(LoadSchema("kkv_schema_exception4_tablet.json"));
    ASSERT_FALSE(LoadSchema("kkv_schema_exception5_tablet.json"));
    ASSERT_FALSE(LoadSchema("kkv_schema_exception6_tablet.json"));
    ASSERT_FALSE(LoadSchema("kkv_schema_with_null_field_tablet.json"));
}

TEST_F(KKVTabletSchemaTest, TestLoadV1SchemaAndCheckIndexConfig)
{
    // we can read legacy schema use tablet config loader

    auto schema = LoadSchema("kkv_schema_v1.json");
    ASSERT_TRUE(schema != nullptr);

    auto indexConfig = schema->GetIndexConfig("kkv", "pkey");
    ASSERT_TRUE(indexConfig);
    auto kkvIndexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
    ASSERT_TRUE(kkvIndexConfig);

    kkvIndexConfig->Check();

    // make sure set FieldId, used by parser
    // may not need in future
    auto valueConfig = kkvIndexConfig->GetValueConfig();
    auto attrCount = valueConfig->GetAttributeCount();
    for (size_t i = 0; i < attrCount; ++i) {
        const auto& attrConfig = valueConfig->GetAttributeConfig(i);
        ASSERT_TRUE(attrConfig);
        ASSERT_GE(attrConfig->GetFieldId(), 0);
    }
}

TEST_F(KKVTabletSchemaTest, TestLoadchemaAndCheckIndexConfig)
{
    auto schema = LoadSchema("kkv_schema.json");
    ASSERT_TRUE(schema != nullptr);

    auto indexConfig = schema->GetIndexConfig("kkv", "pkey");
    ASSERT_TRUE(indexConfig);
    auto kkvIndexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
    ASSERT_TRUE(kkvIndexConfig);

    kkvIndexConfig->Check();

    // make sure set FieldId, used by parser
    // may not need in future
    auto valueConfig = kkvIndexConfig->GetValueConfig();
    auto attrCount = valueConfig->GetAttributeCount();
    for (size_t i = 0; i < attrCount; ++i) {
        const auto& attrConfig = valueConfig->GetAttributeConfig(i);
        ASSERT_TRUE(attrConfig);
        ASSERT_GE(attrConfig->GetFieldId(), 0);
    }
}

TEST_F(KKVTabletSchemaTest, TestOptimizeSKeyStore)
{
    {
        auto schema = LoadSchema("kkv_schema.json");
        ASSERT_TRUE(schema != nullptr);

        auto indexConfig = schema->GetIndexConfig("kkv", "pkey");
        ASSERT_TRUE(indexConfig);
        auto kkvIndexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
        ASSERT_TRUE(kkvIndexConfig);
        kkvIndexConfig->Check();
        ASSERT_EQ(kkvIndexConfig->GetValueConfig()->GetAttributeCount(), 1);
        ASSERT_EQ(kkvIndexConfig->GetValueConfig()->GetAttributeConfig(0)->GetAttrName(), "value");
        ASSERT_TRUE(kkvIndexConfig->OptimizedStoreSKey());
    }

    {
        auto schema = LoadSchema("kkv_schema_disable_optimize_store.json");
        ASSERT_TRUE(schema != nullptr);

        auto indexConfig = schema->GetIndexConfig("kkv", "pkey");
        ASSERT_TRUE(indexConfig);
        auto kkvIndexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
        ASSERT_TRUE(kkvIndexConfig);
        kkvIndexConfig->Check();
        ASSERT_EQ(kkvIndexConfig->GetValueConfig()->GetAttributeCount(), 2);
        ASSERT_EQ(kkvIndexConfig->GetValueConfig()->GetAttributeConfig(0)->GetAttrName(), "skey");
        ASSERT_EQ(kkvIndexConfig->GetValueConfig()->GetAttributeConfig(1)->GetAttrName(), "value");
        ASSERT_FALSE(kkvIndexConfig->OptimizedStoreSKey());
    }

    {
        auto schema = LoadSchema("kkv_schema_single_value.json");
        ASSERT_TRUE(schema != nullptr);

        auto indexConfig = schema->GetIndexConfig("kkv", "pkey");
        ASSERT_TRUE(indexConfig);
        auto kkvIndexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
        ASSERT_TRUE(kkvIndexConfig);
        kkvIndexConfig->Check();
        ASSERT_EQ(kkvIndexConfig->GetValueConfig()->GetAttributeCount(), 1);
        ASSERT_EQ(kkvIndexConfig->GetValueConfig()->GetAttributeConfig(0)->GetAttrName(), "floatmv");
        ASSERT_TRUE(kkvIndexConfig->OptimizedStoreSKey());
    }
}

} // namespace indexlibv2::table
