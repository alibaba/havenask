#include "indexlib/table/kv_table/KVSchemaResolver.h"

#include "indexlib/config/MutableJson.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "unittest/unittest.h"

namespace indexlibv2::table {

class KVSchemaResolverTest : public TESTBASE
{
public:
    KVSchemaResolverTest() = default;
    ~KVSchemaResolverTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void KVSchemaResolverTest::setUp() {}

void KVSchemaResolverTest::tearDown() {}

TEST_F(KVSchemaResolverTest, testTTLForLegacy)
{
    auto resolver = std::make_shared<KVSchemaResolver>();
    auto schema = framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "legacy_ttl_schema.json");

    auto [status, enableTTL] = schema->GetRuntimeSettings().GetValue<bool>("enable_ttl");
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(enableTTL);

    auto [status1, ttlFieldName] = schema->GetRuntimeSettings().GetValue<std::string>("ttl_field_name");
    ASSERT_TRUE(status1.IsOK());
    ASSERT_EQ("a", ttlFieldName);

    auto [status2, defaultTTL] = schema->GetRuntimeSettings().GetValue<int64_t>("default_ttl");
    ASSERT_TRUE(status2.IsOK());
    ASSERT_EQ(8796093022207, defaultTTL);

    auto [status3, ttlFromDoc] = schema->GetRuntimeSettings().GetValue<bool>("ttl_from_doc");
    ASSERT_TRUE(status3.IsOK());
    ASSERT_TRUE(ttlFromDoc);

    auto s = resolver->Resolve("", schema.get());
    ASSERT_TRUE(s.IsOK()) << s.ToString();
    ASSERT_EQ(1, schema->GetIndexConfigs().size());
    auto indexConfig = std::dynamic_pointer_cast<config::KVIndexConfig>(schema->GetIndexConfigs()[0]);
    ASSERT_TRUE(indexConfig);
    auto settings = indexConfig->GetTTLSettings();
    ASSERT_TRUE(settings);
    ASSERT_TRUE(settings->enabled);
    ASSERT_TRUE(settings->ttlFromDoc);
    ASSERT_EQ("a", settings->ttlField);

    ASSERT_FALSE(indexConfig->DenyEmptyPrimaryKey());
    ASSERT_FALSE(indexConfig->IgnoreEmptyPrimaryKey());
}

TEST_F(KVSchemaResolverTest, testStorePKForLegacy)
{
    auto resolver = std::make_shared<KVSchemaResolver>();
    auto schema = framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "legacy_store_pk_schema.json");

    auto [status, needStorePKValue] = schema->GetRuntimeSettings().GetValue<bool>("need_store_pk_value");
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(needStorePKValue);
}
} // namespace indexlibv2::table
