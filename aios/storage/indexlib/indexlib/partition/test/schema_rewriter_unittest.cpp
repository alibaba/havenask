#include "indexlib/partition/test/schema_rewriter_unittest.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_define.h"

using namespace std;

using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SchemaRewriterTest);

SchemaRewriterTest::SchemaRewriterTest() {}

SchemaRewriterTest::~SchemaRewriterTest() {}

void SchemaRewriterTest::CaseSetUp()
{
    string field = "pk:string:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint64:true;"
                   "updatable_multi_long:uint64:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "string1;";

    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);
}

void SchemaRewriterTest::CaseTearDown() {}

void SchemaRewriterTest::CheckDefragSlicePercent(float defragPercent, const IndexPartitionSchemaPtr& schema)
{
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    AttributeSchema::Iterator iter = attrSchema->Begin();
    for (; iter != attrSchema->End(); iter++) {
        const AttributeConfigPtr& attrConfig = *iter;
        ASSERT_EQ(defragPercent, attrConfig->GetDefragSlicePercent());
    }
}

void SchemaRewriterTest::TestForRewriteForPrimaryKey()
{
    IndexPartitionOptions options;
    {
        IndexPartitionSchemaPtr schema(mSchema->Clone());
        // test offline speedup pk
        options.SetIsOnline(false);
        options.GetBuildConfig().speedUpPrimaryKeyReader = true;
        SchemaRewriter::RewriteForPrimaryKey(schema, options);
        CheckPrimaryKeySpeedUp(true, schema);
    }

    {
        IndexPartitionSchemaPtr schema(mSchema->Clone());
        // test online not speedup pk
        options.SetIsOnline(true);
        options.GetBuildConfig().speedUpPrimaryKeyReader = false;
        SchemaRewriter::RewriteForPrimaryKey(schema, options);
        CheckPrimaryKeySpeedUp(false, schema);
    }

    {
        IndexPartitionSchemaPtr schema(mSchema->Clone());
        // test online speedup pk
        options.SetIsOnline(true);
        options.GetBuildConfig().speedUpPrimaryKeyReader = true;
        SchemaRewriter::RewriteForPrimaryKey(schema, options);
        CheckPrimaryKeySpeedUp(true, schema);
    }

    {
        // test no pk not core
        string index = "index1:string:string1;pack1:pack:text1;";
        string field = "string1:string;text1:text;";
        IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
        options.SetIsOnline(true);
        options.GetBuildConfig().speedUpPrimaryKeyReader = true;
        ASSERT_NO_THROW(SchemaRewriter::RewriteForPrimaryKey(schema, options));
    }
}

void SchemaRewriterTest::CheckPrimaryKeySpeedUp(bool speedUp, const IndexPartitionSchemaPtr& schema)
{
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    const IndexConfigPtr& indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    const PrimaryKeyIndexConfigPtr& pkIndexConfig = DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, indexConfig);
    PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode =
        pkIndexConfig->GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode();
    if (speedUp) {
        ASSERT_EQ(PrimaryKeyLoadStrategyParam::HASH_TABLE, loadMode);
        return;
    }
    ASSERT_EQ(PrimaryKeyLoadStrategyParam::SORTED_VECTOR, loadMode);
}

void SchemaRewriterTest::TestForRewriteForKV()
{
    {
        // test case for kkv
        IndexPartitionOptions options;
        options.SetIsOnline(true);
        options.GetOnlineConfig().kvOnlineConfig.countLimits = 10;
        options.GetOnlineConfig().kvOnlineConfig.buildProtectThreshold = 50;

        string fields = "single_int8:int8;single_int16:int16;pkey:string;skey:int32";
        mSchema = SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "single_int8;single_int16");
        const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
        config::KKVIndexConfigPtr kkvConfig =
            DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        ASSERT_FALSE(kkvConfig->NeedSuffixKeyTruncate());
        const_cast<KKVIndexFieldInfo&>(kkvConfig->GetSuffixFieldInfo()).countLimits = 200;
        ASSERT_TRUE(kkvConfig->NeedSuffixKeyTruncate());
        ASSERT_EQ(200, kkvConfig->GetSuffixKeyTruncateLimits());

        const_cast<KKVIndexFieldInfo&>(kkvConfig->GetSuffixFieldInfo()).protectionThreshold = 400;
        ASSERT_EQ(400, kkvConfig->GetSuffixKeyProtectionThreshold());

        IndexFormatVersion indexVersion(INDEX_FORMAT_VERSION);
        SchemaRewriter::RewriteForKV(mSchema, options, indexVersion);
        ASSERT_TRUE(kkvConfig->NeedSuffixKeyTruncate());
        ASSERT_EQ(10, kkvConfig->GetSuffixKeyTruncateLimits());
        ASSERT_EQ(50, kkvConfig->GetSuffixKeyProtectionThreshold());
    }
    {
        // test case for kv table, with index_format_version > 2.1.0
        IndexPartitionOptions options;
        options.SetIsOnline(true);

        IndexFormatVersion indexVersion(INDEX_FORMAT_VERSION);
        string fields = "single_int8:int8;single_int16:int16;pkey:string";
        mSchema = SchemaMaker::MakeKVSchema(fields, "pkey", "single_int8;single_int16");
        SchemaRewriter::RewriteForKV(mSchema, options, indexVersion);
        const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
        const SingleFieldIndexConfigPtr& pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, pkConfig);
        ValueConfigPtr valueConfig = kvConfig->GetValueConfig();
        ASSERT_TRUE(valueConfig->IsCompactFormatEnabled());
    }
    {
        // test case for kv table, with index_format_version <= 2.1.0
        IndexPartitionOptions options;
        options.SetIsOnline(true);

        IndexFormatVersion indexVersion("2.0.0");
        string fields = "single_int8:int8;single_int16:int16;pkey:string";
        mSchema = SchemaMaker::MakeKVSchema(fields, "pkey", "single_int8;single_int16");
        SchemaRewriter::RewriteForKV(mSchema, options, indexVersion);
        const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
        const SingleFieldIndexConfigPtr& pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, pkConfig);
        ValueConfigPtr valueConfig = kvConfig->GetValueConfig();
        ASSERT_FALSE(valueConfig->IsCompactFormatEnabled());
    }
}

void SchemaRewriterTest::TestRewriteForMultiRegionKV()
{
    IndexFormatVersion indexVersion(INDEX_FORMAT_VERSION);
    auto schema = SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_index_multi_region.json");
    IndexPartitionOptions options;
    SchemaRewriter::RewriteForKV(schema, options, indexVersion);

    {
        // region1
        RegionSchemaPtr regionSchema = schema->GetRegionSchema("region1");
        ASSERT_TRUE(regionSchema);

        IndexSchemaPtr indexSchema = regionSchema->GetIndexSchema();
        ASSERT_EQ(it_kv, indexSchema->GetPrimaryKeyIndexType());
        SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
        ASSERT_TRUE(kvIndexConfig);
        ASSERT_FALSE(kvIndexConfig->TTLEnabled());
        ASSERT_TRUE(kvIndexConfig->GetValueConfig()->IsCompactFormatEnabled());
    }
    {
        // region2
        RegionSchemaPtr regionSchema = schema->GetRegionSchema("region2");
        ASSERT_TRUE(regionSchema);

        IndexSchemaPtr indexSchema = regionSchema->GetIndexSchema();
        ASSERT_EQ(it_kv, indexSchema->GetPrimaryKeyIndexType());
        SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
        ASSERT_TRUE(kvIndexConfig);
        ASSERT_TRUE(kvIndexConfig->TTLEnabled());
        ASSERT_EQ((int64_t)3600, kvIndexConfig->GetTTL());
        ASSERT_TRUE(kvIndexConfig->GetValueConfig()->IsCompactFormatEnabled());
    }

    KVIndexConfigPtr dataKVConfig = CreateKVIndexConfigForMultiRegionData(schema);
    ASSERT_FALSE(dataKVConfig->GetValueConfig()->IsCompactFormatEnabled());

    {
        // region1
        RegionSchemaPtr regionSchema = schema->GetRegionSchema("region1");
        ASSERT_TRUE(regionSchema);

        IndexSchemaPtr indexSchema = regionSchema->GetIndexSchema();
        ASSERT_EQ(it_kv, indexSchema->GetPrimaryKeyIndexType());
        SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
        ASSERT_TRUE(kvIndexConfig);
        ASSERT_FALSE(kvIndexConfig->TTLEnabled());
        ASSERT_TRUE(kvIndexConfig->GetValueConfig()->IsCompactFormatEnabled());
    }

    const KVIndexPreference& indexPreference = dataKVConfig->GetIndexPreference();
    ASSERT_EQ(ipt_store, indexPreference.GetType());
    const KVIndexPreference::HashDictParam& hashDictParam = indexPreference.GetHashDictParam();
    ASSERT_EQ(string("cuckoo"), hashDictParam.GetHashType());
    ASSERT_EQ((int)80, hashDictParam.GetOccupancyPct());
    ASSERT_FALSE(hashDictParam.HasEnableCompactHashKey());
    ASSERT_FALSE(hashDictParam.HasEnableShortenOffset());

    const KVIndexPreference::ValueParam& valueParam = indexPreference.GetValueParam();
    ASSERT_TRUE(valueParam.EnableFileCompress());
    ASSERT_EQ(string("zstd"), valueParam.GetFileCompressType());
}

void SchemaRewriterTest::TestRewriteForMultiRegionKKV()
{
    InnerTestRewriteForMultiRegionKKV(true);
    InnerTestRewriteForMultiRegionKKV(false);
}

void SchemaRewriterTest::InnerTestRewriteForMultiRegionKKV(bool isOnline)
{
    IndexFormatVersion indexVersion(INDEX_FORMAT_VERSION);
    auto schema = SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kkv_index_multi_region.json");
    IndexPartitionOptions options;
    options.SetIsOnline(isOnline);
    options.GetOnlineConfig().kvOnlineConfig.countLimits = 10;
    options.GetOnlineConfig().kvOnlineConfig.buildProtectThreshold = 50;
    SchemaRewriter::RewriteForKV(schema, options, indexVersion);

    {
        // region1
        RegionSchemaPtr regionSchema = schema->GetRegionSchema("in_region");
        ASSERT_TRUE(regionSchema);

        IndexSchemaPtr indexSchema = regionSchema->GetIndexSchema();
        ASSERT_EQ(it_kkv, indexSchema->GetPrimaryKeyIndexType());
        SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexConfig);
        ASSERT_TRUE(kkvIndexConfig);
        ASSERT_TRUE(kkvIndexConfig->TTLEnabled());
        ASSERT_EQ((uint32_t)(isOnline ? 50 : 20000), kkvIndexConfig->GetSuffixKeyProtectionThreshold());
        ASSERT_EQ((uint32_t)(isOnline ? 10 : 2000), kkvIndexConfig->GetSuffixKeyTruncateLimits());
    }

    {
        // region2
        RegionSchemaPtr regionSchema = schema->GetRegionSchema("out_region");
        ASSERT_TRUE(regionSchema);

        IndexSchemaPtr indexSchema = regionSchema->GetIndexSchema();
        ASSERT_EQ(it_kkv, indexSchema->GetPrimaryKeyIndexType());
        SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexConfig);
        ASSERT_TRUE(kkvIndexConfig);
        ASSERT_TRUE(kkvIndexConfig->TTLEnabled());
        ASSERT_EQ((uint32_t)(isOnline ? 50 : 20000), kkvIndexConfig->GetSuffixKeyProtectionThreshold());
        ASSERT_EQ((uint32_t)(isOnline ? 10 : 3000), kkvIndexConfig->GetSuffixKeyTruncateLimits());
    }

    KKVIndexConfigPtr dataKKVConfig = CreateKKVIndexConfigForMultiRegionData(schema);
    ASSERT_TRUE(dataKKVConfig->GetSuffixFieldConfig()->GetFieldType() == ft_uint64);
    ASSERT_EQ((uint32_t)(isOnline ? 50 : 20000), dataKKVConfig->GetSuffixKeyProtectionThreshold());
    ASSERT_EQ((uint32_t)(isOnline ? 10 : 2000), dataKKVConfig->GetSuffixKeyTruncateLimits());

    const KKVIndexPreference& indexPreference = dataKKVConfig->GetIndexPreference();
    ASSERT_EQ(ipt_store, indexPreference.GetType());
    const KKVIndexPreference::HashDictParam& hashDictParam = indexPreference.GetHashDictParam();
    ASSERT_EQ(string("separate_chain"), hashDictParam.GetHashType());

    const KKVIndexPreference::SuffixKeyParam& skeyParam = indexPreference.GetSkeyParam();
    ASSERT_TRUE(skeyParam.EnableFileCompress());
    ASSERT_EQ(string("zstd"), skeyParam.GetFileCompressType());

    const KKVIndexPreference::ValueParam& valueParam = indexPreference.GetValueParam();
    ASSERT_TRUE(valueParam.EnableFileCompress());
    ASSERT_EQ(string("zstd"), valueParam.GetFileCompressType());
}
}} // namespace indexlib::partition
