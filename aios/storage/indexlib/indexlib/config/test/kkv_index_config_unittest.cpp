#include "indexlib/config/test/kkv_index_config_unittest.h"

#include "autil/EnvUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, KKVIndexConfigTest);

KKVIndexConfigTest::KKVIndexConfigTest() {}

KKVIndexConfigTest::~KKVIndexConfigTest() {}

void KKVIndexConfigTest::CaseSetUp() {}

void KKVIndexConfigTest::CaseTearDown() {}

void KKVIndexConfigTest::TestSimpleProcess()
{
    string jsonString = R"(
    {
        "table_name": "kkv_table",
        "table_type": "kkv",
        "fields": [
        { "field_name": "nick", "field_type": "STRING" },
        { "field_name": "nid", "field_type": "INT64" }
        ],
        "indexs": [
        {
            "index_name": "nick_nid", "index_type": "PRIMARY_KEY",
            "index_fields": [
                {"field_name":"nick", "key_type":"prefix"},
                {"field_name":"nid", "key_type":"suffix", "count_limits":5000, "skip_list_threshold":200 }
            ],
            "index_preference": {
                "type" : "PERF",
                "parameters" : {			    
                    "hash_dict" : {},
                    "suffix_key" : { "encode" : false },
                    "value" : { "encode" : true, "file_compressor" : "lz4" }
                }
            }
        }
        ],
        "attributes": [ "nick", "nid" ]
    }
    )";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));
    ASSERT_EQ("kkv_table", schema->GetSchemaName());
    ASSERT_EQ(tt_kkv, schema->GetTableType());
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    ASSERT_EQ(it_kkv, indexSchema->GetPrimaryKeyIndexType());
    SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    KKVIndexConfigPtr kkvIndexConfig = std::dynamic_pointer_cast<KKVIndexConfig>(indexConfig);
    ASSERT_TRUE(kkvIndexConfig);
    ASSERT_EQ(0, kkvIndexConfig->GetIndexFormatVersionId());
    ASSERT_TRUE(kkvIndexConfig->SetIndexFormatVersionId(1).IsOK());
    ASSERT_EQ(0, kkvIndexConfig->GetIndexFormatVersionId());

    ASSERT_EQ(hft_murmur, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());

    ASSERT_EQ(hft_murmur, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());
    ASSERT_EQ((uint32_t)200, kkvIndexConfig->GetSuffixKeySkipListThreshold());
}

void KKVIndexConfigTest::TestGetHashFunctionType()
{
    // no use_number_hash
    string jsonString = R"(
    {
        "table_name": "kkv_table",
        "table_type": "kkv",
        "fields": [
        { "field_name": "nick", "field_type": "UINT32" },
        { "field_name": "nid", "field_type": "INT8" }
        ],
        "indexs": [
        {
            "index_name": "nick_nid", "index_type": "PRIMARY_KEY",
            "index_fields": [
                {"field_name":"nick", "key_type":"prefix"},
                {"field_name":"nid", "key_type":"suffix", "count_limits":5000 }
            ],
            "index_preference": {
                "type" : "PERF",
                "parameters" : {			    
                    "hash_dict" : {
                         "enable_compact_hash_key" : true,
                         "enable_shorten_offset" : true
                    },
                    "suffix_key" : { "encode" : false },
                    "value" : { "encode" : true, "file_compressor" : "lz4" }
                }
            }
        }
        ],
        "attributes": [ "nick", "nid" ]
    }
    )";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));

    // inc build or online: for compatible
    KKVIndexConfigPtr kkvIndexConfig =
        std::dynamic_pointer_cast<KKVIndexConfig>(schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(kkvIndexConfig);
    ASSERT_EQ(hft_uint64, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());

    // full build
    ASSERT_EQ(hft_uint64, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());
    string newJsonString = ToString(ToJson(*schema));
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema(""));
    FromJson(*newSchema, ParseJson(newJsonString));
    KVIndexConfigPtr newKvIndexConfig =
        std::dynamic_pointer_cast<KVIndexConfig>(newSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(newKvIndexConfig);
    ASSERT_EQ(hft_uint64, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());

    ASSERT_FALSE(kkvIndexConfig->GetIndexPreference().GetHashDictParam().HasEnableCompactHashKey());
    ASSERT_FALSE(kkvIndexConfig->GetIndexPreference().GetHashDictParam().HasEnableShortenOffset());

    // inc build and online: use_number_hash = true
    jsonString = R"(
    {
        "table_name": "kkv_table",
        "table_type": "kkv",
        "fields": [
        { "field_name": "nick", "field_type": "UINT32" },
        { "field_name": "nid", "field_type": "INT8" }
        ],
        "indexs": [
        {
            "index_name": "nick_nid", "index_type": "PRIMARY_KEY",
            "index_fields": [
                {"field_name":"nick", "key_type":"prefix"},
                {"field_name":"nid", "key_type":"suffix", "count_limits":5000 }
            ],
            "index_preference": {
                "type" : "PERF",
                "parameters" : {			    
                    "hash_dict" : {},
                    "suffix_key" : { "encode" : false },
                    "value" : { "encode" : true, "file_compressor" : "lz4" }
                }
            }, 
            "use_number_hash" : true
        }
        ],
        "attributes": [ "nick", "nid" ]
    }
    )";
    schema.reset(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));
    kkvIndexConfig = std::dynamic_pointer_cast<KKVIndexConfig>(schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(kkvIndexConfig);
    ASSERT_EQ(hft_uint64, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());

    // inc build and online: use_number_hash = false
    jsonString = R"(
    {
        "table_name": "kkv_table",
        "table_type": "kkv",
        "fields": [
        { "field_name": "nick", "field_type": "UINT32" },
        { "field_name": "nid", "field_type": "INT8" }
        ],
        "indexs": [
        {
            "index_name": "nick_nid", "index_type": "PRIMARY_KEY",
            "index_fields": [
                {"field_name":"nick", "key_type":"prefix"},
                {"field_name":"nid", "key_type":"suffix", "count_limits":5000 }
            ],
            "index_preference": {
                "type" : "PERF",
                "parameters" : {			    
                    "hash_dict" : {},
                    "suffix_key" : { "encode" : false },
                    "value" : { "encode" : true, "file_compressor" : "lz4" }
                }
            }, 
            "use_number_hash" : false
        }
        ],
        "attributes": [ "nick", "nid" ]
    }
    )";
    schema.reset(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));
    kkvIndexConfig = std::dynamic_pointer_cast<KKVIndexConfig>(schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(kkvIndexConfig);
    ASSERT_EQ(hft_murmur, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());
}

void KKVIndexConfigTest::TestPlainFormat()
{
    string jsonString = R"(
    {
        "table_name": "kkv_table",
        "table_type": "kkv",
        "fields": [
        { "field_name": "nick", "field_type": "UINT32" },
        { "field_name": "nid", "field_type": "INT8" }
        ],
        "indexs": [
        {
            "index_name": "nick_nid", "index_type": "PRIMARY_KEY",
            "index_fields": [
                {"field_name":"nick", "key_type":"prefix"},
                {"field_name":"nid", "key_type":"suffix", "count_limits":5000 }
            ],
            "index_preference": {
                "type" : "PERF",
                "parameters" : {			    
                    "hash_dict" : {
                         "enable_compact_hash_key" : true,
                         "enable_shorten_offset" : true
                    },
                    "suffix_key" : { "encode" : false },
                    "value" : { "encode" : true, "file_compressor" : "lz4", "plain_format" : true }
                }
            }
        }
        ],
        "attributes": [ "nick", "nid" ]
    }
    )";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));

    string newJsonString = ToString(ToJson(*schema));
    cout << newJsonString << endl;
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema(""));
    FromJson(*newSchema, ParseJson(newJsonString));

    KVIndexConfigPtr kkvIndexConfig =
        std::dynamic_pointer_cast<KVIndexConfig>(newSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(kkvIndexConfig);

    auto valueConfig = kkvIndexConfig->GetValueConfig();
    auto packAttrConfig = valueConfig->CreatePackAttributeConfig();
    ASSERT_TRUE(packAttrConfig->HasEnablePlainFormat());
}

void KKVIndexConfigTest::TestCheckValueFormat()
{
    string jsonString = R"(
    {
        "table_name": "kkv_table",
        "table_type": "kkv",
        "fields": [
        { "field_name": "nick", "field_type": "UINT32" },
        { "field_name": "nid", "field_type": "INT8" }
        ],
        "indexs": [
        {
            "index_name": "nick_nid", "index_type": "PRIMARY_KEY",
            "index_fields": [
                {"field_name":"nick", "key_type":"prefix"},
                {"field_name":"nid", "key_type":"suffix", "count_limits":5000 }
            ],
            "index_preference": {
                "type" : "PERF",
                "parameters" : {			    
                    "hash_dict" : {
                         "enable_compact_hash_key" : true,
                         "enable_shorten_offset" : true
                    },
                    "suffix_key" : { "encode" : false },
                    "value" : { "encode" : true, "file_compressor" : "lz4", "value_impact" : true, "plain_format" : true }
                }
            }
        }
        ],
        "attributes": [ "nick", "nid" ]
    }
    )";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    ASSERT_ANY_THROW(FromJson(*schema, ParseJson(jsonString)));
}

void KKVIndexConfigTest::TestValueImpact()
{
    // no use_number_hash
    string jsonString = R"(
    {
        "table_name": "kkv_table",
        "table_type": "kkv",
        "fields": [
        { "field_name": "nick", "field_type": "UINT32" },
        { "field_name": "nid", "field_type": "INT8" }
        ],
        "indexs": [
        {
            "index_name": "nick_nid", "index_type": "PRIMARY_KEY",
            "index_fields": [
                {"field_name":"nick", "key_type":"prefix"},
                {"field_name":"nid", "key_type":"suffix", "count_limits":5000 }
            ],
            "index_preference": {
                "type" : "PERF",
                "parameters" : {			    
                    "hash_dict" : {
                         "enable_compact_hash_key" : true,
                         "enable_shorten_offset" : true
                    },
                    "suffix_key" : { "encode" : false },
                    "value" : { "encode" : true, "file_compressor" : "lz4", "value_impact" : true }
                }
            }
        }
        ],
        "attributes": [ "nick", "nid" ]
    }
    )";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));

    string newJsonString = ToString(ToJson(*schema));
    cout << newJsonString << endl;
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema(""));
    FromJson(*newSchema, ParseJson(newJsonString));

    KVIndexConfigPtr kkvIndexConfig =
        std::dynamic_pointer_cast<KVIndexConfig>(newSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(kkvIndexConfig);

    auto valueConfig = kkvIndexConfig->GetValueConfig();
    auto packAttrConfig = valueConfig->CreatePackAttributeConfig();
    ASSERT_TRUE(packAttrConfig->HasEnableImpact());
}

void KKVIndexConfigTest::TestAutoValueImpact()
{
    autil::EnvUtil::setEnv("INDEXLIB_OPT_KV_STORE", "true", true);
    // no use_number_hash
    string jsonString = R"(
    {
        "table_name": "kkv_table",
        "table_type": "kkv",
        "fields": [
        { "field_name": "nick", "field_type": "UINT32" },
        { "field_name": "nid", "field_type": "INT8" }
        ],
        "indexs": [
        {
            "index_name": "nick_nid", "index_type": "PRIMARY_KEY",
            "index_fields": [
                {"field_name":"nick", "key_type":"prefix"},
                {"field_name":"nid", "key_type":"suffix", "count_limits":5000 }
            ],
            "index_preference": {
                "type" : "PERF",
                "parameters" : {			    
                    "hash_dict" : {
                         "enable_compact_hash_key" : true,
                         "enable_shorten_offset" : true
                    },
                    "suffix_key" : { "encode" : false },
                    "value" : { "encode" : true, "file_compressor" : "lz4"}
                }
            }
        }
        ],
        "attributes": [ "nick", "nid" ]
    }
    )";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));

    string newJsonString = ToString(ToJson(*schema));
    cout << newJsonString << endl;
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema(""));
    FromJson(*newSchema, ParseJson(newJsonString));
    autil::EnvUtil::unsetEnv("INDEXLIB_OPT_KV_STORE");

    KKVIndexConfigPtr kkvIndexConfig =
        std::dynamic_pointer_cast<KKVIndexConfig>(newSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(kkvIndexConfig);

    auto valueConfig = kkvIndexConfig->GetValueConfig();
    auto packAttrConfig = valueConfig->CreatePackAttributeConfig();
    ASSERT_TRUE(packAttrConfig->HasEnableImpact());

    auto valueParam = kkvIndexConfig->GetIndexPreference().GetValueParam();
    auto compressParams = valueParam.GetFileCompressParameter();
    auto iter = compressParams.find("encode_address_mapper");
    ASSERT_TRUE(iter != compressParams.end());
    ASSERT_EQ(string("true"), iter->second);
}

}} // namespace indexlib::config
