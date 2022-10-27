#include <autil/legacy/any.h>
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include "indexlib/config/test/kkv_index_config_unittest.h"

using namespace std;
using namespace std::tr1;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, KKVIndexConfigTest);

KKVIndexConfigTest::KKVIndexConfigTest()
{
}

KKVIndexConfigTest::~KKVIndexConfigTest()
{
}

void KKVIndexConfigTest::CaseSetUp()
{
}

void KKVIndexConfigTest::CaseTearDown()
{
}

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
    KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexConfig);
    ASSERT_TRUE(kkvIndexConfig);
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
    KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig,
            schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(kkvIndexConfig);
    ASSERT_EQ(hft_uint64, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());

    // full build
    ASSERT_EQ(hft_uint64, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());
    string newJsonString = ToString(ToJson(*schema));
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema(""));
    FromJson(*newSchema, ParseJson(newJsonString));
    KVIndexConfigPtr newKvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig,
            newSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
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
    kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig,
            schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
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
    kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig,
            schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(kkvIndexConfig);
    ASSERT_EQ(hft_murmur, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());
}

IE_NAMESPACE_END(config);

