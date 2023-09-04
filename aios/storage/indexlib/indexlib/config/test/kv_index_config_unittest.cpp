#include "indexlib/config/test/kv_index_config_unittest.h"

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
IE_LOG_SETUP(config, KVIndexConfigTest);

KVIndexConfigTest::KVIndexConfigTest() {}

KVIndexConfigTest::~KVIndexConfigTest() {}

void KVIndexConfigTest::CaseSetUp() {}

void KVIndexConfigTest::CaseTearDown() {}

void KVIndexConfigTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");
    string jsonString = R"(
    {
      "table_name": "kv_table",
      "table_type": "kv",
      "fields": [
        { "field_name": "nid", "field_type": "INT64" },
        { "field_name": "pidvid", "field_type": "INT64", "multi_value": true }
      ],
      "indexs": [
        { "index_name": "nid", "index_fields": "nid", "index_type": "PRIMARY_KEY",
	      "index_preference" : { "type" : "PERF" } }
      ],
      "attributes": [ "nid", "pidvid" ]
    }
    )";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));
    ASSERT_EQ("kv_table", schema->GetSchemaName());
    ASSERT_EQ(tt_kv, schema->GetTableType());
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    ASSERT_EQ(it_kv, indexSchema->GetPrimaryKeyIndexType());
    SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
    ASSERT_TRUE(kvIndexConfig);
    ASSERT_EQ(hft_int64, kvIndexConfig->GetKeyHashFunctionType());

    ASSERT_EQ(0, kvIndexConfig->GetIndexFormatVersionId());
    ASSERT_TRUE(kvIndexConfig->SetIndexFormatVersionId(1).IsOK());
    ASSERT_EQ(0, kvIndexConfig->GetIndexFormatVersionId());
}

void KVIndexConfigTest::TestDisableSimpleValue()
{
    string jsonString = R"(
    {
      "table_name": "kv_table",
      "table_type": "kv",
      "fields": [
        { "field_name": "nid", "field_type": "INT64" },
        { "field_name": "pidvid", "field_type": "INT64" }
      ],
      "indexs": [
        { "index_name": "nid", "index_fields": "nid", "index_type": "PRIMARY_KEY",
	      "index_preference" : { "type" : "PERF" } }
      ],
      "attributes": [ "pidvid" ],
      "versionid" : 1
    }
    )";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
    ASSERT_TRUE(kvIndexConfig);
    auto valueConfig = kvIndexConfig->GetValueConfig();
    ASSERT_FALSE(valueConfig->IsSimpleValue());
}

void KVIndexConfigTest::TestFixValueAutoInline()
{
    // no use_number_hash
    string jsonString = R"(
    {
      "table_name": "kv_table",
      "table_type": "kv",
      "fields": [
        { "field_name": "nid", "field_type": "INT32" },
        { "field_name": "pidvid", "field_type": "INT32", "multi_value": true }
      ],
      "indexs": [
        { "index_name": "nid", "index_fields": "nid", "index_type": "PRIMARY_KEY",
	      "index_preference" : { 
                  "type" : "PERF", 
                  "parameters" : {			    
                      "value" : {
                         "fix_len_auto_inline" : true
                      }
                  }
              } 
        }
      ],
      "attributes": [ "nid", "pidvid" ]
    }
    )";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));

    // inc build or online: for compatible
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
    auto preference = kvIndexConfig->GetIndexPreference();
    auto valueParam = preference.GetValueParam();
    ASSERT_TRUE(valueParam.IsFixLenAutoInline());
}

void KVIndexConfigTest::TestValueImpact()
{
    // no use_number_hash
    string jsonString = R"(
    {
      "table_name": "kv_table",
      "table_type": "kv",
      "fields": [
        { "field_name": "nid", "field_type": "INT64" },
        { "field_name": "pidvid", "field_type": "INT64", "multi_value": true }
      ],
      "indexs": [
        { "index_name": "nid", "index_fields": "nid", "index_type": "PRIMARY_KEY",
	      "index_preference" : { 
                  "type" : "PERF", 
                  "parameters" : {			    
                      "hash_dict" : {
                         "enable_compact_hash_key" : false,
                         "enable_shorten_offset" : false
                      },
                      "value" : {
                         "value_impact" : true
                      }
                  }
              } 
        }
      ],
      "attributes": [ "nid", "pidvid" ]
    }
    )";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));

    string newJsonString = ToString(ToJson(*schema));
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema(""));
    FromJson(*newSchema, ParseJson(newJsonString));

    // inc build or online: for compatible
    IndexSchemaPtr indexSchema = newSchema->GetIndexSchema();
    SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
    ASSERT_TRUE(kvIndexConfig);

    auto valueConfig = kvIndexConfig->GetValueConfig();
    auto packAttrConfig = valueConfig->CreatePackAttributeConfig();
    ASSERT_TRUE(packAttrConfig->HasEnableImpact());
}

void KVIndexConfigTest::TestPlainFormat()
{
    // no use_number_hash
    string jsonString = R"(
    {
      "table_name": "kv_table",
      "table_type": "kv",
      "fields": [
        { "field_name": "nid", "field_type": "INT64" },
        { "field_name": "pidvid", "field_type": "INT64", "multi_value": true }
      ],
      "indexs": [
        { "index_name": "nid", "index_fields": "nid", "index_type": "PRIMARY_KEY",
	      "index_preference" : { 
                  "type" : "PERF", 
                  "parameters" : {			    
                      "hash_dict" : {
                         "enable_compact_hash_key" : false,
                         "enable_shorten_offset" : false
                      },
                      "value" : {
                         "plain_format" : true
                      }
                  }
              } 
        }
      ],
      "attributes": [ "nid", "pidvid" ]
    }
    )";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));

    string newJsonString = ToString(ToJson(*schema));
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema(""));
    FromJson(*newSchema, ParseJson(newJsonString));

    // inc build or online: for compatible
    IndexSchemaPtr indexSchema = newSchema->GetIndexSchema();
    SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
    ASSERT_TRUE(kvIndexConfig);

    auto valueConfig = kvIndexConfig->GetValueConfig();
    auto packAttrConfig = valueConfig->CreatePackAttributeConfig();
    ASSERT_TRUE(packAttrConfig->HasEnablePlainFormat());
}

void KVIndexConfigTest::TestCheckValueFormat()
{
    // check plain_format & value_impact both true
    string jsonString = R"(
    {
      "table_name": "kv_table",
      "table_type": "kv",
      "fields": [
        { "field_name": "nid", "field_type": "INT64" },
        { "field_name": "pidvid", "field_type": "INT64", "multi_value": true }
      ],
      "indexs": [
        { "index_name": "nid", "index_fields": "nid", "index_type": "PRIMARY_KEY",
	      "index_preference" : { 
                  "type" : "PERF", 
                  "parameters" : {			    
                      "hash_dict" : {
                         "enable_compact_hash_key" : false,
                         "enable_shorten_offset" : false
                      },
                      "value" : {
                         "value_impact" : true,
                         "plain_format" : true
                      }
                  }
              } 
        }
      ],
      "attributes": [ "nid", "pidvid" ]
    }
    )";
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    ASSERT_ANY_THROW(FromJson(*schema, ParseJson(jsonString)));
}

void KVIndexConfigTest::TestAutoValueImpact()
{
    autil::EnvUtil::setEnv("INDEXLIB_OPT_KV_STORE", "true", true);

    // no use_number_hash
    string jsonString = R"(
    {
      "table_name": "kv_table",
      "table_type": "kv",
      "fields": [
        { "field_name": "nid", "field_type": "INT64" },
        { "field_name": "pidvid", "field_type": "INT64", "multi_value": true }
      ],
      "indexs": [
        { "index_name": "nid", "index_fields": "nid", "index_type": "PRIMARY_KEY",
	      "index_preference" : { 
                  "type" : "PERF", 
                  "parameters" : {			    
                      "hash_dict" : {
                         "enable_compact_hash_key" : false,
                         "enable_shorten_offset" : false
                      },
                      "value" : {
                          "file_compressor" : "lz4"
                      }
                  }
              } 
        }
      ],
      "attributes": [ "nid", "pidvid" ]
    }
    )";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));

    string newJsonString = ToString(ToJson(*schema));
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema(""));
    FromJson(*newSchema, ParseJson(newJsonString));
    autil::EnvUtil::unsetEnv("INDEXLIB_OPT_KV_STORE");

    // inc build or online: for compatible
    IndexSchemaPtr indexSchema = newSchema->GetIndexSchema();
    SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
    ASSERT_TRUE(kvIndexConfig);

    auto valueConfig = kvIndexConfig->GetValueConfig();
    auto packAttrConfig = valueConfig->CreatePackAttributeConfig();
    ASSERT_TRUE(packAttrConfig->HasEnableImpact());

    auto valueParam = kvIndexConfig->GetIndexPreference().GetValueParam();
    auto compressParams = valueParam.GetFileCompressParameter();
    auto iter = compressParams.find("encode_address_mapper");
    ASSERT_TRUE(iter != compressParams.end());
    ASSERT_EQ(string("true"), iter->second);
}

void KVIndexConfigTest::TestHashFunctionType()
{
    // no use_number_hash
    string jsonString = R"(
    {
      "table_name": "kv_table",
      "table_type": "kv",
      "fields": [
        { "field_name": "nid", "field_type": "INT64" },
        { "field_name": "pidvid", "field_type": "INT64", "multi_value": true }
      ],
      "indexs": [
        { "index_name": "nid", "index_fields": "nid", "index_type": "PRIMARY_KEY",
	      "index_preference" : { 
                  "type" : "PERF", 
                  "parameters" : {			    
                      "hash_dict" : {
                         "enable_compact_hash_key" : false,
                         "enable_shorten_offset" : false
                      }
                  }
              } 
        }
      ],
      "attributes": [ "nid", "pidvid" ]
    }
    )";

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));

    // inc build or online: for compatible
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
    ASSERT_TRUE(kvIndexConfig);
    ASSERT_EQ(hft_int64, kvIndexConfig->GetKeyHashFunctionType());

    // full build
    ASSERT_EQ(hft_int64, kvIndexConfig->GetKeyHashFunctionType());
    string newJsonString = ToString(ToJson(*schema));
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema(""));
    FromJson(*newSchema, ParseJson(newJsonString));
    IndexSchemaPtr newIndexSchema = newSchema->GetIndexSchema();
    SingleFieldIndexConfigPtr newIndexConfig = newIndexSchema->GetPrimaryKeyIndexConfig();
    KVIndexConfigPtr newKvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, newIndexConfig);
    ASSERT_TRUE(newKvIndexConfig);
    ASSERT_EQ(hft_int64, newKvIndexConfig->GetKeyHashFunctionType());
    ASSERT_FALSE(newKvIndexConfig->GetIndexPreference().GetHashDictParam().HasEnableCompactHashKey());
    ASSERT_FALSE(newKvIndexConfig->GetIndexPreference().GetHashDictParam().HasEnableShortenOffset());

    // inc build and online: use_number_hash = true
    jsonString = R"(
    {
      "table_name": "kv_table",
      "table_type": "kv",
      "fields": [
        { "field_name": "nid", "field_type": "UINT64" },
        { "field_name": "pidvid", "field_type": "INT64", "multi_value": true }
      ],
      "indexs": [
        { "index_name": "nid", "index_fields": "nid", "index_type": "PRIMARY_KEY",
	      "index_preference" : { "type" : "PERF" },
          "use_number_hash" : true}
      ],
      "attributes": [ "nid", "pidvid" ]
    }
    )";
    schema.reset(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));
    kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(kvIndexConfig);
    ASSERT_EQ(hft_uint64, kvIndexConfig->GetKeyHashFunctionType());
    ASSERT_TRUE(kvIndexConfig->GetIndexPreference().GetHashDictParam().HasEnableCompactHashKey());
    ASSERT_TRUE(kvIndexConfig->GetIndexPreference().GetHashDictParam().HasEnableShortenOffset());

    // inc build and online: use_number_hash = false
    jsonString = R"(
    {
      "table_name": "kv_table",
      "table_type": "kv",
      "fields": [
        { "field_name": "nid", "field_type": "INT64" },
        { "field_name": "pidvid", "field_type": "INT64", "multi_value": true }
      ],
      "indexs": [
        { "index_name": "nid", "index_fields": "nid", "index_type": "PRIMARY_KEY",
	      "index_preference" : { "type" : "PERF" },
          "use_number_hash" : false}
      ],
      "attributes": [ "nid", "pidvid" ]
    }
    )";
    schema.reset(new IndexPartitionSchema(""));
    FromJson(*schema, ParseJson(jsonString));
    kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(kvIndexConfig);
    ASSERT_EQ(hft_murmur, kvIndexConfig->GetKeyHashFunctionType());
}

void KVIndexConfigTest::TestGetHashFunctionType()
{
    ASSERT_EQ(hft_int64, KVIndexConfig::GetHashFunctionType(ft_int8, true));
    ASSERT_EQ(hft_int64, KVIndexConfig::GetHashFunctionType(ft_int16, true));
    ASSERT_EQ(hft_int64, KVIndexConfig::GetHashFunctionType(ft_int32, true));
    ASSERT_EQ(hft_int64, KVIndexConfig::GetHashFunctionType(ft_int64, true));

    ASSERT_EQ(hft_murmur, KVIndexConfig::GetHashFunctionType(ft_int8, false));
    ASSERT_EQ(hft_murmur, KVIndexConfig::GetHashFunctionType(ft_int16, false));
    ASSERT_EQ(hft_murmur, KVIndexConfig::GetHashFunctionType(ft_int32, false));
    ASSERT_EQ(hft_murmur, KVIndexConfig::GetHashFunctionType(ft_int64, false));

    ASSERT_EQ(hft_uint64, KVIndexConfig::GetHashFunctionType(ft_uint8, true));
    ASSERT_EQ(hft_uint64, KVIndexConfig::GetHashFunctionType(ft_uint16, true));
    ASSERT_EQ(hft_uint64, KVIndexConfig::GetHashFunctionType(ft_uint32, true));
    ASSERT_EQ(hft_uint64, KVIndexConfig::GetHashFunctionType(ft_uint64, true));

    ASSERT_EQ(hft_murmur, KVIndexConfig::GetHashFunctionType(ft_uint8, false));
    ASSERT_EQ(hft_murmur, KVIndexConfig::GetHashFunctionType(ft_uint16, false));
    ASSERT_EQ(hft_murmur, KVIndexConfig::GetHashFunctionType(ft_uint32, false));
    ASSERT_EQ(hft_murmur, KVIndexConfig::GetHashFunctionType(ft_uint64, false));

    ASSERT_EQ(hft_murmur, KVIndexConfig::GetHashFunctionType(ft_double, true));
    ASSERT_EQ(hft_murmur, KVIndexConfig::GetHashFunctionType(ft_double, false));

    ASSERT_EQ(hft_murmur, KVIndexConfig::GetHashFunctionType(ft_float, true));
    ASSERT_EQ(hft_murmur, KVIndexConfig::GetHashFunctionType(ft_float, false));
}
}} // namespace indexlib::config
