#include <autil/legacy/any.h>
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include "indexlib/config/test/kv_index_config_unittest.h"

using namespace std;
using namespace std::tr1;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, KVIndexConfigTest);

KVIndexConfigTest::KVIndexConfigTest()
{
}

KVIndexConfigTest::~KVIndexConfigTest()
{
}

void KVIndexConfigTest::CaseSetUp()
{
}

void KVIndexConfigTest::CaseTearDown()
{
}

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
    SingleFieldIndexConfigPtr newIndexConfig =
        newIndexSchema->GetPrimaryKeyIndexConfig();
    KVIndexConfigPtr newKvIndexConfig =
        DYNAMIC_POINTER_CAST(KVIndexConfig, newIndexConfig);
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
    kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig,
            schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
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
    kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig,
            schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
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

IE_NAMESPACE_END(config);

