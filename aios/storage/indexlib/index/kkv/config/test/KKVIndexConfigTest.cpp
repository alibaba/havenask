
#include "indexlib/index/kkv/config/KKVIndexConfig.h"

#include "autil/EnvUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil::legacy::json;
using namespace autil::legacy;

namespace indexlibv2::config {

class KKVIndexConfigTest : public TESTBASE
{
public:
    void setUp() override {};
    void tearDown() override {};

protected:
    void CreateKKVIndexConfigFromLegacySchema(const string& legacySchemaStr,
                                              std::shared_ptr<KKVIndexConfig>& kkvIndexConfig) const;

    void RecreateLegacySchemaWithKKVIndexConfig(const string& orginLegacySchemaStr,
                                                const std::shared_ptr<KKVIndexConfig>& kkvIndexConfig,
                                                string& newLegacySchemaStr) const;

private:
    void CreateValueConfigJsonFromLegacySchema(const JsonMap& legacySchemaJsonMap, Any& valueConfigJson) const;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.config, KKVIndexConfigTest);

TEST_F(KKVIndexConfigTest, TestLegacySimpleProcess)
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

    std::shared_ptr<KKVIndexConfig> kkvIndexConfig;
    ASSERT_NO_THROW(CreateKKVIndexConfigFromLegacySchema(jsonString, kkvIndexConfig));
    ASSERT_NE(nullptr, kkvIndexConfig);
    ASSERT_EQ(indexlib::hft_murmur, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(indexlib::hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());
    ASSERT_EQ(indexlib::hft_murmur, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(indexlib::hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());
    ASSERT_EQ((uint32_t)200, kkvIndexConfig->GetSuffixKeySkipListThreshold());
}

TEST_F(KKVIndexConfigTest, TestGetHashFunctionType)
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
    std::shared_ptr<KKVIndexConfig> kkvIndexConfig;
    ASSERT_NO_THROW(CreateKKVIndexConfigFromLegacySchema(jsonString, kkvIndexConfig));
    ASSERT_NE(nullptr, kkvIndexConfig);
    ASSERT_EQ(indexlib::hft_uint64, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(indexlib::hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());

    // full build
    string newJsonString;
    ASSERT_NO_THROW(RecreateLegacySchemaWithKKVIndexConfig(jsonString, kkvIndexConfig, newJsonString));
    kkvIndexConfig.reset();
    ASSERT_NO_THROW(CreateKKVIndexConfigFromLegacySchema(newJsonString, kkvIndexConfig));
    ASSERT_NE(nullptr, kkvIndexConfig);
    ASSERT_EQ(indexlib::hft_uint64, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(indexlib::hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());
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
    kkvIndexConfig.reset();
    ASSERT_NO_THROW(CreateKKVIndexConfigFromLegacySchema(jsonString, kkvIndexConfig));
    ASSERT_NE(nullptr, kkvIndexConfig);
    ASSERT_EQ(indexlib::hft_uint64, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(indexlib::hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());

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
    kkvIndexConfig.reset();
    ASSERT_NO_THROW(CreateKKVIndexConfigFromLegacySchema(jsonString, kkvIndexConfig));
    ASSERT_NE(nullptr, kkvIndexConfig);
    ASSERT_EQ(indexlib::hft_murmur, kkvIndexConfig->GetPrefixHashFunctionType());
    ASSERT_EQ(indexlib::hft_int64, kkvIndexConfig->GetSuffixHashFunctionType());
}

TEST_F(KKVIndexConfigTest, TestPlainFormat)
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

    std::shared_ptr<KKVIndexConfig> kkvIndexConfig;
    ASSERT_NO_THROW(CreateKKVIndexConfigFromLegacySchema(jsonString, kkvIndexConfig));
    ASSERT_NE(nullptr, kkvIndexConfig);

    string newJsonString;
    RecreateLegacySchemaWithKKVIndexConfig(jsonString, kkvIndexConfig, newJsonString);
    kkvIndexConfig.reset();
    ASSERT_NO_THROW(CreateKKVIndexConfigFromLegacySchema(newJsonString, kkvIndexConfig));
    ASSERT_NE(nullptr, kkvIndexConfig);
    auto valueConfig = kkvIndexConfig->GetValueConfig();
    auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(packAttrConfig->HasEnablePlainFormat());
}

TEST_F(KKVIndexConfigTest, TestCheckValueFormat)
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
                    "value" : { "encode" : true, "file_compressor" : "lz4", "value_impact" : true, "plain_format" :
                    true }
                }
            }
        }
        ],
        "attributes": [ "nick", "nid" ]
    }
    )";

    std::shared_ptr<KKVIndexConfig> kkvIndexConfig;
    ASSERT_ANY_THROW(CreateKKVIndexConfigFromLegacySchema(jsonString, kkvIndexConfig));
}

TEST_F(KKVIndexConfigTest, TestValueImpact)
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

    std::shared_ptr<KKVIndexConfig> kkvIndexConfig;
    ASSERT_NO_THROW(CreateKKVIndexConfigFromLegacySchema(jsonString, kkvIndexConfig));
    ASSERT_NE(nullptr, kkvIndexConfig);

    string newJsonString;
    ASSERT_NO_THROW(RecreateLegacySchemaWithKKVIndexConfig(jsonString, kkvIndexConfig, newJsonString));
    kkvIndexConfig.reset();
    ASSERT_NO_THROW(CreateKKVIndexConfigFromLegacySchema(newJsonString, kkvIndexConfig));

    auto valueConfig = kkvIndexConfig->GetValueConfig();
    auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(packAttrConfig->HasEnableImpact());
}

TEST_F(KKVIndexConfigTest, TestAutoValueImpact)
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
    std::shared_ptr<KKVIndexConfig> kkvIndexConfig;
    ASSERT_NO_THROW(CreateKKVIndexConfigFromLegacySchema(jsonString, kkvIndexConfig));
    ASSERT_NE(nullptr, kkvIndexConfig);

    string newJsonString;
    ASSERT_NO_THROW(RecreateLegacySchemaWithKKVIndexConfig(jsonString, kkvIndexConfig, newJsonString));
    kkvIndexConfig.reset();
    ASSERT_NO_THROW(CreateKKVIndexConfigFromLegacySchema(newJsonString, kkvIndexConfig));

    autil::EnvUtil::unsetEnv("INDEXLIB_OPT_KV_STORE");

    auto valueConfig = kkvIndexConfig->GetValueConfig();
    auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(packAttrConfig->HasEnableImpact());

    auto valueParam = kkvIndexConfig->GetIndexPreference().GetValueParam();
    auto compressParams = valueParam.GetFileCompressParameter();
    auto iter = compressParams.find("encode_address_mapper");
    ASSERT_TRUE(iter != compressParams.end());
    ASSERT_EQ(string("true"), iter->second);
}

void KKVIndexConfigTest::CreateKKVIndexConfigFromLegacySchema(const string& legacySchemaStr,
                                                              std::shared_ptr<KKVIndexConfig>& kkvIndexConfig) const
{
    JsonMap jsonMap;
    try {
        FromJsonString(jsonMap, legacySchemaStr);
    } catch (const ExceptionBase& e) {
        AUTIL_LOG(ERROR, "exception %s", e.what());
        throw e;
    }

    std::vector<std::shared_ptr<FieldConfig>> fieldConfigs;
    Any prefixFieldConfigAny;
    Any suffixFieldConfigAny;
    {
        auto iterator = jsonMap.find("fields");
        ASSERT_NE(jsonMap.end(), iterator);
        try {
            auto anyFieldConfigs = AnyCast<vector<Any>>(iterator->second);
            for (const auto& anyField : anyFieldConfigs) {
                auto field = std::make_shared<FieldConfig>();
                FromJson(*field, anyField);
                fieldConfigs.emplace_back(std::move(field));
            }
            ASSERT_EQ(2, anyFieldConfigs.size());
            prefixFieldConfigAny = anyFieldConfigs[0];
            suffixFieldConfigAny = anyFieldConfigs[1];
        } catch (const ExceptionBase& e) {
            AUTIL_LOG(ERROR, "exception %s", e.what());
            throw e;
        }
    }

    map<string, Any> indexConfigAny;
    {
        auto iterator = jsonMap.find("indexs");
        ASSERT_NE(jsonMap.end(), iterator);
        try {
            auto anyFieldConfigs = AnyCast<vector<Any>>(iterator->second);
            ASSERT_EQ(1, anyFieldConfigs.size());
            indexConfigAny = AnyCast<map<string, Any>>(anyFieldConfigs[0]);
        } catch (const ExceptionBase& e) {
            AUTIL_LOG(ERROR, "exception %s", e.what());
            throw e;
        }
    }

    auto prefixFieldConfig = std::make_shared<FieldConfig>();
    try {
        autil::legacy::FromJson(*prefixFieldConfig, prefixFieldConfigAny);
    } catch (const ExceptionBase& e) {
        AUTIL_LOG(ERROR, "exception %s", e.what());
        throw e;
    }

    auto suffixFieldConfig = std::make_shared<FieldConfig>();
    try {
        autil::legacy::FromJson(*suffixFieldConfig, suffixFieldConfigAny);
    } catch (const ExceptionBase& e) {
        AUTIL_LOG(ERROR, "exception %s", e.what());
        throw e;
    }

    Any valueConfigJson;
    CreateValueConfigJsonFromLegacySchema(jsonMap, valueConfigJson);
    indexConfigAny["value_fields"] = valueConfigJson;

    kkvIndexConfig = std::make_shared<KKVIndexConfig>();
    MutableJson runtimeSettings;
    IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
    try {
        kkvIndexConfig->Deserialize(indexConfigAny, 0, resource);
    } catch (const ExceptionBase& e) {
        AUTIL_LOG(ERROR, "exception %s", e.what());
        kkvIndexConfig.reset();
        throw e;
    }
}

void KKVIndexConfigTest::CreateValueConfigJsonFromLegacySchema(const JsonMap& legacySchemaJsonMap,
                                                               Any& valueConfigJson) const
{
    auto it = legacySchemaJsonMap.find("attributes");
    ASSERT_TRUE(it != legacySchemaJsonMap.end());
    valueConfigJson = it->second;
}

void KKVIndexConfigTest::RecreateLegacySchemaWithKKVIndexConfig(const string& orginLegacySchemaStr,
                                                                const std::shared_ptr<KKVIndexConfig>& kkvIndexConfig,
                                                                string& newLegacySchemaStr) const
{
    JsonMap jsonMap;
    try {
        FromJsonString(jsonMap, orginLegacySchemaStr);
    } catch (const ExceptionBase& e) {
        AUTIL_LOG(ERROR, "exception %s", e.what());
        throw e;
    }

    try {
        autil::legacy::Jsonizable::JsonWrapper json;
        kkvIndexConfig->Serialize(json);
        auto indexConfigAny = json.GetMap();
        jsonMap["indexs"] = vector<Any>({indexConfigAny});
        newLegacySchemaStr = ToJsonString(jsonMap);
    } catch (const ExceptionBase& e) {
        AUTIL_LOG(ERROR, "exception %s", e.what());
        throw e;
    }
}

} // namespace indexlibv2::config
