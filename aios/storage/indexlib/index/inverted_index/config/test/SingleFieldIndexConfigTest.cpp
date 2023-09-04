#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/index/inverted_index/InvertedIndexFactory.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class SingleFieldIndexConfigTest : public TESTBASE
{
};

namespace {
std::shared_ptr<SingleFieldIndexConfig> CreateSingleFieldIndexConfig(const autil::legacy::Any& any)
{
    indexlib::index::InvertedIndexFactory factory;
    std::shared_ptr<IIndexConfig> indexConfig = factory.CreateIndexConfig(any);
    assert(indexConfig);
    const auto& singleFieldIndexConfig = std::dynamic_pointer_cast<SingleFieldIndexConfig>(indexConfig);
    assert(singleFieldIndexConfig);
    return singleFieldIndexConfig;
}
} // namespace

TEST_F(SingleFieldIndexConfigTest, testSimpleLoad)
{
    std::string jsonStr = R"(
    {
			"index_fields":"id",
			"index_name":"id",
			"index_type":"NUMBER",
			"term_frequency_flag":0,
			"high_frequency_adaptive_dictionary":"percent"
    }
    )";
    auto any = autil::legacy::json::ParseJson(jsonStr);
    std::vector<std::shared_ptr<FieldConfig>> fieldConfigs;
    auto fieldConfig = std::make_shared<FieldConfig>("id", ft_integer, false);
    fieldConfig->SetFieldId(0);
    fieldConfigs.push_back(fieldConfig);
    auto checkConfig = [](const auto& config) {
        ASSERT_EQ("id", config->GetIndexName());
        ASSERT_EQ(it_number_int32, config->GetInvertedIndexType());
        auto fieldConfigs = config->GetFieldConfigs();
        ASSERT_EQ(1, fieldConfigs.size());
        auto fieldConfig = fieldConfigs[0];
        ASSERT_TRUE(fieldConfig);
        ASSERT_EQ("id", fieldConfig->GetFieldName());
        ASSERT_EQ(ft_int32, fieldConfig->GetFieldType());
        ASSERT_FALSE(fieldConfig->IsMultiValue());
        ASSERT_FALSE(config->GetOptionFlag() & of_term_frequency);
        auto adaptiveDictConfig = config->GetAdaptiveDictionaryConfig();
        ASSERT_TRUE(adaptiveDictConfig);
        ASSERT_EQ("percent", adaptiveDictConfig->GetRuleName());
        ASSERT_EQ(indexlib::config::AdaptiveDictionaryConfig::PERCENT_ADAPTIVE, adaptiveDictConfig->GetDictType());
        ASSERT_EQ(5, adaptiveDictConfig->GetThreshold());
    };
    std::vector<indexlib::config::AdaptiveDictionaryConfig> adaptiveDicts;
    adaptiveDicts.emplace_back("percent", "PERCENT", 5);
    adaptiveDicts.emplace_back("size", "INDEX_SIZE", 0);
    MutableJson runtimeSettings;
    ASSERT_TRUE(runtimeSettings.SetValue(InvertedIndexConfig::ADAPTIVE_DICTIONARIES, adaptiveDicts));
    IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
    auto config = CreateSingleFieldIndexConfig(any);
    config->Deserialize(any, 0, resource);
    config->Check();
    checkConfig(config);

    autil::legacy::Jsonizable::JsonWrapper json;
    config->Serialize(json);
    auto config2 = CreateSingleFieldIndexConfig(any);
    IndexConfigDeserializeResource resource2(fieldConfigs, runtimeSettings);
    config2->Deserialize(json.GetMap(), 0, resource);
    config2->Check();
    checkConfig(config2);
}

} // namespace indexlibv2::config
