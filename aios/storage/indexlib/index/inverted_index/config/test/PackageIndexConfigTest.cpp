#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/index/inverted_index/InvertedIndexFactory.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class PackageIndexConfigTest : public TESTBASE
{
};

namespace {
std::shared_ptr<PackageIndexConfig> CreatePackageIndexConfig(const autil::legacy::Any& any)
{
    indexlib::index::InvertedIndexFactory factory;
    std::shared_ptr<IIndexConfig> indexConfig = factory.CreateIndexConfig(any);
    assert(indexConfig);
    const auto& packageIndexConfig = std::dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
    assert(packageIndexConfig);
    return packageIndexConfig;
}
} // namespace

TEST_F(PackageIndexConfigTest, testSimpleLoad)
{
    std::string jsonStr = R"(
    {
    "doc_payload_flag":
      1,
    "format_version_id":
      0,
    "has_shortlist_vbyte_compress":
      false,
    "index_analyzer":
      "internet_analyzer",
    "index_fields":
      [
        {
        "boost":
          100,
        "field_name":
          "title"
        },
        {
        "boost":
          200,
        "field_name":
          "body"
        }
      ],
    "index_name":
      "pack_index",
    "index_type":
      "PACK",
    "position_list_flag":
      1,
    "position_payload_flag":
      1,
    "term_frequency_bitmap":
      0,
    "term_payload_flag":
      1
    }
    )";
    auto any = autil::legacy::json::ParseJson(jsonStr);
    std::vector<std::string> fields {"title", "body"};
    std::vector<std::shared_ptr<FieldConfig>> fieldConfigs;
    fieldid_t fieldId = 0;
    for (const auto& fieldName : fields) {
        auto fieldConfig = std::make_shared<FieldConfig>(fieldName, ft_text, false);
        fieldConfig->SetFieldId(fieldId++);
        fieldConfig->SetAnalyzerName("internet_analyzer");
        fieldConfigs.push_back(fieldConfig);
    }
    auto checkConfig = [](const auto& config) {
        ASSERT_TRUE(config->GetOptionFlag() & of_doc_payload);
        ASSERT_EQ(0, config->GetIndexFormatVersionId());
        ASSERT_FALSE(config->IsShortListVbyteCompress());
        ASSERT_EQ("internet_analyzer", config->GetAnalyzer());
        auto fieldConfigs = config->GetFieldConfigs();
        ASSERT_EQ(2, fieldConfigs.size());
        {
            auto fieldConfig = fieldConfigs[0];
            ASSERT_TRUE(fieldConfig);
            ASSERT_EQ("title", fieldConfig->GetFieldName());
            ASSERT_EQ(ft_text, fieldConfig->GetFieldType());
            ASSERT_FALSE(fieldConfig->IsMultiValue());
            ASSERT_EQ("internet_analyzer", fieldConfig->GetAnalyzerName());
            ASSERT_EQ(0, fieldConfig->GetFieldId());
        }
        {
            auto fieldConfig = fieldConfigs[1];
            ASSERT_TRUE(fieldConfig);
            ASSERT_EQ("body", fieldConfig->GetFieldName());
            ASSERT_EQ(ft_text, fieldConfig->GetFieldType());
            ASSERT_FALSE(fieldConfig->IsMultiValue());
            ASSERT_EQ("internet_analyzer", fieldConfig->GetAnalyzerName());
            ASSERT_EQ(1, fieldConfig->GetFieldId());
        }
        ASSERT_EQ(2, config->GetFieldCount());
        ASSERT_EQ(100, config->GetFieldBoost(0));
        ASSERT_EQ(200, config->GetFieldBoost(1));
        ASSERT_EQ("pack_index", config->GetIndexName());
        ASSERT_EQ(it_pack, config->GetInvertedIndexType());
        ASSERT_TRUE(config->GetOptionFlag() & of_position_list);
        ASSERT_FALSE(config->GetOptionFlag() & of_tf_bitmap);
        ASSERT_TRUE(config->GetOptionFlag() & of_term_payload);
    };
    autil::legacy::json::JsonMap settings;
    MutableJson runtimeSettings;
    IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
    auto config = CreatePackageIndexConfig(any);
    config->Deserialize(any, 0, resource);
    config->Check();
    checkConfig(config);

    autil::legacy::Jsonizable::JsonWrapper json;
    config->Serialize(json);
    auto config2 = CreatePackageIndexConfig(any);
    IndexConfigDeserializeResource resource2(fieldConfigs, runtimeSettings);
    config2->Deserialize(json.GetMap(), 0, resource);
    config2->Check();
    checkConfig(config2);
}

TEST_F(PackageIndexConfigTest, TestCaseFor33FieldCountInPack)
{
    PackageIndexConfig indexConfig("test", it_pack);
    for (size_t i = 0; i < 33; ++i) {
        auto fieldConfig = std::make_shared<FieldConfig>("f" + std::to_string(i), ft_text, false);
        fieldConfig->SetFieldId(i);
        auto status = indexConfig.AddFieldConfig(fieldConfig, i);
        if (i != 32) {
            ASSERT_TRUE(status.IsOK());
        } else {
            ASSERT_FALSE(status.IsOK());
        }
    }
}

TEST_F(PackageIndexConfigTest, TestCaseFor9FieldCountInExpack)
{
    PackageIndexConfig indexConfig("test", it_expack);
    for (size_t i = 0; i < 9; ++i) {
        auto fieldConfig = std::make_shared<FieldConfig>("f" + std::to_string(i), ft_text, false);
        fieldConfig->SetFieldId(i);
        auto status = indexConfig.AddFieldConfig(fieldConfig, i);
        if (i != 8) {
            ASSERT_TRUE(status.IsOK());
        } else {
            ASSERT_FALSE(status.IsOK());
        }
    }
}

} // namespace indexlibv2::config
