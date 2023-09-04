#include "indexlib/index/inverted_index/config/DateIndexConfig.h"

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/index/inverted_index/InvertedIndexFactory.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class DateIndexConfigTest : public TESTBASE
{
};

namespace {
std::shared_ptr<DateIndexConfig> CreateDateIndexConfig(const autil::legacy::Any& any)
{
    indexlib::index::InvertedIndexFactory factory;
    std::shared_ptr<IIndexConfig> indexConfig = factory.CreateIndexConfig(any);
    assert(indexConfig);
    const auto& dateIndexConfig = std::dynamic_pointer_cast<DateIndexConfig>(indexConfig);
    assert(dateIndexConfig);
    return dateIndexConfig;
}
} // namespace

TEST_F(DateIndexConfigTest, testSimpleLoad)
{
    std::string jsonStr = R"(
    {
    "build_granularity":
      "minute",
    "format_version_id":
      0,
    "has_shortlist_vbyte_compress":
      false,
    "index_fields":
      "test_time",
    "index_name":
      "test_time",
    "index_type":
      "DATE"
    }
    )";
    auto any = autil::legacy::json::ParseJson(jsonStr);
    std::vector<std::shared_ptr<FieldConfig>> fieldConfigs;
    auto fieldConfig = std::make_shared<FieldConfig>("test_time", ft_uint64, false);
    fieldConfig->SetFieldId(0);
    fieldConfigs.push_back(fieldConfig);
    auto checkConfig = [](const auto& config) {
        ASSERT_EQ(indexlib::config::DateLevelFormat::MINUTE, config->GetBuildGranularity());
        ASSERT_EQ(0, config->GetIndexFormatVersionId());
        ASSERT_FALSE(config->IsShortListVbyteCompress());
        auto fieldConfigs = config->GetFieldConfigs();
        ASSERT_EQ(1, fieldConfigs.size());
        auto fieldConfig = fieldConfigs[0];
        ASSERT_TRUE(fieldConfig);
        ASSERT_EQ("test_time", fieldConfig->GetFieldName());
        ASSERT_EQ(ft_uint64, fieldConfig->GetFieldType());
        ASSERT_FALSE(fieldConfig->IsMultiValue());
        ASSERT_EQ(0, fieldConfig->GetFieldId());
        ASSERT_EQ("test_time", config->GetIndexName());
        ASSERT_EQ(it_date, config->GetInvertedIndexType());
    };
    autil::legacy::json::JsonMap settings;
    MutableJson runtimeSettings;
    IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
    auto config = CreateDateIndexConfig(any);
    config->Deserialize(any, 0, resource);
    config->Check();
    checkConfig(config);

    autil::legacy::Jsonizable::JsonWrapper json;
    config->Serialize(json);
    auto config2 = CreateDateIndexConfig(any);
    IndexConfigDeserializeResource resource2(fieldConfigs, runtimeSettings);
    config2->Deserialize(json.GetMap(), 0, resource);
    config2->Check();
    checkConfig(config2);
}

} // namespace indexlibv2::config
