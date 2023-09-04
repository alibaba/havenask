#include "indexlib/config/test/package_index_config_unittest.h"

#include "autil/legacy/jsonizable.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlib::util;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, PackageIndexConfigTest);

PackageIndexConfigTest::PackageIndexConfigTest() {}

PackageIndexConfigTest::~PackageIndexConfigTest() {}

void PackageIndexConfigTest::TestJsonize()
{
    // From Json
    {
        string configStr = "{\n            \
\"has_section_attribute\": false, \n   \
\"use_hash_typed_dictionary\":true, \n \
\"section_attribute_config\": \n       \
{ \n                                   \
\"compress_type\":\"uniq\", \n         \
\"has_section_weight\":false, \n       \
\"has_field_id\":false \n             \
}\n                                    \
}";

        PackageIndexConfig packIndexConfig("packIndex", it_pack);
        Any any = ParseJson(configStr);
        FromJson(packIndexConfig, any);

        ASSERT_FALSE(packIndexConfig.HasSectionAttribute());
        ASSERT_FALSE(packIndexConfig.GetSectionAttributeConfig());
        ASSERT_TRUE(packIndexConfig.IsHashTypedDictionary());
    }

    {
        string configStr = "{\n            \
\"has_section_attribute\": true \n         \
}";

        PackageIndexConfig packIndexConfig("packIndex", it_pack);
        Any any = ParseJson(configStr);
        FromJson(packIndexConfig, any);

        ASSERT_TRUE(packIndexConfig.HasSectionAttribute());

        SectionAttributeConfigPtr sectionAttrConfig = packIndexConfig.GetSectionAttributeConfig();
        ASSERT_TRUE(sectionAttrConfig);
        ASSERT_FALSE(sectionAttrConfig->IsUniqEncode());
        ASSERT_TRUE(sectionAttrConfig->HasSectionWeight());
        ASSERT_TRUE(sectionAttrConfig->HasFieldId());
    }

    {
        string configStr = "{\n            \
\"has_section_attribute\": true, \n        \
\"section_attribute_config\": \n           \
{ \n                                       \
\"compress_type\":\"uniq\", \n             \
\"has_section_weight\":false, \n           \
\"has_field_id\":false \n                  \
}\n                                        \
}";

        PackageIndexConfig packIndexConfig("packIndex", it_pack);
        Any any = ParseJson(configStr);
        FromJson(packIndexConfig, any);

        ASSERT_TRUE(packIndexConfig.HasSectionAttribute());

        SectionAttributeConfigPtr sectionAttrConfig = packIndexConfig.GetSectionAttributeConfig();
        ASSERT_TRUE(sectionAttrConfig);
        ASSERT_TRUE(sectionAttrConfig->IsUniqEncode());
        ASSERT_FALSE(sectionAttrConfig->HasSectionWeight());
        ASSERT_FALSE(sectionAttrConfig->HasFieldId());
    }

    // TO JSON
    {
        string configStr = "{\n            \
\"has_section_attribute\": true, \n        \
\"section_attribute_config\": \n           \
{ \n                                         \
\"compress_type\":\"uniq\", \n               \
\"has_section_weight\":false, \n             \
\"has_field_id\":false \n                    \
}\n                                          \
}";

        PackageIndexConfig packIndexConfig("packIndex", it_pack);
        Any any = ParseJson(configStr);
        FromJson(packIndexConfig, any);

        Any toAny = ToJson(packIndexConfig);
        string toString = ToString(toAny);

        PackageIndexConfig newPackIndexConfig("packIndex", it_pack);
        Any newAny = ParseJson(toString);
        FromJson(newPackIndexConfig, newAny);

        ASSERT_NO_THROW(packIndexConfig.AssertEqual(newPackIndexConfig));
    }
}

void PackageIndexConfigTest::TestSupportNull()
{
    PackageIndexConfig packIndexConfig("packIndex", it_pack);
    packIndexConfig.AddFieldConfig(MakeFieldConfig("field1", ft_text, 0, false));
    ASSERT_FALSE(packIndexConfig.SupportNull());

    packIndexConfig.AddFieldConfig(MakeFieldConfig("field2", ft_text, 1, true));
    ASSERT_TRUE(packIndexConfig.SupportNull());

    FieldConfigPtr fieldConfig = MakeFieldConfig("field3", ft_text, 2, true);
    fieldConfig->SetNullFieldLiteralString("new_default_null");
    ASSERT_THROW(packIndexConfig.AddFieldConfig(fieldConfig), SchemaException);
}

void PackageIndexConfigTest::TestCheck()
{
    PackageIndexConfig packIndexConfig("packIndex", it_pack);
    packIndexConfig.AddFieldConfig(MakeFieldConfig("field1", ft_text, 0, false));
    packIndexConfig.Check(); // nothing happen

    {
        FieldConfigPtr fieldConfig = MakeFieldConfig("field2", ft_text, 1, false);
        fieldConfig->SetUserDefinedParam(ConvertToJsonMap({{"noused", "555"}}));
        packIndexConfig.AddFieldConfig(fieldConfig);
        ASSERT_EQ(KeyValueMap {}, packIndexConfig.GetDictHashParams());
        packIndexConfig.Check(); // nothing happen
    }
    {
        FieldConfigPtr fieldConfig = MakeFieldConfig("field3", ft_text, 2, false);
        fieldConfig->SetUserDefinedParam(util::ConvertToJsonMap({{"dict_hash_func", "default"}}));
        packIndexConfig.AddFieldConfig(fieldConfig);
        ASSERT_EQ(KeyValueMap {}, packIndexConfig.GetDictHashParams());
        packIndexConfig.Check(); // nothing happen
    }
    {
        FieldConfigPtr fieldConfig = MakeFieldConfig("field4", ft_text, 3, false);
        fieldConfig->SetUserDefinedParam(ConvertToJsonMap({{"dict_hash_func", "layerhash"}}));
        packIndexConfig.AddFieldConfig(fieldConfig);
        ASSERT_EQ(KeyValueMap {}, packIndexConfig.GetDictHashParams());
        ASSERT_THROW(packIndexConfig.Check(), SchemaException); // pack dict func must be default
    }
}

FieldConfigPtr PackageIndexConfigTest::MakeFieldConfig(const string& fieldName, FieldType fieldType, fieldid_t fieldId,
                                                       bool isNull)
{
    FieldConfigPtr fieldConfig(new FieldConfig(fieldName, fieldType, false));
    fieldConfig->SetEnableNullField(isNull);
    fieldConfig->SetFieldId(fieldId);
    return fieldConfig;
}

}} // namespace indexlib::config
