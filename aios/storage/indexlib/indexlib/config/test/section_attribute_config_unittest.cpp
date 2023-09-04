#include "indexlib/config/test/section_attribute_config_unittest.h"

#include "autil/legacy/jsonizable.h"
#include "indexlib/config/CompressTypeOption.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, SectionAttributeConfigTest);

SectionAttributeConfigTest::SectionAttributeConfigTest() {}

SectionAttributeConfigTest::~SectionAttributeConfigTest() {}

void SectionAttributeConfigTest::TestJsonize()
{
    string configStr = "{ \n           \
\"compress_type\":\"uniq|equal\",\n    \
\"has_section_weight\":false, \n       \
\"has_field_id\":false \n              \
}";

    SectionAttributeConfig sectionAttrConfig;
    Any any = ParseJson(configStr);
    FromJson(sectionAttrConfig, any);

    ASSERT_TRUE(sectionAttrConfig.IsUniqEncode());
    ASSERT_TRUE(sectionAttrConfig.HasEquivalentCompress());
    ASSERT_FALSE(sectionAttrConfig.HasSectionWeight());
    ASSERT_FALSE(sectionAttrConfig.HasFieldId());

    Any toAny = ToJson(sectionAttrConfig);
    string toStr = ToString(toAny);

    SectionAttributeConfig newSectionAttrConfig;
    Any newAny = ParseJson(toStr);
    FromJson(newSectionAttrConfig, newAny);

    ASSERT_NO_THROW(sectionAttrConfig.AssertEqual(newSectionAttrConfig));
}

void SectionAttributeConfigTest::TestAssertEqual()
{
    SectionAttributeConfig config("uniq|equal", true, true);

    {
        SectionAttributeConfig newConfig("uniq|equal", true, true);
        ASSERT_NO_THROW(config.AssertEqual(newConfig));
    }

    {
        SectionAttributeConfig newConfig("uniq", true, true);
        ASSERT_ANY_THROW(config.AssertEqual(newConfig));
    }

    {
        SectionAttributeConfig newConfig("uniq|equal", false, true);
        ASSERT_ANY_THROW(config.AssertEqual(newConfig));
    }

    {
        SectionAttributeConfig newConfig("uniq|equal", true, false);
        ASSERT_ANY_THROW(config.AssertEqual(newConfig));
    }
}

void SectionAttributeConfigTest::TestEqual()
{
    SectionAttributeConfig config("uniq|equal", true, true);

    {
        SectionAttributeConfig newConfig("uniq|equal", true, true);
        ASSERT_TRUE(config == newConfig);
        ASSERT_FALSE(config != newConfig);
    }

    {
        SectionAttributeConfig newConfig("uniq", true, true);
        ASSERT_FALSE(config == newConfig);
        ASSERT_TRUE(config != newConfig);
    }

    {
        SectionAttributeConfig newConfig("uniq|equal", false, true);
        ASSERT_FALSE(config == newConfig);
        ASSERT_TRUE(config != newConfig);
    }

    {
        SectionAttributeConfig newConfig("uniq|equal", true, false);
        ASSERT_FALSE(config == newConfig);
        ASSERT_TRUE(config != newConfig);
    }
}

void SectionAttributeConfigTest::TestCreateAttributeConfig()
{
    {
        SectionAttributeConfig sectionAttributeConfig("uniq|equal", true, true);
        AttributeConfigPtr attrConfig = sectionAttributeConfig.CreateAttributeConfig("index1");
        CompressTypeOption compressType = attrConfig->GetCompressType();
        ASSERT_TRUE(compressType.HasEquivalentCompress());
        ASSERT_TRUE(attrConfig->IsUniqEncode());
        ASSERT_TRUE(!attrConfig->IsMultiValue());
        ASSERT_TRUE(!attrConfig->IsAttributeUpdatable());
        ASSERT_EQ("index1_section", attrConfig->GetAttrName());
    }

    {
        SectionAttributeConfig sectionAttributeConfig("equal", true, true);
        AttributeConfigPtr attrConfig = sectionAttributeConfig.CreateAttributeConfig("index1");
        CompressTypeOption compressType = attrConfig->GetCompressType();
        ASSERT_TRUE(compressType.HasEquivalentCompress());
        ASSERT_TRUE(!attrConfig->IsUniqEncode());
        ASSERT_TRUE(!attrConfig->IsMultiValue());
        ASSERT_TRUE(!attrConfig->IsAttributeUpdatable());
        ASSERT_EQ("index1_section", attrConfig->GetAttrName());
    }
}

void SectionAttributeConfigTest::TestSectionAttributeNameToIndexName()
{
    ASSERT_EQ(string("index_name_section"), SectionAttributeConfig::IndexNameToSectionAttributeName("index_name"));
    ASSERT_EQ(string("index_name"), SectionAttributeConfig::SectionAttributeNameToIndexName("index_name_section"));

    ASSERT_EQ(string(""), SectionAttributeConfig::SectionAttributeNameToIndexName(""));
    ASSERT_EQ(string(""), SectionAttributeConfig::SectionAttributeNameToIndexName("abc"));
    ASSERT_EQ(string(""), SectionAttributeConfig::SectionAttributeNameToIndexName("abc_abc"));
}
}} // namespace indexlib::config
