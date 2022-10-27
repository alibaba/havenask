#include "indexlib/config/test/package_index_config_unittest.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, PackageIndexConfigTest);

PackageIndexConfigTest::PackageIndexConfigTest()
{
}

PackageIndexConfigTest::~PackageIndexConfigTest()
{
}

void PackageIndexConfigTest::SetUp()
{
}

void PackageIndexConfigTest::TearDown()
{
}

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

        SectionAttributeConfigPtr sectionAttrConfig = 
            packIndexConfig.GetSectionAttributeConfig();
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

        SectionAttributeConfigPtr sectionAttrConfig = 
            packIndexConfig.GetSectionAttributeConfig();
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

IE_NAMESPACE_END(config);

