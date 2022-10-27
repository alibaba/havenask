#include "indexlib/config/test/attribute_config_unittest.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/compress_type_option.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, AttributeConfigTest);

AttributeConfigTest::AttributeConfigTest()
{
}

AttributeConfigTest::~AttributeConfigTest()
{
}

void AttributeConfigTest::SetUp()
{
}

void AttributeConfigTest::TearDown()
{
}

void AttributeConfigTest::TestCreateAttributeConfig()
{    
    // single string
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_string, false, true, false));
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        attrConfig->Init(fieldConfig);
        ASSERT_FALSE(attrConfig->IsLengthFixed());
    }
    // single fixed string
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_string, false, true, false));
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        fieldConfig->SetFixedMultiValueCount(10);
        attrConfig->Init(fieldConfig);
        ASSERT_TRUE(attrConfig->IsLengthFixed());
    }
    
    // single int8
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_int8, false, true, false));
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        attrConfig->Init(fieldConfig);
        ASSERT_TRUE(attrConfig->IsLengthFixed());
    }

    // multi int8
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_int8, true, true, false));
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        attrConfig->Init(fieldConfig);
        ASSERT_FALSE(attrConfig->IsLengthFixed());
    }
    // fixed multi int8
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_int8, true, true, false));
        fieldConfig->SetFixedMultiValueCount(10);
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        attrConfig->Init(fieldConfig);
        ASSERT_TRUE(attrConfig->IsLengthFixed());
    }
    // multi string
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_string, true, true, false));
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        attrConfig->Init(fieldConfig);
        ASSERT_FALSE(attrConfig->IsLengthFixed());
    }
    // fix multi string
    {
        FieldConfigPtr fieldConfig(new FieldConfig("index1", ft_string, true, true, false));
        AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
        fieldConfig->SetFixedMultiValueCount(10);
        attrConfig->Init(fieldConfig);
        ASSERT_FALSE(attrConfig->IsLengthFixed());
    }


}


IE_NAMESPACE_END(config);

