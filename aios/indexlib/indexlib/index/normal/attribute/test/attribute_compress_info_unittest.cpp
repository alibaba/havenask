#include "indexlib/index/normal/attribute/test/attribute_compress_info_unittest.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeCompressInfoTest);

AttributeCompressInfoTest::AttributeCompressInfoTest()
{
}

AttributeCompressInfoTest::~AttributeCompressInfoTest()
{
}

void AttributeCompressInfoTest::SetUp()
{
}

void AttributeCompressInfoTest::TearDown()
{
}

void AttributeCompressInfoTest::TestNeedCompressData()
{
    {
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig("field", ft_string, false, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        ASSERT_TRUE(!AttributeCompressInfo::NeedCompressData(attrConfig));
    }

    {
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig("field", ft_integer, true, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        ASSERT_TRUE(!AttributeCompressInfo::NeedCompressData(attrConfig));
    }

    {
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig("field", ft_integer, false, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        ASSERT_TRUE(AttributeCompressInfo::NeedCompressData(attrConfig));
    }        
}

void AttributeCompressInfoTest::TestNeedCompressOffset()
{
    {
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig("field", ft_string, false, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        ASSERT_TRUE(AttributeCompressInfo::NeedCompressOffset(attrConfig));
    }

    {
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig("field", ft_integer, true, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        ASSERT_TRUE(AttributeCompressInfo::NeedCompressOffset(attrConfig));
    }

    {
        AttributeConfigPtr attrConfig(new AttributeConfig);
        FieldConfigPtr fieldConfig(new FieldConfig("field", ft_integer, false, false));
        fieldConfig->SetCompressType("equal|uniq");
        attrConfig->Init(fieldConfig);
        ASSERT_FALSE(AttributeCompressInfo::NeedCompressOffset(attrConfig));
    }
}

IE_NAMESPACE_END(index);

