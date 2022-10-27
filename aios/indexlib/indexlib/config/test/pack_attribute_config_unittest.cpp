#include "indexlib/config/test/pack_attribute_config_unittest.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, PackAttributeConfigTest);

PackAttributeConfigTest::PackAttributeConfigTest()
{
}

PackAttributeConfigTest::~PackAttributeConfigTest()
{
}

void PackAttributeConfigTest::SetUp()
{
}

void PackAttributeConfigTest::TearDown()
{
}

void PackAttributeConfigTest::TestCreateAttributeConfig()
{
    // single string
    FieldConfigPtr stringField(new FieldConfig("string", ft_string, false, true, false));
    AttributeConfigPtr stringAttr(new AttributeConfig(AttributeConfig::ct_normal));
    stringAttr->Init(stringField);
    // single fixed string
    FieldConfigPtr fixedStringField(new FieldConfig("fixedString", ft_string, false, true, false));
    fixedStringField->SetFixedMultiValueCount(5);
    AttributeConfigPtr fixedStringAttr(new AttributeConfig(AttributeConfig::ct_normal));
    fixedStringAttr->Init(fixedStringField);
    // single int16
    FieldConfigPtr int16Field(new FieldConfig("int16", ft_int16, false, true, false));
    AttributeConfigPtr int16Attr(new AttributeConfig(AttributeConfig::ct_normal));
    int16Attr->Init(int16Field);
    // single int64
    FieldConfigPtr int64Field(new FieldConfig("int64", ft_int64, false, true, false));
    AttributeConfigPtr int64Attr(new AttributeConfig(AttributeConfig::ct_normal));
    int64Attr->Init(int64Field);

    // multi int32
    FieldConfigPtr multiInt32Field(new FieldConfig("multiInt32", ft_int32, true, true, false));
    AttributeConfigPtr multiInt32Attr(new AttributeConfig(AttributeConfig::ct_normal));
    multiInt32Attr->Init(multiInt32Field);
    // fixed multi int32
    FieldConfigPtr fixedMultiInt32Field(new FieldConfig("fixedMultiInt32", ft_int32, true, true, false));
    fixedMultiInt32Field->SetFixedMultiValueCount(8);
    AttributeConfigPtr fixedMultiInt32Attr(new AttributeConfig(AttributeConfig::ct_normal));
    fixedMultiInt32Attr->Init(fixedMultiInt32Field);

    CompressTypeOption compressType;
    compressType.Init("uniq");
    // 1.total length is fixed, mEnableCompactFormat=true
    // single int16, fixed multi int32
    {
        PackAttributeConfigPtr packAttrConfig(new PackAttributeConfig("attr",
                        compressType, FIELD_DEFAULT_DEFRAG_SLICE_PERCENT));
        packAttrConfig->AddAttributeConfig(int16Attr);
        packAttrConfig->AddAttributeConfig(fixedMultiInt32Attr);
        AttributeConfigPtr packAttr = packAttrConfig->CreateAttributeConfig();
        ASSERT_FALSE(packAttr->IsLengthFixed());
    }
}

IE_NAMESPACE_END(config);

