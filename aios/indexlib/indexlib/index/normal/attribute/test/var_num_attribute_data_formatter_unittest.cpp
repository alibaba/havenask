#include "indexlib/index/normal/attribute/test/var_num_attribute_data_formatter_unittest.h"
#include "indexlib/common/field_format/attribute/multi_string_attribute_convertor.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, VarNumAttributeDataFormatterTest);

VarNumAttributeDataFormatterTest::VarNumAttributeDataFormatterTest()
{
}

VarNumAttributeDataFormatterTest::~VarNumAttributeDataFormatterTest()
{
}

void VarNumAttributeDataFormatterTest::CaseSetUp()
{
}

void VarNumAttributeDataFormatterTest::CaseTearDown()
{
}

void VarNumAttributeDataFormatterTest::TestInit()
{
    CheckFormatterInit(ft_integer, true, true, 2);
    CheckFormatterInit(ft_float, true, true, 2);
    CheckFormatterInit(ft_double, true, true, 3);
    CheckFormatterInit(ft_long, true, true, 3);
    CheckFormatterInit(ft_uint32, true, true, 2);
    CheckFormatterInit(ft_uint64, true, true, 3);
    CheckFormatterInit(ft_uint8, true, true, 0);
    CheckFormatterInit(ft_int8, true, true, 0);
    CheckFormatterInit(ft_int16, true, true, 1);
    CheckFormatterInit(ft_uint16, true, true, 1);
    CheckFormatterInit(ft_string, true, true, 0);
    CheckFormatterInit(ft_string, false, true, 0);
}

void VarNumAttributeDataFormatterTest::TestGetNormalAttrDataLength()
{
    uint8_t value[10];
    VarNumAttributeFormatter::EncodeCount(2, (char*)value, 10);
    {
        //test string
        AttributeConfigPtr attrConfig = CreateAttrConfig(ft_string, false, true);
        VarNumAttributeDataFormatter formatter;
        formatter.Init(attrConfig);
        ASSERT_EQ((uint32_t)3, formatter.GetNormalAttrDataLength(value));
    }
    {
        //test int
        AttributeConfigPtr attrConfig = CreateAttrConfig(ft_uint32, true, true);
        VarNumAttributeDataFormatter formatter;
        formatter.Init(attrConfig);
        ASSERT_EQ((uint32_t)9, formatter.GetNormalAttrDataLength(value));
    }
}

void VarNumAttributeDataFormatterTest::TestGetNormalEncodeFloatAttrDataLength()
{
    {
        // fixed length, no compress
        AttributeConfigPtr attrConfig = CreateAttrConfig(ft_float, true, true);
        attrConfig->GetFieldConfig()->SetFixedMultiValueCount(4);
        VarNumAttributeDataFormatter formatter;
        formatter.Init(attrConfig);
        ASSERT_EQ((uint32_t)16, formatter.GetNormalAttrDataLength(NULL));
    }
    {
        // fixed length, fp16 compress        
        AttributeConfigPtr attrConfig = CreateAttrConfig(ft_float, true, true);
        attrConfig->GetFieldConfig()->SetFixedMultiValueCount(4);
        attrConfig->GetFieldConfig()->SetCompressType("fp16");
        
        VarNumAttributeDataFormatter formatter;
        formatter.Init(attrConfig);
        ASSERT_EQ((uint32_t)8, formatter.GetNormalAttrDataLength(NULL));
    }
    {
        // fixed length, block_fp compress                
        AttributeConfigPtr attrConfig = CreateAttrConfig(ft_float, true, true);
        attrConfig->GetFieldConfig()->SetFixedMultiValueCount(4);
        attrConfig->GetFieldConfig()->SetCompressType("block_fp");
        
        VarNumAttributeDataFormatter formatter;
        formatter.Init(attrConfig);
        ASSERT_EQ((uint32_t)9, formatter.GetNormalAttrDataLength(NULL));
    }
}

void VarNumAttributeDataFormatterTest::TestGetMultiStringAttrDataLength()
{
    MultiStringAttributeConvertor convertor;
    string value = convertor.Encode("abcefgefgh");

    common::AttrValueMeta meta = convertor.Decode(ConstString(value));
    AttributeConfigPtr attrConfig = CreateAttrConfig(ft_string, true, true);
    VarNumAttributeDataFormatter formatter;
    formatter.Init(attrConfig);

    uint32_t expectLength = VarNumAttributeFormatter::GetEncodedCountLength(3) + 
                            sizeof(uint8_t) + sizeof(uint8_t) * 3 +
                            VarNumAttributeFormatter::GetEncodedCountLength(3) + 3 +
                            VarNumAttributeFormatter::GetEncodedCountLength(3) + 3 +
                            VarNumAttributeFormatter::GetEncodedCountLength(4) + 4;
    ASSERT_EQ(expectLength, formatter.GetMultiStringAttrDataLength(
                    (const uint8_t*)meta.data.data()));
}

AttributeConfigPtr VarNumAttributeDataFormatterTest::CreateAttrConfig(
        FieldType fieldType, bool IsMultiValue, bool IsUpdatableMultiValue)
{
    FieldConfigPtr fieldConfig(new FieldConfig("test", fieldType, IsMultiValue));
    fieldConfig->SetUpdatableMultiValue(IsUpdatableMultiValue);
    AttributeConfigPtr attrConfig(new AttributeConfig());
    attrConfig->Init(fieldConfig);
    return attrConfig;
}

void VarNumAttributeDataFormatterTest::CheckFormatterInit(
        FieldType fieldType, bool IsMultiValue, bool IsUpdatable, uint32_t fieldSizeShift)
{
    AttributeConfigPtr attrConfig = 
        CreateAttrConfig(fieldType, IsMultiValue, IsUpdatable);
    
    VarNumAttributeDataFormatter formatter;
    formatter.Init(attrConfig);
    ASSERT_EQ(fieldType==ft_string && IsMultiValue, formatter.mIsMultiString);
    ASSERT_EQ(fieldSizeShift, formatter.mFieldSizeShift);
}

IE_NAMESPACE_END(index);

