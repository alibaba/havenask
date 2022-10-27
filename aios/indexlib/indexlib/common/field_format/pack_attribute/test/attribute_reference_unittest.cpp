#include "indexlib/common/field_format/pack_attribute/test/attribute_reference_unittest.h"
#include "indexlib/util/fp16_encoder.h"
#include "indexlib/util/float_int8_encoder.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, AttributeReferenceTest);

AttributeReferenceTest::AttributeReferenceTest()
{
}

AttributeReferenceTest::~AttributeReferenceTest()
{
}

void AttributeReferenceTest::CaseSetUp()
{
}

void AttributeReferenceTest::CaseTearDown()
{
}

void AttributeReferenceTest::TestSetAndGetCountedMultiValueAttr()
{
    size_t len = 50;
    char* pool = new char[len];
    ConstString valueBuf(pool, len);
    CompressTypeOption compressType;
    // use random offset to read and write
    {
        // fixed string
        ConstString value("abcd");
        size_t offset = 20;
        AttributeReferenceTyped<CountedMultiChar> attrRef(
            offset, "", compressType, value.size());
        attrRef.SetValue(valueBuf.data(), offset, value);

        CountedMultiChar result;
        attrRef.GetValue(valueBuf.data(), result);
        string expected(value.data(), value.size());
        string actual(result.data(), value.size());
        ASSERT_EQ(expected, actual);
    }
    {
        // fixed multi int16
        size_t len = 4;
        uint16_t data[4] = {1,2,3,4};
        CountedMultiUInt16 countedData(data, 4);
        ConstString value(countedData.getBaseAddress(), (size_t)countedData.length());

        size_t offset = 18;
        AttributeReferenceTyped<CountedMultiUInt16> attrRef(
            offset, "", compressType, len);
        attrRef.SetValue(valueBuf.data(), offset, value);
        
        CountedMultiUInt16 result;
        attrRef.GetValue(valueBuf.data(), result);
        ASSERT_EQ(len, result.size());
        for (size_t i = 0; i < len; i++)
        {
            ASSERT_EQ(data[i], result[i]);
        }
    }
    
    delete[] pool;
}

void AttributeReferenceTest::TestSingleFloat()
{
    size_t len = 50;
    char* pool = new char[len];
    ConstString valueBuf(pool, len);
    {
        CompressTypeOption compressType;
        //no compress
        size_t offset = 18;
        AttributeReferenceTyped<float> attrRef(
            offset, "", compressType, 1);
        float temp = 99.111;
        ConstString value((char*)&temp, sizeof(float));
        ASSERT_EQ(sizeof(float), attrRef.SetValue(valueBuf.data(), offset, value));
        string valueStr;
        ASSERT_TRUE(attrRef.GetStrValue(valueBuf.data(), valueStr, NULL));
        ASSERT_EQ("99.111", valueStr);
        
        ConstString data = attrRef.GetDataValue(valueBuf.data());
        float dataValue = *((float*)data.data());
        ASSERT_TRUE(abs(dataValue - temp) <= 0.001);

        float typedValue;
        ASSERT_TRUE(attrRef.GetValue(valueBuf.data(), typedValue));
        ASSERT_TRUE(abs(typedValue - temp) <= 0.001);
    }
    {
        //fp16
        CompressTypeOption compressType;
        compressType.Init("fp16");
        size_t offset = 18;
        AttributeReferenceTyped<float> attrRef(
            offset, "", compressType, 1);
        float temp = 0.99111;
        int16_t testValue;
        Fp16Encoder::Encode(temp, (char*)&testValue);
        ConstString value((char*)&testValue, sizeof(int16_t));
        ASSERT_EQ(sizeof(int16_t), attrRef.SetValue(valueBuf.data(), offset, value));
        string valueStr;
        ASSERT_TRUE(attrRef.GetStrValue(valueBuf.data(), valueStr, NULL));
        ASSERT_EQ("0.990723", valueStr);
        
        ConstString data = attrRef.GetDataValue(valueBuf.data());
        float dataValue = 0.0;
        ASSERT_EQ(1, Fp16Encoder::Decode(data, (char*)&dataValue));
        ASSERT_TRUE(abs(dataValue - temp) <= 0.001);

        float typedValue;
        ASSERT_TRUE(attrRef.GetValue(valueBuf.data(), typedValue));
        ASSERT_TRUE(abs(typedValue - temp) <= 0.001);
    }
    {
        //int8
        CompressTypeOption compressType;
        compressType.Init("int8#1");
        size_t offset = 18;
        AttributeReferenceTyped<float> attrRef(
            offset, "", compressType, 1);
        float temp = -0.8;
        int8_t testValue;
        util::FloatInt8Encoder::Encode(1.0, temp, (char*)&testValue);
        ConstString value((char*)&testValue, sizeof(int8_t));
        ASSERT_EQ(sizeof(int8_t), attrRef.SetValue(valueBuf.data(), offset, value));
        string valueStr;
        ASSERT_TRUE(attrRef.GetStrValue(valueBuf.data(), valueStr, NULL));
        ASSERT_EQ("-0.80315", valueStr);
        
        ConstString data = attrRef.GetDataValue(valueBuf.data());
        float dataValue = 0.0;
        ASSERT_EQ(1, util::FloatInt8Encoder::Decode(1.0, data, (char*)&dataValue));
        ASSERT_TRUE(abs(dataValue - temp) <= 0.01);

        float typedValue;
        ASSERT_TRUE(attrRef.GetValue(valueBuf.data(), typedValue));
        ASSERT_TRUE(abs(typedValue - temp) <= 0.01);
    }
    delete[] pool;
}

IE_NAMESPACE_END(common);

