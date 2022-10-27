#ifndef __INDEXLIB_COMPRESSSINGLEFLOATATTRIBUTECONVERTORTEST_H
#define __INDEXLIB_COMPRESSSINGLEFLOATATTRIBUTECONVERTORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/attribute/compress_single_float_attribute_convertor.h"
#include "indexlib/util/fp16_encoder.h"
#include "indexlib/util/float_int8_encoder.h"


IE_NAMESPACE_BEGIN(common);

class CompressSingleFloatAttributeConvertorTest : public INDEXLIB_TESTBASE
{
public:
    CompressSingleFloatAttributeConvertorTest();
    ~CompressSingleFloatAttributeConvertorTest();

    DECLARE_CLASS_NAME(CompressSingleFloatAttributeConvertorTest);
    
public:
    template <typename T>
    void InnerTestEncode(std::string compressType, float value);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

template <typename T>
void CompressSingleFloatAttributeConvertorTest::InnerTestEncode(
        std::string compressType, float value)
{
    config::CompressTypeOption type;
    type.Init(compressType);
    std::string input = autil::StringUtil::toString<float>(value);
    CompressSingleFloatAttributeConvertor<T> convertor(type, "testfield");
    std::string resultStr = convertor.Encode(input);
    ASSERT_EQ(sizeof(T), resultStr.size());

    autil::mem_pool::Pool pool;
    autil::ConstString encodedStr1(resultStr.data(), resultStr.size());
    common::AttrValueMeta attrValueMeta = convertor.Decode(encodedStr1);
    autil::ConstString encodedStr2 = convertor.EncodeFromAttrValueMeta(attrValueMeta, &pool);
    EXPECT_EQ(encodedStr1, encodedStr2);

    float output = 0.0;
    int32_t count = 0;
    if (type.HasFp16EncodeCompress()) {
        count = util::Fp16Encoder::Decode(encodedStr2, (char*)&output);
    }
    else if (type.HasInt8EncodeCompress()) {
        count = util::FloatInt8Encoder::Decode(type.GetInt8AbsMax(), encodedStr2, (char*)&output);
    }
    ASSERT_EQ(1, count);
    ASSERT_TRUE(abs(value - output) <= 0.02) << "value: " << value << " output: " << output;
}


INDEXLIB_UNIT_TEST_CASE(CompressSingleFloatAttributeConvertorTest, TestSimpleProcess);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_COMPRESSSINGLEFLOATATTRIBUTECONVERTORTEST_H
