#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/format/single_value_attribute_formatter.h"
#include "indexlib/config/compress_type_option.h"
#include "indexlib/common/field_format/attribute/single_value_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/compress_single_float_attribute_convertor.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);

class SingleValueAttributeFormatterTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForSimple()
    {
        InnerTestForSliceArray<int32_t>(100, 4, 12);
        InnerTestForSliceArray<float>(35, 6, 12);
        InnerTestForMemBuffer<uint64_t>(500, 8, 16);
        InnerTestForMemBuffer<double>(250, 8, 17);
        InnerTestForSliceBuffer<uint64_t>(500, 8, 16);
        InnerTestForSliceBuffer<double>(250, 8, 17);
    }

    void TestCompressedFloat()
    {
        SingleValueAttributeFormatter<float> formatter;
        CompressTypeOption compress;
        compress.Init("fp16");
        formatter.Init(0, 2, compress);
        formatter.SetAttrConvertor(
            AttributeConvertorPtr(
                new CompressSingleFloatAttributeConvertor<int16_t>(compress, "")));

        
        char data[1024];
        float value1 = 1.1, value2 = 1.2, value3 = -1.1;
        float buf1,buf2,buf3;
        util::Fp16Encoder::Encode(value1, (char*)&buf1);
        util::Fp16Encoder::Encode(value2, (char*)&buf2);
        util::Fp16Encoder::Encode(value3, (char*)&buf3);
        formatter.Set(0, (uint8_t*)data, buf1);
        formatter.Set(1, (uint8_t*)data, buf2);
        formatter.Set(2, (uint8_t*)data, buf3);
        
        float output1, output2, output3;
        formatter.Get(0, (uint8_t*)data, output1);
        formatter.Get(1, (uint8_t*)data, output2);
        formatter.Get(2, (uint8_t*)data, output3);
        EXPECT_FLOAT_EQ(1.0996094, output1);
        EXPECT_FLOAT_EQ(1.1992188, output2);
        EXPECT_FLOAT_EQ(-1.0996094, output3);


        char tmpBuffer[4];
        ConstString cs1(tmpBuffer, 4), cs2(tmpBuffer, 4), cs3(tmpBuffer, 4);
        formatter.Get(0, (uint8_t*)data, cs1);
        EXPECT_EQ(*(float*)(cs1.data()), output1);
        formatter.Get(1, (uint8_t*)data, cs2);
        EXPECT_EQ(*(float*)(cs2.data()), output2);
        formatter.Get(2, (uint8_t*)data, cs3);
        EXPECT_EQ(*(float*)(cs3.data()), output3);

        string outputStr;
        const uint8_t* constData = (uint8_t*)data;
        formatter.Get(0, constData, outputStr);
        EXPECT_EQ("1.09961", outputStr);
        formatter.Get(1, constData, outputStr);
        EXPECT_EQ("1.19922", outputStr);
        formatter.Get(2, constData, outputStr);
        EXPECT_EQ("-1.09961", outputStr);
    }

    template<typename T>
    void InnerTestForSliceArray(uint32_t docNum, uint32_t offset, uint32_t recodeSize)
    {
        typedef tr1::shared_ptr<SingleValueAttributeConvertor<T> > SingleValueAttributeConvertorPtr;
        SingleValueAttributeFormatter<T> formatter;
        formatter.Init(offset, recodeSize);
        formatter.SetAttrConvertor(SingleValueAttributeConvertorPtr(
                        new SingleValueAttributeConvertor<T>()));
        ByteAlignedSliceArrayPtr sliceArray(new ByteAlignedSliceArray(16 * 1024));
        
        vector<string> expectedData;
        AddData<T>(sliceArray, formatter, expectedData, docNum);
        CheckData<T>(sliceArray, formatter, expectedData);
    }

    template<typename T>
    void InnerTestForMemBuffer(uint32_t docNum, uint32_t offset, uint32_t recodeSize)
    {
        typedef tr1::shared_ptr<SingleValueAttributeConvertor<T> > SingleValueAttributeConvertorPtr;
        SingleValueAttributeFormatter<T> formatter;
        formatter.Init(offset, recodeSize);
        formatter.SetAttrConvertor(SingleValueAttributeConvertorPtr(
                        new SingleValueAttributeConvertor<T>()));
        
        uint8_t * data = new uint8_t[recodeSize * docNum];
        vector<T> expectedData;
        AddData<T>(data, formatter, expectedData, docNum);
        CheckData<T>(data, formatter, expectedData);
        delete[] data;
    }

    template<typename T>
    void InnerTestForSliceBuffer(uint32_t docNum, uint32_t offset, uint32_t recodeSize)
    {
        typedef tr1::shared_ptr<SingleValueAttributeConvertor<T> > SingleValueAttributeConvertorPtr;
        SingleValueAttributeFormatter<T> formatter;
        formatter.Init(offset, recodeSize);
        formatter.SetAttrConvertor(SingleValueAttributeConvertorPtr(
                        new SingleValueAttributeConvertor<T>()));
        
        uint8_t * data = new uint8_t[recodeSize * docNum];
        vector<T> expectedData;
        AddData<T>(data, formatter, expectedData, docNum, recodeSize);
        CheckData<T>(data, formatter, expectedData);
        delete[] data;
    }

    template<typename T>
    void AddData(uint8_t * data,
                 SingleValueAttributeFormatter<T>& formatter, 
                 vector<T>& expectedData, uint32_t docNum)
    {
        expectedData.resize(docNum);
        T value;
        for (uint32_t i = 0; i < (docNum - 1); ++i)
        {
            value = i * 3 % 10;
            expectedData[i] = value;
            formatter.Set((docid_t)i, data, value);
        }
        // last doc for empty value
        expectedData[docNum - 1] = 0;
        value = 0;
        formatter.Set((docid_t)(docNum - 1), data, value);
    }

    template<typename T>
    void AddData(uint8_t * data,
                 SingleValueAttributeFormatter<T>& formatter, 
                 vector<T>& expectedData, uint32_t docNum,
                 uint32_t recodeSize)
    {
        expectedData.resize(docNum);
        T value;
        for (uint32_t i = 0; i < (docNum - 1); ++i)
        {
            value = i * 3 % 10;
            expectedData[i] = value;
            string fieldValue = StringUtil::toString(value);
            ConstString encodeStr = formatter.mAttrConvertor->Encode(ConstString(fieldValue), &mPool);
            formatter.Set(encodeStr, data + i * recodeSize);
        }
        // last doc for empty value
        expectedData[docNum - 1] = 0;
        value = 0;
        string fieldValue = StringUtil::toString(value);
        ConstString encodeStr = formatter.mAttrConvertor->Encode(ConstString(fieldValue), &mPool);
        formatter.Set(encodeStr, data + (docNum - 1) * recodeSize);
    }

    template<typename T>
    void CheckData(uint8_t * data, SingleValueAttributeFormatter<T>& formatter, 
                   vector<T> &expectedData)
    {
        T value;
        for (uint32_t i = 0; i < expectedData.size(); ++i)
        {
            formatter.Get((docid_t)i, data, value);
            INDEXLIB_TEST_EQUAL(expectedData[i], value);
        }
    }

    template<typename T>
    void AddData(ByteAlignedSliceArrayPtr& data,
                 SingleValueAttributeFormatter<T>& formatter, 
                 vector<string>& expectedData, 
                 uint32_t docNum)
    {
        expectedData.resize(docNum);
        uint32_t i = 0;
        T value = 0;
        for (i = 0; i < (docNum - 1); ++i)
        {
            value = i * 3 % 10;
            string fieldValue = StringUtil::toString(value);
            expectedData[i] = fieldValue;
            ConstString encodeStr = formatter.mAttrConvertor->Encode(ConstString(fieldValue), &mPool);
            formatter.Set((docid_t)i, encodeStr, data);
        }
        
        // Reset fieldvalue
        value += 1;
        string fieldValue = StringUtil::toString(value);
        expectedData[i] = fieldValue;
        ConstString encodeStr = formatter.mAttrConvertor->Encode(ConstString(fieldValue), &mPool);
        formatter.Reset((docid_t)i, encodeStr, data);

        // last doc for empty value
        expectedData[docNum - 1] = "0";
        formatter.Set((docid_t)(docNum - 1), ConstString(), data);
    }

    template<typename T>
    void CheckData(ByteAlignedSliceArrayPtr& data,
                   SingleValueAttributeFormatter<T>& formatter, 
                   vector<string>& expectedData)
    {
        for (uint32_t i = 0; i < expectedData.size(); ++i)
        {
            string value;
            formatter.Get((docid_t)i, data, value);
            INDEXLIB_TEST_EQUAL(expectedData[i], value);
        }
    }

private:
    autil::mem_pool::Pool mPool;
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, SingleValueAttributeFormatterTest);

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeFormatterTest, TestCaseForSimple);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeFormatterTest, TestCompressedFloat);

IE_NAMESPACE_END(index);
