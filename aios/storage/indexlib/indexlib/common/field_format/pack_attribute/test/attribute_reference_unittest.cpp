#include "indexlib/common/field_format/pack_attribute/test/attribute_reference_unittest.h"

#include "indexlib/util/FloatInt8Encoder.h"
#include "indexlib/util/Fp16Encoder.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, AttributeReferenceTest);

AttributeReferenceTest::AttributeReferenceTest() {}

AttributeReferenceTest::~AttributeReferenceTest() {}

void AttributeReferenceTest::CaseSetUp() {}

void AttributeReferenceTest::CaseTearDown() {}

void AttributeReferenceTest::TestSetAndGetCountedMultiValueAttr()
{
    size_t len = 50;
    char* pool = new char[len];
    StringView valueBuf(pool, len);
    CompressTypeOption compressType;
    // use random offset to read and write
    {
        // fixed string
        StringView value("abcd");
        size_t offset = 20;
        AttributeReferenceTyped<MultiChar> attrRef(autil::PackOffset::normalOffset(offset), "", compressType,
                                                   value.size());
        attrRef.SetValue((char*)valueBuf.data(), offset, value);

        MultiChar result;
        attrRef.GetValue(valueBuf.data(), result);
        string expected(value.data(), value.size());
        string actual(result.data(), value.size());
        ASSERT_EQ(expected, actual);
    }
    {
        // fixed multi int16
        size_t len = 4;
        uint16_t data[4] = {1, 2, 3, 4};
        MultiUInt16 countedData(data, 4);
        StringView value(countedData.getData(), (size_t)countedData.getDataSize());

        size_t offset = 18;
        AttributeReferenceTyped<MultiUInt16> attrRef(autil::PackOffset::normalOffset(offset), "", compressType, len);
        attrRef.SetValue((char*)valueBuf.data(), offset, value);

        MultiUInt16 result;
        attrRef.GetValue(valueBuf.data(), result);
        ASSERT_EQ(len, result.size());
        for (size_t i = 0; i < len; i++) {
            ASSERT_EQ(data[i], result[i]);
        }
    }

    delete[] pool;
}

void AttributeReferenceTest::TestSingleFloat()
{
    size_t len = 50;
    char* pool = new char[len];
    StringView valueBuf(pool, len);
    {
        CompressTypeOption compressType;
        // no compress
        size_t offset = 18;
        AttributeReferenceTyped<float> attrRef(autil::PackOffset::normalOffset(offset), "", compressType, 1);
        float temp = 99.111;
        StringView value((char*)&temp, sizeof(float));
        ASSERT_EQ(sizeof(float), attrRef.SetValue((char*)valueBuf.data(), offset, value));
        string valueStr;
        ASSERT_TRUE(attrRef.GetStrValue(valueBuf.data(), valueStr, NULL));
        ASSERT_EQ("99.111", valueStr);

        StringView data = attrRef.GetDataValue(valueBuf.data()).valueStr;
        float dataValue = *((float*)data.data());
        ASSERT_TRUE(abs(dataValue - temp) <= 0.001);

        float typedValue;
        ASSERT_TRUE(attrRef.GetValue(valueBuf.data(), typedValue));
        ASSERT_TRUE(abs(typedValue - temp) <= 0.001);
    }
    {
        // fp16
        CompressTypeOption compressType;
        ASSERT_TRUE(compressType.Init("fp16").IsOK());
        size_t offset = 18;
        AttributeReferenceTyped<float> attrRef(autil::PackOffset::normalOffset(offset), "", compressType, 1);
        float temp = 0.99111;
        int16_t testValue;
        Fp16Encoder::Encode(temp, (char*)&testValue);
        StringView value((char*)&testValue, sizeof(int16_t));
        ASSERT_EQ(sizeof(int16_t), attrRef.SetValue((char*)valueBuf.data(), offset, value));
        string valueStr;
        ASSERT_TRUE(attrRef.GetStrValue(valueBuf.data(), valueStr, NULL));
        ASSERT_EQ("0.990723", valueStr);

        StringView data = attrRef.GetDataValue(valueBuf.data()).valueStr;
        float dataValue = 0.0;
        ASSERT_EQ(1, Fp16Encoder::Decode(data, (char*)&dataValue));
        ASSERT_TRUE(abs(dataValue - temp) <= 0.001);

        float typedValue;
        ASSERT_TRUE(attrRef.GetValue(valueBuf.data(), typedValue));
        ASSERT_TRUE(abs(typedValue - temp) <= 0.001);
    }
    {
        // int8
        CompressTypeOption compressType;
        ASSERT_TRUE(compressType.Init("int8#1").IsOK());
        size_t offset = 18;
        AttributeReferenceTyped<float> attrRef(autil::PackOffset::normalOffset(offset), "", compressType, 1);
        float temp = -0.8;
        int8_t testValue;
        util::FloatInt8Encoder::Encode(1.0, temp, (char*)&testValue);
        StringView value((char*)&testValue, sizeof(int8_t));
        ASSERT_EQ(sizeof(int8_t), attrRef.SetValue((char*)valueBuf.data(), offset, value));
        string valueStr;
        ASSERT_TRUE(attrRef.GetStrValue(valueBuf.data(), valueStr, NULL));
        ASSERT_EQ("-0.80315", valueStr);

        StringView data = attrRef.GetDataValue(valueBuf.data()).valueStr;
        float dataValue = 0.0;
        ASSERT_EQ(1, util::FloatInt8Encoder::Decode(1.0, data, (char*)&dataValue));
        ASSERT_TRUE(abs(dataValue - temp) <= 0.01);

        float typedValue;
        ASSERT_TRUE(attrRef.GetValue(valueBuf.data(), typedValue));
        ASSERT_TRUE(abs(typedValue - temp) <= 0.01);
    }
    delete[] pool;
}

void AttributeReferenceTest::TestSetAndGetCompactMultiValue()
{
    std::vector<uint32_t> dataVec = {1, 3, 7};
    size_t validBufferLen = autil::MultiValueFormatter::calculateBufferLen(dataVec.size(), sizeof(uint32_t));
    char buffer[validBufferLen];
    autil::MultiValueFormatter::formatToBuffer(dataVec, buffer, sizeof(buffer));
    autil::StringView value(buffer, validBufferLen);

    // set value for only one var field
    {
        char attrBuf[50];
        CompressTypeOption compressType;
        auto offset = autil::PackOffset::impactOffset(20, 0, 1, true);
        AttributeReferenceTyped<MultiValueType<uint32_t>> attrRef(offset, "", compressType, -1);

        size_t cursor = 20;
        ASSERT_EQ(value.size(), attrRef.SetValue(attrBuf, cursor, value));

        MultiValueType<uint32_t> result;
        attrRef.GetValue(attrBuf, result);
        ASSERT_EQ(3, result.size());
        ASSERT_EQ(1, result[0]);
        ASSERT_EQ(3, result[1]);
        ASSERT_EQ(7, result[2]);

        auto dataValue = attrRef.GetDataValue(attrBuf);
        ASSERT_EQ(value.size(), dataValue.valueStr.size());
        ASSERT_TRUE(dataValue.hasCountInValueStr);
    }

    // set value for first field, has count
    {
        char attrBuf[128];
        CompressTypeOption compressType;
        auto offset = autil::PackOffset::impactOffset(20, 0, 4, true);
        attrBuf[20] = (char)1;
        AttributeReferenceTyped<MultiValueType<uint32_t>> attrRef(offset, "", compressType, -1);

        // [21:1byte] [22 ~ 24: 1 * 3 = 3 bytes]
        size_t cursor = 20 + sizeof(char) + 3;
        ASSERT_EQ(value.size(), attrRef.SetValue(attrBuf, cursor, value));

        MultiValueType<uint32_t> result;
        attrRef.GetValue(attrBuf, result);
        ASSERT_EQ(3, result.size());
        ASSERT_EQ(1, result[0]);
        ASSERT_EQ(3, result[1]);
        ASSERT_EQ(7, result[2]);

        auto dataValue = attrRef.GetDataValue(attrBuf);
        ASSERT_EQ(value.size(), dataValue.valueStr.size());
        ASSERT_TRUE(dataValue.hasCountInValueStr);
    }

    // set value for first field, without count
    {
        char attrBuf[128];
        CompressTypeOption compressType;
        auto offset = autil::PackOffset::impactOffset(20, 0, 4, false);
        attrBuf[20] = (char)2;
        AttributeReferenceTyped<MultiValueType<uint32_t>> attrRef(offset, "", compressType, -1);

        // [21:1byte] [22 ~ 24: 2 * 3 = 6 bytes]
        size_t cursor = 20 + sizeof(char) + 6;
        ASSERT_EQ(12, attrRef.SetValue(attrBuf, cursor, value));

        auto nextOffset = autil::PackOffset::impactOffset(20, 1, 4, false);
        autil::PackDataFormatter::setVarLenOffset(nextOffset, attrBuf, cursor + 12);

        MultiValueType<uint32_t> result;
        attrRef.GetValue(attrBuf, result);
        ASSERT_EQ(3, result.size());
        ASSERT_EQ(1, result[0]);
        ASSERT_EQ(3, result[1]);
        ASSERT_EQ(7, result[2]);

        auto dataValue = attrRef.GetDataValue(attrBuf);
        ASSERT_EQ(12, dataValue.valueStr.size());
        ASSERT_FALSE(dataValue.hasCountInValueStr);
        ASSERT_EQ(3, dataValue.valueCount);
    }

    // set value for last field, must has count
    {
        char attrBuf[128];
        CompressTypeOption compressType;
        auto offset = autil::PackOffset::impactOffset(20, 3, 4, true);
        attrBuf[20] = (char)1;
        AttributeReferenceTyped<MultiValueType<uint32_t>> attrRef(offset, "", compressType, -1);

        // [21:2byte] [22 ~ 24: 1 * 3 = 3 bytes]
        size_t cursor = 30;
        ASSERT_EQ(value.size(), attrRef.SetValue(attrBuf, cursor, value));

        MultiValueType<uint32_t> result;
        attrRef.GetValue(attrBuf, result);
        ASSERT_EQ(3, result.size());
        ASSERT_EQ(1, result[0]);
        ASSERT_EQ(3, result[1]);
        ASSERT_EQ(7, result[2]);

        auto dataValue = attrRef.GetDataValue(attrBuf);
        ASSERT_EQ(value.size(), dataValue.valueStr.size());
        ASSERT_TRUE(dataValue.hasCountInValueStr);
    }

    // set value for middle field with header
    {
        char attrBuf[128];
        CompressTypeOption compressType;
        auto offset = autil::PackOffset::impactOffset(20, 2, 4, true);
        attrBuf[20] = (char)2;

        // [21:2byte] [22 ~ 27: 2 * 3 = 6 bytes]
        AttributeReferenceTyped<MultiValueType<uint32_t>> attrRef(offset, "", compressType, -1);

        size_t cursor = 40;
        ASSERT_EQ(value.size(), attrRef.SetValue(attrBuf, cursor, value));

        MultiValueType<uint32_t> result;
        attrRef.GetValue(attrBuf, result);
        ASSERT_EQ(3, result.size());
        ASSERT_EQ(1, result[0]);
        ASSERT_EQ(3, result[1]);
        ASSERT_EQ(7, result[2]);

        auto dataValue = attrRef.GetDataValue(attrBuf);
        ASSERT_EQ(value.size(), dataValue.valueStr.size());
        ASSERT_TRUE(dataValue.hasCountInValueStr);
    }

    // set value for middle field without header
    {
        char attrBuf[128];
        CompressTypeOption compressType;
        auto offset = autil::PackOffset::impactOffset(20, 2, 4, false);
        attrBuf[20] = (char)4;

        // [21:1byte] [22 ~ 33: 4 * 3 = 12 bytes]
        AttributeReferenceTyped<MultiValueType<uint32_t>> attrRef(offset, "", compressType, -1);

        size_t cursor = 40;
        ASSERT_EQ(12, attrRef.SetValue(attrBuf, cursor, value));

        auto nextOffset = autil::PackOffset::impactOffset(20, 3, 4, true);
        autil::PackDataFormatter::setVarLenOffset(nextOffset, attrBuf, cursor + 12);

        MultiValueType<uint32_t> result;
        attrRef.GetValue(attrBuf, result);
        ASSERT_EQ(3, result.size());
        ASSERT_EQ(1, result[0]);
        ASSERT_EQ(3, result[1]);
        ASSERT_EQ(7, result[2]);

        auto dataValue = attrRef.GetDataValue(attrBuf);
        ASSERT_EQ(12, dataValue.valueStr.size());
        ASSERT_FALSE(dataValue.hasCountInValueStr);
        ASSERT_EQ(3, dataValue.valueCount);
    }
}

void AttributeReferenceTest::TestReferenceOffset()
{
    {
        // test normal offset
        auto offset = autil::PackOffset::normalOffset(18);
        ASSERT_EQ(18, offset.toUInt64());

        autil::PackOffset newOffset;
        newOffset.fromUInt64(18);
        ASSERT_FALSE(newOffset.isImpactFormat());
        ASSERT_TRUE(newOffset.needVarLenHeader());
        ASSERT_EQ(18, newOffset.getOffset());
    }
    {
        // test impact value
        auto offset = autil::PackOffset::impactOffset(31, 2, 6, false);
        uint64_t value = offset.toUInt64();

        autil::PackOffset newOffset;
        newOffset.fromUInt64(value);
        ASSERT_TRUE(newOffset.isImpactFormat());
        ASSERT_FALSE(newOffset.needVarLenHeader());
        ASSERT_EQ(31, newOffset.getOffset());

        ASSERT_EQ(2, newOffset.getVarLenFieldIdx());
        ASSERT_EQ(6, newOffset.getVarLenFieldNum());
    }

    autil::PackOffset offset;
    offset.fromUInt64(4611686018427387904);
    cout << "impact:" << autil::StringUtil::toString(offset.isImpactFormat()) << ","
         << "needheader:" << autil::StringUtil::toString(offset.needVarLenHeader()) << ","
         << "offset:" << offset.getOffset() << ","
         << "idx:" << offset.getVarLenFieldIdx() << ",num:" << offset.getVarLenFieldNum() << endl;
}

}} // namespace indexlib::common
