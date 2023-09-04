#include "indexlib/common/field_format/attribute/test/compress_float_attribute_convertor_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/util/BlockFpEncoder.h"
#include "indexlib/util/FloatInt8Encoder.h"
#include "indexlib/util/Fp16Encoder.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, CompressFloatAttributeConvertorTest);

CompressFloatAttributeConvertorTest::CompressFloatAttributeConvertorTest() {}

CompressFloatAttributeConvertorTest::~CompressFloatAttributeConvertorTest() {}

void CompressFloatAttributeConvertorTest::CaseSetUp() {}

void CompressFloatAttributeConvertorTest::CaseTearDown() {}

void CompressFloatAttributeConvertorTest::TestSimpleProcess()
{
    float dataArray[] = {1.1, -2.003, 0.776, 0.001};
    vector<float> data(dataArray, dataArray + sizeof(dataArray) / sizeof(float));
    CompressTypeOption blockFp;
    ASSERT_TRUE(blockFp.Init("block_fp").IsOK());
    InnerTestEncode(data, true, data.size(), blockFp);
    InnerTestEncode(data, false, data.size(), blockFp);

    InnerTestEncode(data, true, data.size() + 2, blockFp);
    InnerTestEncode(data, false, data.size() - 1, blockFp);

    CompressTypeOption fp16;
    ASSERT_TRUE(fp16.Init("fp16").IsOK());
    InnerTestEncode(data, true, data.size(), fp16);
    InnerTestEncode(data, false, data.size(), fp16);

    InnerTestEncode(data, true, data.size() + 2, fp16);
    InnerTestEncode(data, false, data.size() - 1, fp16);

    CompressTypeOption int8;
    ASSERT_TRUE(int8.Init("int8#2.5").IsOK());
    InnerTestEncode(data, true, data.size(), int8);
    InnerTestEncode(data, false, data.size(), int8);

    InnerTestEncode(data, true, data.size() + 2, int8);
    InnerTestEncode(data, false, data.size() - 1, int8);
}

void CompressFloatAttributeConvertorTest::InnerTestEncode(const std::vector<float>& data, bool needHash,
                                                          int32_t fixedValueCount,
                                                          CompressTypeOption compressTypeOption)
{
    string input;
    for (size_t i = 0; i < data.size(); ++i) {
        string value = autil::StringUtil::toString<float>(data[i]);
        input += value;
        input += MULTI_VALUE_SEPARATOR;
    }
    CompressFloatAttributeConvertor convertor(compressTypeOption, needHash, "", fixedValueCount);

    string resultStr = convertor.Encode(input);

    Pool pool;
    // test EncodeFromAttrValueMeta
    StringView encodedStr1(resultStr.data(), resultStr.size());
    AttrValueMeta attrValueMeta = convertor.Decode(encodedStr1);
    StringView encodedStr2 = convertor.EncodeFromAttrValueMeta(attrValueMeta, &pool);
    ASSERT_EQ(encodedStr1, encodedStr2);

    uint64_t expectedKey = (uint64_t)-1;
    if (needHash) {
        expectedKey = util::HashString::Hash(input.data(), input.size());
    }
    INDEXLIB_TEST_EQUAL(expectedKey, attrValueMeta.hashKey);

    StringView encodedValues = attrValueMeta.data;
    vector<float> outVec;
    outVec.resize(fixedValueCount);

    typedef int32_t (*DecodeFunc)(const StringView&, char*, size_t);
    auto checkValue = [&data, &fixedValueCount](DecodeFunc decodeFunc, const StringView& input, vector<float>& outVec,
                                                float expectedPrecision) {
        int32_t valueCount = decodeFunc(input, (char*)outVec.data(), outVec.size() * sizeof(float));
        ASSERT_EQ(valueCount, fixedValueCount);
        for (int32_t i = 0; i < fixedValueCount; i++) {
            if (i >= (int32_t)data.size()) {
                ASSERT_EQ(0, outVec[i]);
            } else {
                ASSERT_TRUE(abs(data[i] - outVec[i]) <= expectedPrecision) << data[i] << "," << outVec[i];
            }
        }
    };
    if (compressTypeOption.HasBlockFpEncodeCompress()) {
        checkValue(&BlockFpEncoder::Decode, encodedValues, outVec, 0.0001);
        ASSERT_EQ(encodedValues.size(), BlockFpEncoder::GetEncodeBytesLen(fixedValueCount));
    } else if (compressTypeOption.HasFp16EncodeCompress()) {
        checkValue(&Fp16Encoder::Decode, encodedValues, outVec, 0.01);
        ASSERT_EQ(encodedValues.size(), Fp16Encoder::GetEncodeBytesLen(fixedValueCount));
    } else if (compressTypeOption.HasInt8EncodeCompress()) {
        int32_t valueCount = FloatInt8Encoder::Decode(compressTypeOption.GetInt8AbsMax(), encodedValues,
                                                      (char*)outVec.data(), outVec.size() * sizeof(float));
        ASSERT_EQ(valueCount, fixedValueCount);
        for (int32_t i = 0; i < fixedValueCount; i++) {
            if (i >= (int32_t)data.size()) {
                ASSERT_EQ(0, outVec[i]);
            } else {
                ASSERT_TRUE(abs(data[i] - outVec[i]) <= 0.01) << data[i] << "," << outVec[i];
            }
        }
        ASSERT_EQ(encodedValues.size(), FloatInt8Encoder::GetEncodeBytesLen(fixedValueCount));
    }
}
}} // namespace indexlib::common
