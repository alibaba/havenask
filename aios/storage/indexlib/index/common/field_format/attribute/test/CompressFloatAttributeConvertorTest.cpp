#include "indexlib/index/common/field_format/attribute/CompressFloatAttributeConvertor.h"

#include "autil/StringUtil.h"
#include "indexlib/util/BlockFpEncoder.h"
#include "indexlib/util/FloatInt8Encoder.h"
#include "indexlib/util/Fp16Encoder.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class CompressFloatAttributeConvertorTest : public TESTBASE
{
public:
    CompressFloatAttributeConvertorTest() = default;
    ~CompressFloatAttributeConvertorTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    void InnerTestEncode(const std::vector<float>& data, bool needHash, int32_t fixedValueCount,
                         indexlib::config::CompressTypeOption compressTypeOption);
};

TEST_F(CompressFloatAttributeConvertorTest, TestSimpleProcess)
{
    float dataArray[] = {1.1, -2.003, 0.776, 0.001};
    std::vector<float> data(dataArray, dataArray + sizeof(dataArray) / sizeof(float));
    indexlib::config::CompressTypeOption blockFp;
    ASSERT_TRUE(blockFp.Init("block_fp").IsOK());
    InnerTestEncode(data, true, data.size(), blockFp);
    InnerTestEncode(data, false, data.size(), blockFp);

    InnerTestEncode(data, true, data.size() + 2, blockFp);
    InnerTestEncode(data, false, data.size() - 1, blockFp);

    indexlib::config::CompressTypeOption fp16;
    ASSERT_TRUE(fp16.Init("fp16").IsOK());
    InnerTestEncode(data, true, data.size(), fp16);
    InnerTestEncode(data, false, data.size(), fp16);

    InnerTestEncode(data, true, data.size() + 2, fp16);
    InnerTestEncode(data, false, data.size() - 1, fp16);

    indexlib::config::CompressTypeOption int8;
    ASSERT_TRUE(int8.Init("int8#2.5").IsOK());
    InnerTestEncode(data, true, data.size(), int8);
    InnerTestEncode(data, false, data.size(), int8);

    InnerTestEncode(data, true, data.size() + 2, int8);
    InnerTestEncode(data, false, data.size() - 1, int8);
}

void CompressFloatAttributeConvertorTest::InnerTestEncode(const std::vector<float>& data, bool needHash,
                                                          int32_t fixedValueCount,
                                                          indexlib::config::CompressTypeOption compressTypeOption)
{
    std::string input;
    for (size_t i = 0; i < data.size(); ++i) {
        std::string value = autil::StringUtil::toString<float>(data[i]);
        input += value;
        input += autil::MULTI_VALUE_DELIMITER;
    }
    CompressFloatAttributeConvertor convertor(compressTypeOption, needHash, "", fixedValueCount,
                                              INDEXLIB_MULTI_VALUE_SEPARATOR_STR);

    std::string resultStr = convertor.Encode(input);

    autil::mem_pool::Pool pool;
    // test EncodeFromAttrValueMeta
    autil::StringView encodedStr1(resultStr.data(), resultStr.size());
    AttrValueMeta attrValueMeta = convertor.Decode(encodedStr1);
    autil::StringView encodedStr2 = convertor.EncodeFromAttrValueMeta(attrValueMeta, &pool);
    ASSERT_EQ(encodedStr1, encodedStr2);

    uint64_t expectedKey = (uint64_t)-1;
    if (needHash) {
        expectedKey = indexlib::util::HashString::Hash(input.data(), input.size());
    }
    ASSERT_EQ(expectedKey, attrValueMeta.hashKey);

    autil::StringView encodedValues = attrValueMeta.data;
    std::vector<float> outVec;
    outVec.resize(fixedValueCount);

    typedef int32_t (*DecodeFunc)(const autil::StringView&, char*, size_t);
    auto checkValue = [&data, &fixedValueCount](DecodeFunc decodeFunc, const autil::StringView& input,
                                                std::vector<float>& outVec, float expectedPrecision) {
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
        checkValue(&indexlib::util::BlockFpEncoder::Decode, encodedValues, outVec, 0.0001);
        ASSERT_EQ(encodedValues.size(), indexlib::util::BlockFpEncoder::GetEncodeBytesLen(fixedValueCount));
    } else if (compressTypeOption.HasFp16EncodeCompress()) {
        checkValue(&indexlib::util::Fp16Encoder::Decode, encodedValues, outVec, 0.01);
        ASSERT_EQ(encodedValues.size(), indexlib::util::Fp16Encoder::GetEncodeBytesLen(fixedValueCount));
    } else if (compressTypeOption.HasInt8EncodeCompress()) {
        int32_t valueCount = indexlib::util::FloatInt8Encoder::Decode(
            compressTypeOption.GetInt8AbsMax(), encodedValues, (char*)outVec.data(), outVec.size() * sizeof(float));
        ASSERT_EQ(valueCount, fixedValueCount);
        for (int32_t i = 0; i < fixedValueCount; i++) {
            if (i >= (int32_t)data.size()) {
                ASSERT_EQ(0, outVec[i]);
            } else {
                ASSERT_TRUE(abs(data[i] - outVec[i]) <= 0.01) << data[i] << "," << outVec[i];
            }
        }
        ASSERT_EQ(encodedValues.size(), indexlib::util::FloatInt8Encoder::GetEncodeBytesLen(fixedValueCount));
    }
}
} // namespace indexlibv2::index
