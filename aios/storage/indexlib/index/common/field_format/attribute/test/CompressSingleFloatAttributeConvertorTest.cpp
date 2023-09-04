#include "indexlib/index/common/field_format/attribute/CompressSingleFloatAttributeConvertor.h"

#include "autil/StringUtil.h"
#include "indexlib/util/BlockFpEncoder.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {
class CompressSingleFloatAttributeConvertorTest : public TESTBASE
{
public:
    CompressSingleFloatAttributeConvertorTest() = default;
    ~CompressSingleFloatAttributeConvertorTest() = default;
    void setUp() override {}
    void tearDown() override {}

    template <typename T>
    void InnerTestEncode(std::string compressType, float value);
};

template <typename T>
void CompressSingleFloatAttributeConvertorTest::InnerTestEncode(std::string compressType, float value)
{
    indexlib::config::CompressTypeOption type;
    ASSERT_TRUE(type.Init(compressType).IsOK());
    std::string input = autil::StringUtil::toString<float>(value);
    CompressSingleFloatAttributeConvertor<T> convertor(type, "testfield");
    std::string resultStr = convertor.Encode(input);
    ASSERT_EQ(sizeof(T), resultStr.size());

    autil::mem_pool::Pool pool;
    autil::StringView encodedStr1(resultStr.data(), resultStr.size());
    AttrValueMeta attrValueMeta = convertor.Decode(encodedStr1);
    autil::StringView encodedStr2 = convertor.EncodeFromAttrValueMeta(attrValueMeta, &pool);
    EXPECT_EQ(encodedStr1, encodedStr2);

    float output = 0.0;
    int32_t count = 0;
    if (type.HasFp16EncodeCompress()) {
        count = indexlib::util::Fp16Encoder::Decode(encodedStr2, (char*)&output);
    } else if (type.HasInt8EncodeCompress()) {
        count = indexlib::util::FloatInt8Encoder::Decode(type.GetInt8AbsMax(), encodedStr2, (char*)&output);
    }
    ASSERT_EQ(1, count);
    ASSERT_TRUE(abs(value - output) <= 0.02) << "value: " << value << " output: " << output;
}

TEST_F(CompressSingleFloatAttributeConvertorTest, TestSimpleProcess)
{
    // TODO add encode error test
    InnerTestEncode<int16_t>("fp16", 1.1);
    InnerTestEncode<int8_t>("int8#2", 1.3);
}
} // namespace indexlibv2::index
