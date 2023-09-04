#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"

#include "unittest/unittest.h"

namespace indexlibv2::index {

class MultiValueAttributeFormatterTest : public TESTBASE
{
public:
    MultiValueAttributeFormatterTest() = default;
    ~MultiValueAttributeFormatterTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    void InnerTestEncodeAndDecodeCount(uint32_t countValue);
};

TEST_F(MultiValueAttributeFormatterTest, TestSimpleProcess)
{
    InnerTestEncodeAndDecodeCount(0);
    InnerTestEncodeAndDecodeCount(12);
    InnerTestEncodeAndDecodeCount(63);
    InnerTestEncodeAndDecodeCount(64);
    InnerTestEncodeAndDecodeCount(255);
    InnerTestEncodeAndDecodeCount(16383);
    InnerTestEncodeAndDecodeCount(16384);
    InnerTestEncodeAndDecodeCount(12351461);
    InnerTestEncodeAndDecodeCount(1073741823);
    InnerTestEncodeAndDecodeCount(MultiValueAttributeFormatter::MULTI_VALUE_NULL_FIELD_VALUE_COUNT);
}

void MultiValueAttributeFormatterTest::InnerTestEncodeAndDecodeCount(uint32_t countValue)
{
    char buffer[4] = {0};

    size_t encodeLen = MultiValueAttributeFormatter::EncodeCount(countValue, buffer, 4);
    size_t decodeLen = 0;
    bool isNull = false;
    uint32_t decodeValue = MultiValueAttributeFormatter::DecodeCount(buffer, decodeLen, isNull);
    ASSERT_EQ(decodeLen, encodeLen);
    if (!isNull) {
        ASSERT_EQ(decodeValue, countValue);
    }

    if (countValue == MultiValueAttributeFormatter::MULTI_VALUE_NULL_FIELD_VALUE_COUNT) {
        ASSERT_TRUE(isNull);
    } else {
        ASSERT_FALSE(isNull);
    }
}
} // namespace indexlibv2::index
