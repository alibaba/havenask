#include "indexlib/index/common/numeric_compress/EquivalentCompressTraits.h"

#include "unittest/unittest.h"

namespace indexlib::index {

class EquivalentCompressTraitsTest : public TESTBASE
{
};
TEST_F(EquivalentCompressTraitsTest, TestZigZagEncode)
{
    {
        uint8_t value = 3;
        uint8_t encodeValue = EquivalentCompressTraits<uint8_t>::ZigZagEncoder::Encode(value);

        ASSERT_EQ(value, encodeValue);

        uint8_t decodeValue = EquivalentCompressTraits<uint8_t>::ZigZagEncoder::Decode(encodeValue);

        ASSERT_EQ(value, decodeValue);
    }
    {
        int8_t value = -4;
        uint8_t encodeValue = EquivalentCompressTraits<int8_t>::ZigZagEncoder::Encode(value);

        int8_t decodeValue = EquivalentCompressTraits<int8_t>::ZigZagEncoder::Decode(encodeValue);

        ASSERT_EQ(value, decodeValue);
    }
    {
        int16_t value = -4381;
        uint16_t encodeValue = EquivalentCompressTraits<int16_t>::ZigZagEncoder::Encode(value);

        int16_t decodeValue = EquivalentCompressTraits<int16_t>::ZigZagEncoder::Decode(encodeValue);

        ASSERT_EQ(value, decodeValue);
    }
    {
        int32_t value = -742913;
        uint32_t encodeValue = EquivalentCompressTraits<int32_t>::ZigZagEncoder::Encode(value);

        int32_t decodeValue = EquivalentCompressTraits<int32_t>::ZigZagEncoder::Decode(encodeValue);

        ASSERT_EQ(value, decodeValue);
    }

    {
        int64_t value = -438183713;
        uint64_t encodeValue = EquivalentCompressTraits<int64_t>::ZigZagEncoder::Encode(value);

        int64_t decodeValue = EquivalentCompressTraits<int64_t>::ZigZagEncoder::Decode(encodeValue);

        ASSERT_EQ(value, decodeValue);
    }
}
} // namespace indexlib::index
