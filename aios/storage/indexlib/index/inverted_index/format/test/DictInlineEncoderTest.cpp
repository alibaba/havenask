#include "indexlib/index/inverted_index/format/DictInlineEncoder.h"

#include "unittest/unittest.h"

namespace indexlib::index {

class DictInlineEncoderTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(DictInlineEncoderTest, testEncode)
{
    {
        uint32_t data[] = {1, 2, 3, 4, 5};
        std::vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));

        uint64_t result;
        ASSERT_TRUE(DictInlineEncoder::Encode(vec, result));
    }

    {
        uint32_t data[] = {128, 128, 256, 4, 5};
        std::vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));

        uint64_t result;
        ASSERT_FALSE(DictInlineEncoder::Encode(vec, result));
    }
}

TEST_F(DictInlineEncoderTest, testCalculateCompressedSize)
{
    {
        uint32_t data[] = {1, 2, 3, 4, 5};
        std::vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));

        ASSERT_EQ((uint32_t)5, DictInlineEncoder::CalculateCompressedSize(vec));
    }

    {
        uint32_t data[] = {128, 128, 256, 4, 5};
        std::vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));

        ASSERT_EQ((uint32_t)8, DictInlineEncoder::CalculateCompressedSize(vec));
    }
}

} // namespace indexlib::index
