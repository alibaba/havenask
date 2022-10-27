#include "indexlib/index/normal/inverted_index/test/dict_inline_encoder_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DictInlineEncoderTest);

DictInlineEncoderTest::DictInlineEncoderTest()
{
}

DictInlineEncoderTest::~DictInlineEncoderTest()
{
}

void DictInlineEncoderTest::CaseSetUp()
{
}

void DictInlineEncoderTest::CaseTearDown()
{
}

void DictInlineEncoderTest::TestEncode()
{
    {
        uint32_t data[] = {1, 2, 3, 4, 5};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));
        
        uint64_t result;
        ASSERT_TRUE(DictInlineEncoder::Encode(vec, result));
    }

    {
        uint32_t data[] = {128, 128, 256, 4, 5};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));
        
        uint64_t result;
        ASSERT_FALSE(DictInlineEncoder::Encode(vec, result));
    }
}


void DictInlineEncoderTest::TestCalculateCompressedSize()
{
    {
        uint32_t data[] = {1, 2, 3, 4, 5};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));
        
        ASSERT_EQ((uint32_t)5, DictInlineEncoder::CalculateCompressedSize(vec));
    }

    {
        uint32_t data[] = {128, 128, 256, 4, 5};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));
        
        ASSERT_EQ((uint32_t)8, DictInlineEncoder::CalculateCompressedSize(vec));
    }
}

IE_NAMESPACE_END(index);

