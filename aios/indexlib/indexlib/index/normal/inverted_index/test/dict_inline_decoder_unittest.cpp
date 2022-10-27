#include "indexlib/index/normal/inverted_index/test/dict_inline_decoder_unittest.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_encoder.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DictInlineDecoderTest);

DictInlineDecoderTest::DictInlineDecoderTest()
{
}

DictInlineDecoderTest::~DictInlineDecoderTest()
{
}

void DictInlineDecoderTest::CaseSetUp()
{
}

void DictInlineDecoderTest::CaseTearDown()
{
}

void DictInlineDecoderTest::TestSimpleProcess()
{
    uint32_t data[] = {1, 2, 3, 5};
    vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));

    uint64_t result = 0;
    ASSERT_TRUE(DictInlineEncoder::Encode(vec, result));    

    uint32_t targetVec[vec.size()];
    DictInlineDecoder::Decode(result, vec.size(), targetVec);
    for (size_t i = 0; i < vec.size(); i++)
    {
        ASSERT_EQ(vec[i], targetVec[i]);
    }
}

IE_NAMESPACE_END(index);

