#include "indexlib/index/inverted_index/format/DictInlineDecoder.h"

#include "indexlib/index/inverted_index/format/DictInlineEncoder.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DictInlineDecoderTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(DictInlineDecoderTest, testSimpleProcess)
{
    uint32_t data[] = {1, 2, 3, 5};
    std::vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));

    uint64_t result = 0;
    ASSERT_TRUE(DictInlineEncoder::Encode(vec, result));

    uint32_t targetVec[vec.size()];
    DictInlineDecoder::Decode(result, vec.size(), targetVec);
    for (size_t i = 0; i < vec.size(); i++) {
        ASSERT_EQ(vec[i], targetVec[i]);
    }

    docid_t startDocid = 12736;
    df_t df = 2716235;
    InlinePostingValueWithOrder inlinePostingValueWithOrder;
    ASSERT_TRUE(DictInlineEncoder::EncodeContinuousDocId(startDocid, df, false, inlinePostingValueWithOrder));

    docid_t targetStartDocid;
    df_t targetDf;
    DictInlineDecoder::DecodeContinuousDocId(inlinePostingValueWithOrder, targetStartDocid, targetDf);
    ASSERT_EQ(startDocid, targetStartDocid);
    ASSERT_EQ(df, targetDf);
}

TEST_F(DictInlineDecoderTest, testForLongDf)
{
    docid_t startDocid = 0;
    df_t df = 1 << 30;
    InlinePostingValueWithOrder inlinePostingValueWithOrder;
    ASSERT_FALSE(DictInlineEncoder::EncodeContinuousDocId(startDocid, df, false, inlinePostingValueWithOrder));

    ASSERT_TRUE(DictInlineEncoder::EncodeContinuousDocId(startDocid, df, true, inlinePostingValueWithOrder));
    ASSERT_FALSE(inlinePostingValueWithOrder.first /*dffirst*/);
    docid_t targetStartDocid;
    df_t targetDf;
    DictInlineDecoder::DecodeContinuousDocId(inlinePostingValueWithOrder, targetStartDocid, targetDf);
    ASSERT_EQ(startDocid, targetStartDocid);
    ASSERT_EQ(df, targetDf);

    startDocid = 1 << 30;
    ASSERT_FALSE(DictInlineEncoder::EncodeContinuousDocId(startDocid, df, false, inlinePostingValueWithOrder));
    ASSERT_FALSE(DictInlineEncoder::EncodeContinuousDocId(startDocid, df, true, inlinePostingValueWithOrder));
}

} // namespace indexlib::index
