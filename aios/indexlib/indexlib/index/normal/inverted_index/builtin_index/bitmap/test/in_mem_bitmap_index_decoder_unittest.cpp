#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/test/in_mem_bitmap_index_decoder_unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"

using namespace std;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemBitmapIndexDecoderTest);

InMemBitmapIndexDecoderTest::InMemBitmapIndexDecoderTest()
{
}

InMemBitmapIndexDecoderTest::~InMemBitmapIndexDecoderTest()
{
}

void InMemBitmapIndexDecoderTest::CaseSetUp()
{
}

void InMemBitmapIndexDecoderTest::CaseTearDown()
{
}

void InMemBitmapIndexDecoderTest::TestInit()
{
    SCOPED_TRACE("Failed");

    BitmapPostingWriter postingWriter;
    for (size_t i = 0; i < 10; ++i)
    {
        postingWriter.AddPosition(0, 0, 0);
    }
    postingWriter.EndDocument(10000, 0);

    InMemBitmapIndexDecoder bitmapDecoder;
    bitmapDecoder.Init(&postingWriter);

    const TermMeta *tm = bitmapDecoder.GetTermMeta();
    ASSERT_EQ((df_t)1, tm->GetDocFreq());
    ASSERT_EQ((tf_t)10, tm->GetTotalTermFreq());
    ASSERT_EQ((termpayload_t)0, tm->GetPayload());
    ASSERT_EQ((uint32_t)10001, bitmapDecoder.GetBitmapItemCount());

    uint32_t* bitmapData = bitmapDecoder.GetBitmapData();
    postingWriter.EndDocument(128 * 1024 + 1, 0);
    bitmapDecoder.Init(&postingWriter);

    uint32_t* newBitmapData = bitmapDecoder.GetBitmapData();
    ASSERT_EQ((df_t)2, bitmapDecoder.GetTermMeta()->GetDocFreq());
    ASSERT_TRUE(bitmapData != newBitmapData);
}

IE_NAMESPACE_END(index);

