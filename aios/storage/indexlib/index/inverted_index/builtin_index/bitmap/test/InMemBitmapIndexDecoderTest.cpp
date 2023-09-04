#include "indexlib/index/inverted_index/builtin_index/bitmap/InMemBitmapIndexDecoder.h"

#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::index;

namespace indexlibv2::index {
class InMemBitmapIndexDecoderTest : public TESTBASE
{
public:
    void TestInit();
};

void InMemBitmapIndexDecoderTest::TestInit()
{
    SCOPED_TRACE("Failed");

    BitmapPostingWriter postingWriter;
    for (size_t i = 0; i < 10; ++i) {
        postingWriter.AddPosition(0, 0, 0);
    }
    postingWriter.EndDocument(10000, 0);

    InMemBitmapIndexDecoder bitmapDecoder;
    bitmapDecoder.Init(&postingWriter);

    const TermMeta* tm = bitmapDecoder.GetTermMeta();
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
TEST_F(InMemBitmapIndexDecoderTest, TestInit) { TestInit(); }
} // namespace indexlibv2::index
