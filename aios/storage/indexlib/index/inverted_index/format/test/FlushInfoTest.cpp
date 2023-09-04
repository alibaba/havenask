#include "indexlib/index/inverted_index/format/FlushInfo.h"

#include "unittest/unittest.h"

namespace indexlib::index {

class FlushInfoTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(FlushInfoTest, TestSimpleProcess)
{
    uint32_t flushLengh = 100;
    bool isValid = true;
    uint8_t compressMode = SHORT_LIST_COMPRESS_MODE;
    uint32_t flushCount = 240;

    FlushInfo flushInfo;
    flushInfo.SetFlushLength(flushLengh);
    ASSERT_EQ(flushLengh, flushInfo.GetFlushLength());

    flushInfo.SetIsValidShortBuffer(isValid);
    ASSERT_EQ(isValid, flushInfo.IsValidShortBuffer());

    flushInfo.SetCompressMode(compressMode);
    ASSERT_EQ(compressMode, flushInfo.GetCompressMode());

    flushInfo.SetFlushCount(flushCount);
    ASSERT_EQ(flushCount, flushInfo.GetFlushCount());
}

} // namespace indexlib::index
