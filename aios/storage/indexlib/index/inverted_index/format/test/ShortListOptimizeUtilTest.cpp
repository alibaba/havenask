#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"

#include "unittest/unittest.h"

namespace indexlib::index {

class ShortListOptimizeUtilTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(ShortListOptimizeUtilTest, testSimpleProcess)
{
    // TODO: more boundary ut
    ASSERT_EQ(SHORT_LIST_COMPRESS_MODE, ShortListOptimizeUtil::GetPosListCompressMode(1));
    ASSERT_EQ(PFOR_DELTA_COMPRESS_MODE, ShortListOptimizeUtil::GetPosListCompressMode(100));

    ASSERT_EQ(SHORT_LIST_COMPRESS_MODE, ShortListOptimizeUtil::GetDocListCompressMode(1, PFOR_DELTA_COMPRESS_MODE));
    ASSERT_EQ(PFOR_DELTA_COMPRESS_MODE, ShortListOptimizeUtil::GetDocListCompressMode(100, PFOR_DELTA_COMPRESS_MODE));

    ASSERT_EQ(0x0F, ShortListOptimizeUtil::GetCompressMode(1, 1, PFOR_DELTA_COMPRESS_MODE));
    ASSERT_EQ(0x00, ShortListOptimizeUtil::GetCompressMode(100, 100, PFOR_DELTA_COMPRESS_MODE));
}

TEST_F(ShortListOptimizeUtilTest, testIsDictInlineCompressMode)
{
    bool isDocList = false;
    bool dfFirst = true;
    {
        dictvalue_t dictInlineValue = ShortListOptimizeUtil::CreateDictInlineValue(123, isDocList, true);
        ASSERT_TRUE(ShortListOptimizeUtil::IsDictInlineCompressMode(dictInlineValue, isDocList, dfFirst));
    }
    {
        dictvalue_t commonDictValue = ShortListOptimizeUtil::CreateDictValue(PFOR_DELTA_COMPRESS_MODE, 123);
        ASSERT_TRUE(!ShortListOptimizeUtil::IsDictInlineCompressMode(commonDictValue, isDocList, dfFirst));
    }

    {
        isDocList = false;
        dfFirst = true;
        dictvalue_t dictInlineValue = ShortListOptimizeUtil::CreateDictInlineValue(123, true, false);
        ASSERT_TRUE(ShortListOptimizeUtil::IsDictInlineCompressMode(dictInlineValue, isDocList, dfFirst));
        ASSERT_TRUE(isDocList);
        ASSERT_FALSE(dfFirst);
    }
}

} // namespace indexlib::index
