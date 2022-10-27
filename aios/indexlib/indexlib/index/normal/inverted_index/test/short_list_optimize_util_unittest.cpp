#include "indexlib/index/normal/inverted_index/test/short_list_optimize_util_unittest.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"

using namespace std;


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, ShortListOptimizeUtilTest);

ShortListOptimizeUtilTest::ShortListOptimizeUtilTest()
{
}

ShortListOptimizeUtilTest::~ShortListOptimizeUtilTest()
{
}

void ShortListOptimizeUtilTest::SetUp()
{
}

void ShortListOptimizeUtilTest::TearDown()
{
}

void ShortListOptimizeUtilTest::TestSimpleProcess()
{
    //TODO: more boundary ut
    INDEXLIB_TEST_EQUAL(SHORT_LIST_COMPRESS_MODE,
                        ShortListOptimizeUtil::GetPosListCompressMode(1));
    INDEXLIB_TEST_EQUAL(PFOR_DELTA_COMPRESS_MODE,
                        ShortListOptimizeUtil::GetPosListCompressMode(100));

    INDEXLIB_TEST_EQUAL(SHORT_LIST_COMPRESS_MODE,
                        ShortListOptimizeUtil::GetDocListCompressMode(1, PFOR_DELTA_COMPRESS_MODE));
    INDEXLIB_TEST_EQUAL(PFOR_DELTA_COMPRESS_MODE,
                        ShortListOptimizeUtil::GetDocListCompressMode(100, PFOR_DELTA_COMPRESS_MODE));

    INDEXLIB_TEST_EQUAL(0x0F, ShortListOptimizeUtil::GetCompressMode(1, 1, PFOR_DELTA_COMPRESS_MODE));
    INDEXLIB_TEST_EQUAL(0x00, ShortListOptimizeUtil::GetCompressMode(
                    100, 100, PFOR_DELTA_COMPRESS_MODE));
}

void ShortListOptimizeUtilTest::TestIsDictInlineCompressMode()
{
    {
        dictvalue_t dictInlineValue = 
            ShortListOptimizeUtil::CreateDictInlineValue(123);
        INDEXLIB_TEST_TRUE(ShortListOptimizeUtil::IsDictInlineCompressMode(
                        dictInlineValue));
    }
    {
        dictvalue_t commonDictValue = ShortListOptimizeUtil::CreateDictValue(
                PFOR_DELTA_COMPRESS_MODE, 123);
        INDEXLIB_TEST_TRUE(!ShortListOptimizeUtil::IsDictInlineCompressMode(
                        commonDictValue));
    }
}

IE_NAMESPACE_END(index);

