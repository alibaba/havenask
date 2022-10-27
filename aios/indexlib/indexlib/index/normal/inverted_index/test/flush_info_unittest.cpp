#include "indexlib/index/normal/inverted_index/test/flush_info_unittest.h"

using namespace std;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, FlushInfoTest);

FlushInfoTest::FlushInfoTest()
{
}

FlushInfoTest::~FlushInfoTest()
{
}

void FlushInfoTest::CaseSetUp()
{
}

void FlushInfoTest::CaseTearDown()
{
}

void FlushInfoTest::TestSimpleProcess()
{
    uint32_t flushLengh = 100;
    bool isValid = true;
    uint8_t compressMode = SHORT_LIST_COMPRESS_MODE;
    uint32_t flushCount = 240;

    FlushInfo flushInfo;
    flushInfo.SetFlushLength(flushLengh);
    INDEXLIB_TEST_EQUAL(flushLengh, flushInfo.GetFlushLength());

    flushInfo.SetIsValidShortBuffer(isValid);
    INDEXLIB_TEST_EQUAL(isValid, flushInfo.IsValidShortBuffer());

    flushInfo.SetCompressMode(compressMode);
    INDEXLIB_TEST_EQUAL(compressMode, flushInfo.GetCompressMode());

    flushInfo.SetFlushCount(flushCount);
    INDEXLIB_TEST_EQUAL(flushCount, flushInfo.GetFlushCount());
}

IE_NAMESPACE_END(index);

