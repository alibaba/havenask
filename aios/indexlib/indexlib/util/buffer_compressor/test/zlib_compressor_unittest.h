#ifndef __INDEXLIB_ZLIBCOMPRESSORTEST_H
#define __INDEXLIB_ZLIBCOMPRESSORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/buffer_compressor/test/buffer_compressor_unittest.h"

IE_NAMESPACE_BEGIN(util);

class ZlibCompressorTest : public BufferCompressorTest
{
public:
    ZlibCompressorTest();
    ~ZlibCompressorTest();

    DECLARE_CLASS_NAME(ZlibCompressorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    BufferCompressorPtr CreateCompressor() const override;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ZlibCompressorTest, TestCaseForCompress);
INDEXLIB_UNIT_TEST_CASE(ZlibCompressorTest, TestCaseForCompressLargeData);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_ZLIBCOMPRESSORTEST_H
