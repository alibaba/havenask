#ifndef __INDEXLIB_ZLIBDEFAULTCOMPRESSORTEST_H
#define __INDEXLIB_ZLIBDEFAULTCOMPRESSORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/buffer_compressor/test/buffer_compressor_unittest.h"

IE_NAMESPACE_BEGIN(util);

class ZlibDefaultCompressorTest : public BufferCompressorTest
{
public:
    ZlibDefaultCompressorTest();
    ~ZlibDefaultCompressorTest();

    DECLARE_CLASS_NAME(ZlibDefaultCompressorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    BufferCompressorPtr CreateCompressor() const override;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ZlibDefaultCompressorTest, TestCaseForCompress);
INDEXLIB_UNIT_TEST_CASE(ZlibDefaultCompressorTest, TestCaseForCompressLargeData);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_ZLIBDEFAULTCOMPRESSORTEST_H
