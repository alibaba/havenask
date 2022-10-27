#ifndef __INDEXLIB_SNAPPYCOMPRESSORTEST_H
#define __INDEXLIB_SNAPPYCOMPRESSORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/buffer_compressor/test/buffer_compressor_unittest.h"

IE_NAMESPACE_BEGIN(util);

class SnappyCompressorTest : public BufferCompressorTest
{
public:
    SnappyCompressorTest();
    ~SnappyCompressorTest();

    DECLARE_CLASS_NAME(SnappyCompressorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    util::BufferCompressorPtr CreateCompressor() const override;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SnappyCompressorTest, TestCaseForCompress);
INDEXLIB_UNIT_TEST_CASE(SnappyCompressorTest, TestCaseForCompressLargeData);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_SNAPPYCOMPRESSORTEST_H
