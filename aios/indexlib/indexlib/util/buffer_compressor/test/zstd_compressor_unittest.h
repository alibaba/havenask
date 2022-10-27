#ifndef __INDEXLIB_ZSTDCOMPRESSORTEST_H
#define __INDEXLIB_ZSTDCOMPRESSORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/buffer_compressor/zstd_compressor.h"
#include "indexlib/util/buffer_compressor/test/buffer_compressor_unittest.h"

IE_NAMESPACE_BEGIN(util);

class ZstdCompressorTest : public BufferCompressorTest
{
public:
    ZstdCompressorTest();
    ~ZstdCompressorTest();

    DECLARE_CLASS_NAME(ZstdCompressorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    BufferCompressorPtr CreateCompressor() const override;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ZstdCompressorTest, TestCaseForCompress);
INDEXLIB_UNIT_TEST_CASE(ZstdCompressorTest, TestCaseForCompressLargeData);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_ZSTDCOMPRESSORTEST_H
