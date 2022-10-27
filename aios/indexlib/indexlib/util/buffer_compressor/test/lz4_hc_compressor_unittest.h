#ifndef __INDEXLIB_LZ4HCCOMPRESSORTEST_H
#define __INDEXLIB_LZ4HCCOMPRESSORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/buffer_compressor/test/buffer_compressor_unittest.h"

IE_NAMESPACE_BEGIN(util);

class Lz4HcCompressorTest : public BufferCompressorTest
{
public:
    Lz4HcCompressorTest();
    ~Lz4HcCompressorTest();

    DECLARE_CLASS_NAME(Lz4HcCompressorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    util::BufferCompressorPtr CreateCompressor() const override;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(Lz4HcCompressorTest, TestCaseForCompress);
INDEXLIB_UNIT_TEST_CASE(Lz4HcCompressorTest, TestCaseForCompressLargeData);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_LZ4HCCOMPRESSORTEST_H
