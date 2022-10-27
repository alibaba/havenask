#ifndef __INDEXLIB_LZ4COMPRESSORTEST_H
#define __INDEXLIB_LZ4COMPRESSORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/buffer_compressor/test/buffer_compressor_unittest.h"

IE_NAMESPACE_BEGIN(util);

class Lz4CompressorTest : public BufferCompressorTest
{
public:
    Lz4CompressorTest();
    ~Lz4CompressorTest();

    DECLARE_CLASS_NAME(Lz4CompressorTest);
    
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    BufferCompressorPtr CreateCompressor() const override;
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(Lz4CompressorTest, TestCaseForCompress);
INDEXLIB_UNIT_TEST_CASE(Lz4CompressorTest, TestCaseForCompressLargeData);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_LZ4COMPRESSORTEST_H
