#ifndef __INDEXLIB_BUFFERCOMPRESSORTEST_H
#define __INDEXLIB_BUFFERCOMPRESSORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/buffer_compressor/buffer_compressor.h"

IE_NAMESPACE_BEGIN(util);

class BufferCompressorTest : public INDEXLIB_TESTBASE
{
public:
    BufferCompressorTest(bool supportStream = true);
    virtual ~BufferCompressorTest();

    DECLARE_CLASS_NAME(BufferCompressorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForCompress();
    void TestCaseForCompressLargeData();

private:
    virtual util::BufferCompressorPtr CreateCompressor() const = 0;

protected:
    bool mSupportStream = false;;

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BUFFERCOMPRESSORTEST_H
