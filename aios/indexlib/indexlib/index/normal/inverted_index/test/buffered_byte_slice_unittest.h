#ifndef __INDEXLIB_POSTINGBUFFERTEST_H
#define __INDEXLIB_POSTINGBUFFERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format.h"

IE_NAMESPACE_BEGIN(index);

class BufferedByteSliceTest : public INDEXLIB_TESTBASE {
public:
    BufferedByteSliceTest();
    ~BufferedByteSliceTest();

    DECLARE_CLASS_NAME(BufferedByteSliceTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestDecode();
    void TestFlush();
    void TestSnapShot();

private:
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::RecyclePool* mBufferPool;
    BufferedByteSlicePtr mBufferedByteSlice;
    DocListFormatPtr mDocListFormat;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BufferedByteSliceTest, TestDecode);
INDEXLIB_UNIT_TEST_CASE(BufferedByteSliceTest, TestFlush);
INDEXLIB_UNIT_TEST_CASE(BufferedByteSliceTest, TestSnapShot);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTINGBUFFERTEST_H
