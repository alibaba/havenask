#ifndef __INDEXLIB_INMEMBITMAPINDEXDECODERTEST_H
#define __INDEXLIB_INMEMBITMAPINDEXDECODERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/in_mem_bitmap_index_decoder.h"

IE_NAMESPACE_BEGIN(index);

class InMemBitmapIndexDecoderTest : public INDEXLIB_TESTBASE
{
public:
    InMemBitmapIndexDecoderTest();
    ~InMemBitmapIndexDecoderTest();

    DECLARE_CLASS_NAME(InMemBitmapIndexDecoderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInit();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemBitmapIndexDecoderTest, TestInit);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INMEMBITMAPINDEXDECODERTEST_H
