#ifndef __INDEXLIB_BYTEALIGNEDSLICEARRAYTEST_H
#define __INDEXLIB_BYTEALIGNEDSLICEARRAYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/slice_array/byte_aligned_slice_array.h"

IE_NAMESPACE_BEGIN(util);

class ByteAlignedSliceArrayTest : public INDEXLIB_TESTBASE {
public:
    ByteAlignedSliceArrayTest();
    ~ByteAlignedSliceArrayTest();
public:
    void SetUp();
    void TearDown();
    void TestSetByteList();
    void TestGetDataLen();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ByteAlignedSliceArrayTest, TestSetByteList);
INDEXLIB_UNIT_TEST_CASE(ByteAlignedSliceArrayTest, TestGetDataLen);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BYTEALIGNEDSLICEARRAYTEST_H
