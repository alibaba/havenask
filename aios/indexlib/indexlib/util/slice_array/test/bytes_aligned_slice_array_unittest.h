#ifndef __INDEXLIB_BYTESALIGNEDSLICEARRAYTEST_H
#define __INDEXLIB_BYTESALIGNEDSLICEARRAYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/slice_array/bytes_aligned_slice_array.h"

IE_NAMESPACE_BEGIN(util);

class BytesAlignedSliceArrayTest : public INDEXLIB_TESTBASE
{
public:
    BytesAlignedSliceArrayTest();
    ~BytesAlignedSliceArrayTest();

    DECLARE_CLASS_NAME(BytesAlignedSliceArrayTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForZero();
    void TestCaseForSliceLength();
    void TestCaseForSimple();
    void TestCaseForMultiSlice();
    void TestCaseForRandomSmallSlice();
    void TestCaseForRandomBigSlice();
    //void TestCaseForStorageMetrics();
    void TestCaseForSingleSlice();
    void TestCaseForExtendException();
    void TestCaseForMassData();
    void TestAllocateSlice();
    void TestReleaseSlice();
private:
    template<typename T>
    void testRandom(size_t sliceLen, size_t valueCount);

private:
    std::string mRootDir;
    SimpleMemoryQuotaControllerPtr mMemoryController;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForZero);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForSliceLength);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForSimple);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForMultiSlice);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForRandomSmallSlice);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForRandomBigSlice);
//INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForStorageMetrics);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForSingleSlice);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForExtendException);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestCaseForMassData);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestAllocateSlice);
INDEXLIB_UNIT_TEST_CASE(BytesAlignedSliceArrayTest, TestReleaseSlice);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BYTESALIGNEDSLICEARRAYTEST_H
