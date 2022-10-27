#ifndef __INDEXLIB_DEFRAGSLICEARRAYTEST_H
#define __INDEXLIB_DEFRAGSLICEARRAYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/slice_array/defrag_slice_array.h"

IE_NAMESPACE_BEGIN(util);

class DefragSliceArrayTest : public INDEXLIB_TESTBASE
{
public:
    typedef DefragSliceArray::SliceArray SliceArray;
    typedef DefragSliceArray::SliceArrayPtr SliceArrayPtr;

public:
    DefragSliceArrayTest();
    ~DefragSliceArrayTest();

    DECLARE_CLASS_NAME(DefragSliceArrayTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAppendAndGet();
    void TestAppendAndReuse();
    void TestFree();
    void TestAppendWhenTurnToNextSlice();
    void TestFreeLastSliceSpace();

private:
    SimpleMemoryQuotaControllerPtr mMemController;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DefragSliceArrayTest, TestAppendAndGet);
INDEXLIB_UNIT_TEST_CASE(DefragSliceArrayTest, TestAppendAndReuse);
INDEXLIB_UNIT_TEST_CASE(DefragSliceArrayTest, TestFree);
INDEXLIB_UNIT_TEST_CASE(DefragSliceArrayTest, TestAppendWhenTurnToNextSlice);
INDEXLIB_UNIT_TEST_CASE(DefragSliceArrayTest, TestFreeLastSliceSpace);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_DEFRAGSLICEARRAYTEST_H
