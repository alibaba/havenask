#ifndef __INDEXLIB_RADIXTREETEST_H
#define __INDEXLIB_RADIXTREETEST_H

#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/SimpleAllocator.h>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/radix_tree.h"

IE_NAMESPACE_BEGIN(common);

class RadixTreeTest : public INDEXLIB_TESTBASE {
public:
    RadixTreeTest();
    ~RadixTreeTest();

    DECLARE_CLASS_NAME(RadixTreeTest);
    typedef RadixTreeNode::Slot Slot;
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCalculatePowerOf2();
    void TestCalculateSliceId();
    void TestCalculateIdxInSlice();
    void TestAppendSliceAndSearch();
    void TestAppend();
    void TestOccupyItem();
    void TestGetSliceSize();
    void TestNeedGrowUp();
    void TestAllocateConsecutiveSlices();
    void TestAllocate();
private:
    void InnerTestAppendSliceAndSearch(RadixTree& tree, uint64_t sliceSize, Slot slot);
    void InnerTestAppend(RadixTree& tree, const char* data,
                         size_t size, uint64_t expectOffset);
private:
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::SimpleAllocator mAllocator;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RadixTreeTest, TestCalculatePowerOf2);
INDEXLIB_UNIT_TEST_CASE(RadixTreeTest, TestCalculateSliceId);
INDEXLIB_UNIT_TEST_CASE(RadixTreeTest, TestCalculateIdxInSlice);
INDEXLIB_UNIT_TEST_CASE(RadixTreeTest, TestAppendSliceAndSearch);
INDEXLIB_UNIT_TEST_CASE(RadixTreeTest, TestAppend);
INDEXLIB_UNIT_TEST_CASE(RadixTreeTest, TestOccupyItem);
INDEXLIB_UNIT_TEST_CASE(RadixTreeTest, TestGetSliceSize);
INDEXLIB_UNIT_TEST_CASE(RadixTreeTest, TestNeedGrowUp);
INDEXLIB_UNIT_TEST_CASE(RadixTreeTest, TestAllocateConsecutiveSlices);
INDEXLIB_UNIT_TEST_CASE(RadixTreeTest, TestAllocate);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_RADIXTREETEST_H
