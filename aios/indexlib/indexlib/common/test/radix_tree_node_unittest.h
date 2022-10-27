#ifndef __INDEXLIB_RADIXTREENODETEST_H
#define __INDEXLIB_RADIXTREENODETEST_H

#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/SimpleAllocator.h>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/radix_tree_node.h"

IE_NAMESPACE_BEGIN(common);

class RadixTreeNodeTest : public INDEXLIB_TESTBASE {
public:
    RadixTreeNodeTest();
    ~RadixTreeNodeTest();

    DECLARE_CLASS_NAME(RadixTreeNodeTest);
    typedef RadixTreeNode::Slot Slot;
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInit();
    void TestGetSlotNum();
    void TestCalculateCurrentSlot();
    void TestInsertAndSearch();
private:
    void InnerTestCalculateCurrentSlot(
            uint8_t powerOfSlotNum, uint8_t height, uint64_t slotId);

    void InnerTestInsertAndSearch(
        uint8_t powerOfSlotNum, uint8_t height,
        uint64_t slotId, Slot& slot, size_t slotSize);

private:
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::SimpleAllocator mAllocator;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RadixTreeNodeTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(RadixTreeNodeTest, TestGetSlotNum);
INDEXLIB_UNIT_TEST_CASE(RadixTreeNodeTest, TestCalculateCurrentSlot);
INDEXLIB_UNIT_TEST_CASE(RadixTreeNodeTest, TestInsertAndSearch);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_RADIXTREENODETEST_H
