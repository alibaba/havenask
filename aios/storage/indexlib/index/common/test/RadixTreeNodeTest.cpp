#include "indexlib/index/common/RadixTreeNode.h"

#include <math.h>

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil::mem_pool;

namespace indexlib::index {

class RadixTreeNodeTest : public INDEXLIB_TESTBASE
{
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
    void InnerTestCalculateCurrentSlot(uint8_t powerOfSlotNum, uint8_t height, uint64_t slotId);

    void InnerTestInsertAndSearch(uint8_t powerOfSlotNum, uint8_t height, uint64_t slotId, Slot& slot, size_t slotSize);

private:
    autil::mem_pool::Pool* _byteSlicePool;
    autil::mem_pool::SimpleAllocator _allocator;
};

INDEXLIB_UNIT_TEST_CASE(RadixTreeNodeTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(RadixTreeNodeTest, TestGetSlotNum);
INDEXLIB_UNIT_TEST_CASE(RadixTreeNodeTest, TestCalculateCurrentSlot);

RadixTreeNodeTest::RadixTreeNodeTest() { _byteSlicePool = new Pool(&_allocator, 10240); }

RadixTreeNodeTest::~RadixTreeNodeTest() { delete _byteSlicePool; }

void RadixTreeNodeTest::CaseSetUp() {}

void RadixTreeNodeTest::CaseTearDown() {}

void RadixTreeNodeTest::TestInit()
{
    RadixTreeNode node(1, 4);
    ASSERT_TRUE(NULL == node._slotArray);
    ASSERT_EQ((uint8_t)1, node._powerOfSlotNum);
    ASSERT_EQ((uint8_t)4, node._height);

    node.Init(_byteSlicePool, NULL);
    ASSERT_FALSE(NULL == node._slotArray);

    for (uint8_t i = 0; i < 2; i++) {
        ASSERT_TRUE(NULL == node._slotArray[i]);
    }
}

void RadixTreeNodeTest::TestGetSlotNum()
{
    {
        RadixTreeNode node(1, 4);
        ASSERT_EQ((uint8_t)2, node.GetSlotNum());
    }

    {
        RadixTreeNode node(2, 4);
        ASSERT_EQ((uint8_t)4, node.GetSlotNum());
    }

    {
        RadixTreeNode node(0, 4);
        ASSERT_EQ((uint8_t)1, node.GetSlotNum());
    }
}

void RadixTreeNodeTest::InnerTestCalculateCurrentSlot(uint8_t powerOfSlotNum, uint8_t height, uint64_t slotId)
{
    uint64_t oneSlotCapacity = (uint64_t)pow(2.0, (double)(powerOfSlotNum * height));
    uint32_t slotIndex = (uint8_t)(slotId / oneSlotCapacity);

    RadixTreeNode node(powerOfSlotNum, height);
    ASSERT_EQ(slotIndex, node.ExtractSubSlotId(slotId)) << oneSlotCapacity << " " << slotId;
}

void RadixTreeNodeTest::TestCalculateCurrentSlot()
{
    // sliceId < 2 ^ (powerOfSlotNum * (height + 1))
    // slotNum = 1 <== powerOfSlotNum = 0
    InnerTestCalculateCurrentSlot(0, 0, 0);
    InnerTestCalculateCurrentSlot(0, 1, 0);

    // slotNum = 2 <== powerOfSlotNum = 1
    InnerTestCalculateCurrentSlot(1, 0, 0);
    InnerTestCalculateCurrentSlot(1, 0, 1);

    // high htight
    InnerTestCalculateCurrentSlot(2, 2, 63);
    InnerTestCalculateCurrentSlot(4, 5, 104857);
    InnerTestCalculateCurrentSlot(4, 5, 16777215);

    // slotNum = 256 <== powerOfSlotNum = 8
    InnerTestCalculateCurrentSlot(8, 0, 0);
    InnerTestCalculateCurrentSlot(8, 0, 255);
}

void RadixTreeNodeTest::TestInsertAndSearch()
{
    char slice[5] = {'1', '2', '3', '4', '5'};
    Slot slot;
    slot = slice;
    {
        InnerTestInsertAndSearch(1, 0, 0, slot, 5);
        InnerTestInsertAndSearch(1, 0, 1, slot, 5);
        InnerTestInsertAndSearch(1, 1, 2, slot, 5);
        InnerTestInsertAndSearch(2, 4, 1000, slot, 5);
        InnerTestInsertAndSearch(4, 5, 16777215, slot, 5);
    }

    {
        SCOPED_TRACE("test for nonexist");
        RadixTreeNode node(1, 0);
        node.Init(_byteSlicePool, NULL);

        Slot expectSlot = node.Search(1);
        ASSERT_TRUE(NULL == expectSlot);
    }
}

void RadixTreeNodeTest::InnerTestInsertAndSearch(uint8_t powerOfSlotNum, uint8_t height, uint64_t slotId, Slot& slot,
                                                 size_t slotSize)
{
    RadixTreeNode node(powerOfSlotNum, height);
    node.Init(_byteSlicePool, NULL);
    node.Insert(slot, slotId, _byteSlicePool);

    Slot expectSlot = node.Search(slotId);

    ASSERT_EQ(expectSlot, slot);
    for (size_t i = 0; i < slotSize; i++) {
        ASSERT_EQ(((char*)expectSlot)[i], ((char*)slot)[i]);
    }
}
} // namespace indexlib::index
