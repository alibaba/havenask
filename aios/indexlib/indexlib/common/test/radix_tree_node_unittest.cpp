#include "indexlib/common/test/radix_tree_node_unittest.h"
#include <math.h>

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, RadixTreeNodeTest);

RadixTreeNodeTest::RadixTreeNodeTest()
{
    mByteSlicePool = new Pool(&mAllocator, 10240);
}

RadixTreeNodeTest::~RadixTreeNodeTest()
{
    delete mByteSlicePool;
}

void RadixTreeNodeTest::CaseSetUp()
{
}

void RadixTreeNodeTest::CaseTearDown()
{
}

void RadixTreeNodeTest::TestInit()
{
    RadixTreeNode node(1, 4);
    ASSERT_TRUE(NULL == node.mSlotArray);
    ASSERT_EQ((uint8_t)1, node.mPowerOfSlotNum);
    ASSERT_EQ((uint8_t)4, node.mHeight);

    node.Init(mByteSlicePool, NULL);
    ASSERT_FALSE(NULL == node.mSlotArray);
    
    for (uint8_t i = 0; i < 2; i++)
    {
        ASSERT_TRUE(NULL == node.mSlotArray[i]);
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

void RadixTreeNodeTest::InnerTestCalculateCurrentSlot(
        uint8_t powerOfSlotNum, uint8_t height, uint64_t slotId)
{
    uint64_t oneSlotCapacity = (uint64_t)pow(2.0, (double)(powerOfSlotNum * height));
    uint32_t slotIndex = (uint8_t)(slotId / oneSlotCapacity);
    
    RadixTreeNode node(powerOfSlotNum, height);
    ASSERT_EQ(slotIndex, node.ExtractSubSlotId(slotId)) << oneSlotCapacity << " " << slotId;
}

void RadixTreeNodeTest::TestCalculateCurrentSlot()
{
    //sliceId < 2 ^ (powerOfSlotNum * (height + 1))
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
        node.Init(mByteSlicePool, NULL);
        
        Slot expectSlot = node.Search(1);
        ASSERT_TRUE(NULL == expectSlot);
    }
}

void RadixTreeNodeTest::InnerTestInsertAndSearch(
        uint8_t powerOfSlotNum, uint8_t height,
        uint64_t slotId, Slot& slot,
        size_t slotSize)
{
    RadixTreeNode node(powerOfSlotNum, height);
    node.Init(mByteSlicePool, NULL);
    node.Insert(slot, slotId, mByteSlicePool);
    
    Slot expectSlot = node.Search(slotId);

    ASSERT_EQ(expectSlot, slot);
    for (size_t i = 0; i < slotSize; i++)
    {
        ASSERT_EQ(((char*)expectSlot)[i], ((char*)slot)[i]);
    }
}

IE_NAMESPACE_END(common);

