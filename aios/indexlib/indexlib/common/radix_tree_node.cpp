#include "indexlib/common/radix_tree_node.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, RadixTreeNode);

RadixTreeNode::RadixTreeNode(uint8_t powerOfSlotNum, uint8_t height)
    : mSlotArray(NULL)
    , mPowerOfSlotNum(powerOfSlotNum)
    , mHeight(height)
    , mShift(mPowerOfSlotNum * mHeight)
    , mMask((1 << mPowerOfSlotNum) - 1)
{
    assert(mPowerOfSlotNum<=8);
    assert(mPowerOfSlotNum*mHeight<=64);
}

RadixTreeNode::~RadixTreeNode() 
{
}

void RadixTreeNode::Init(Pool* byteSlicePool, RadixTreeNode* firtNode)
{
    uint32_t slotNum = GetSlotNum();
    mSlotArray = (Slot*)byteSlicePool->allocate(slotNum * sizeof(Slot));
    mSlotArray[0] = firtNode;
    for (uint32_t i = 1; i < slotNum; i++)
    {
        mSlotArray[i] = NULL;
    }
}

void RadixTreeNode::Insert(Slot slot, uint64_t slotId, Pool* byteSlicePool)
{
    uint32_t slotIndex = ExtractSubSlotId(slotId);
    
    if (mHeight == 0)
    {
        mSlotArray[slotIndex] = slot;
        return;
    }

    RadixTreeNode*& currentSlot = (RadixTreeNode*&) mSlotArray[slotIndex];

    if (NULL == currentSlot)
    {
        void* buffer = byteSlicePool->allocate(sizeof(RadixTreeNode));
        RadixTreeNode *tempNode = (RadixTreeNode* )new (buffer)
            RadixTreeNode(mPowerOfSlotNum, mHeight - 1);
        tempNode->Init(byteSlicePool, NULL);
        currentSlot = tempNode;
    }

    currentSlot->Insert(slot, slotId, byteSlicePool);
    return;
}

IE_NAMESPACE_END(common);

