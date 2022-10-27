#ifndef __INDEXLIB_RADIX_TREE_NODE_H
#define __INDEXLIB_RADIX_TREE_NODE_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);

class RadixTreeNode
{
public:
    typedef void* Slot;

public:
    //All Node in a tree have same powerOfSlotNum
    //0 <= powerOfSlotNum * height <= 64, 1 <= powerOfSlotNum <= 8
    RadixTreeNode(uint8_t powerOfSlotNum, uint8_t height);
    ~RadixTreeNode();

public:
    void Init(autil::mem_pool::Pool* byteSlicePool,
              RadixTreeNode* firstNode);
    
    //outer space must gurantee slotId must in this node
    void Insert(Slot slot, uint64_t slotId,
                autil::mem_pool::Pool* byteSlicePool);

    Slot Search(uint64_t slotId);

    uint8_t GetHeight() const { return mHeight; }

private:
    uint32_t GetSlotNum() const { return 1 << mPowerOfSlotNum; }
    uint32_t ExtractSubSlotId(uint64_t slotId) const
    { return (uint8_t)((slotId >> mShift) & mMask); }

private:
    Slot* mSlotArray;
    const uint8_t mPowerOfSlotNum;
    const uint8_t mHeight;
    const uint8_t mShift;
    const uint8_t mMask;

private:
    friend class RadixTreeNodeTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RadixTreeNode);
///////////////////////////////////////////////////////
inline RadixTreeNode::Slot RadixTreeNode::Search(uint64_t slotId)
{
    RadixTreeNode* tmpNode = this;
    while (tmpNode)
    {
        uint32_t subSlotId = tmpNode->ExtractSubSlotId(slotId);
        if (tmpNode->mHeight == 0)
        {
            return tmpNode->mSlotArray[subSlotId];
        }
        
        tmpNode = (RadixTreeNode*)tmpNode->mSlotArray[subSlotId];
    }
    return NULL;
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_RADIX_TREE_NODE_H
