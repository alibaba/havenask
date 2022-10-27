#ifndef __INDEXLIB_RADIX_TREE_H
#define __INDEXLIB_RADIX_TREE_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/radix_tree_node.h"

IE_NAMESPACE_BEGIN(common);

// ATTENTION:
// only support one write and multi read,
// and read is safe only if corresponding item has been written
class RadixTree
{
public:
    //slot number only support 2,4,8,16...,128,256
    RadixTree(uint32_t slotNum, uint32_t itemNumInSlice, 
              autil::mem_pool::Pool* byteSlicePool, uint16_t itemSize = 1);
    ~RadixTree();

public:
    uint8_t* OccupyOneItem();
    uint8_t* Search(uint64_t itemIdx);

    uint32_t GetSliceNum() const { return mSliceNum; }
    uint32_t GetSliceSize(uint32_t sliceId) const;
    uint8_t* GetSlice(uint64_t sliceId)
    { return (uint8_t*)mRoot->Search(sliceId); }
    // return current cursor in bytes
    uint64_t GetCurrentOffset() const 
    { return ((uint64_t)mSliceNum - 1) * CalculateSliceSize() + mSliceCursor; }

    //TODO: conflict with itemsize??
    bool Append(const uint8_t* data, size_t length);
    uint8_t* Allocate(size_t appendSize);

private:

    void DoInit(uint32_t slotNum, uint64_t sliceSize);
    RadixTreeNode* CreateNode(uint8_t height, RadixTreeNode* firstSubNode);
    void AppendSlice(RadixTreeNode::Slot slice);
    bool NeedGrowUp(uint8_t height) const
    { return mSliceNum >= (uint32_t)(1 << (mPowerOfSlotNum * (height + 1))); }

    uint64_t CalculateIdxInSlice(uint64_t itemIdx) const
    { return itemIdx & ((1 << mPowerOfItemNum) - 1); }
    uint64_t CalculateSliceId(uint64_t itemIdx) const
    { return itemIdx >> mPowerOfItemNum; }
    uint32_t CalculateSliceSize() const 
    { return mItemSize * (1 << mPowerOfItemNum); }

    uint32_t CalculateNeededSliceNum(uint32_t dataSize, uint32_t sliceSize)
    {
        return (dataSize + sliceSize - 1) / sliceSize;
    }

    uint8_t* AllocateConsecutiveSlices(uint32_t sliceNum, uint32_t sliceSize);

    static uint8_t CalculatePowerOf2(uint64_t value);
private:
    autil::mem_pool::Pool* mByteSlicePool;
    RadixTreeNode* volatile mRoot;
    uint32_t mSliceNum;
    uint32_t mSliceCursor;
    //sliceSize = mItemSize * (1 << mPowerOfItemNum)
    //unit size support for slice size is not equal 2^n
    uint16_t mItemSize; 
    uint8_t mPowerOfSlotNum;
    uint8_t mPowerOfItemNum;

private:
    friend class RadixTreeTest;
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////
inline uint8_t* RadixTree::Search(uint64_t itemIdx)
{
    uint64_t sliceId = CalculateSliceId(itemIdx);

    assert(sliceId < mSliceNum);

    uint8_t* slice = GetSlice(sliceId);
    return slice + CalculateIdxInSlice(itemIdx) * mItemSize;
}

inline uint8_t RadixTree::CalculatePowerOf2(uint64_t value)
{
    uint8_t power = 0;
    uint64_t tmpValue = 1;
    
    while(tmpValue < value)
    {
        power++;
        tmpValue <<= 1;
    }
    
    return power;
}

inline uint32_t RadixTree::GetSliceSize(uint32_t sliceId) const
{
    if (sliceId + 1 < mSliceNum)
    {
        return CalculateSliceSize();
    }
    else if (sliceId + 1 == mSliceNum)
    {
        return mSliceCursor;
    }
    return 0;
}

DEFINE_SHARED_PTR(RadixTree);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_RADIX_TREE_H
