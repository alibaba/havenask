#include "indexlib/common/radix_tree.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, RadixTree);

RadixTree::RadixTree(uint32_t slotNum, uint32_t itemNumInSlice,
                     Pool* byteSlicePool, uint16_t unitSize)
    : mByteSlicePool(byteSlicePool)
    , mRoot(NULL)
    , mSliceNum(0)
    , mItemSize(unitSize)
{
    assert(slotNum <= 256);
    DoInit(slotNum, itemNumInSlice);
}

RadixTree::~RadixTree() 
{
}

void RadixTree::DoInit(uint32_t slotNum, uint64_t itemNumInSlice)
{
    assert(slotNum > 1);
    assert(itemNumInSlice != 0);

    mPowerOfSlotNum = CalculatePowerOf2(slotNum);
    mPowerOfItemNum = CalculatePowerOf2(itemNumInSlice);
    mSliceCursor = CalculateSliceSize();
}

bool RadixTree::Append(const uint8_t* data, size_t length)
{
    //append 0 bytes, allocate memory for search
    // assert(mItemSize == 1);
    if (length == 0)
    {
        Allocate(length);
        return true;
    }

    uint8_t* address = Allocate(length);
    if (address)
    {
        memcpy(address, data, length);
        return true;
    }
    IE_LOG(WARN, "Append data fail, can not allocate memory [%lu]",
           (uint64_t)length);
    return false;
}

uint8_t* RadixTree::AllocateConsecutiveSlices(uint32_t sliceNum, uint32_t sliceSize)
{
    if (sliceNum <= 0)
    {
        return NULL;
    }

    uint8_t* currentSlice = (uint8_t*)mByteSlicePool->allocate(
            sliceSize * sliceNum);

    if (currentSlice == NULL)
    {
        IE_LOG(ERROR, "allocate data fail,"
               "can not allocate sliceNum[%u]", sliceNum);
        return NULL;
    }

    mSliceCursor = 0;

    uint8_t* tmpSlice = currentSlice;
    for (uint32_t i = 0; i < sliceNum; i++)
    {
        AppendSlice((RadixTreeNode::Slot)tmpSlice);
        tmpSlice += sliceSize;
    }
    return currentSlice;
}

uint8_t* RadixTree::Allocate(size_t appendSize)
{
    uint32_t sliceSize = CalculateSliceSize();
    uint8_t* currentSlice = NULL;

    if (appendSize > sliceSize - mSliceCursor)
    {
        uint32_t sliceNumToAllocate = CalculateNeededSliceNum(appendSize, sliceSize);
        currentSlice = AllocateConsecutiveSlices(sliceNumToAllocate, sliceSize);

        if (currentSlice == NULL)
        {
            IE_LOG(ERROR, "allocate data fail,"
               "can not allocate size [%lu]", appendSize);
            return NULL;
        }

        mSliceCursor = appendSize - (sliceNumToAllocate - 1) * sliceSize;
    }
    else if (appendSize == 0 && mSliceCursor == sliceSize)
    {
        //to avoid read core
        currentSlice = AllocateConsecutiveSlices(1, sliceSize);
        mSliceCursor = 0;
    }
    else
    {
        currentSlice = (uint8_t*)mRoot->Search(mSliceNum - 1) + mSliceCursor;
        mSliceCursor += appendSize;
    }

    return currentSlice;
}

uint8_t* RadixTree::OccupyOneItem()
{
    return Allocate(mItemSize);
}

void RadixTree::AppendSlice(RadixTreeNode::Slot slice)
{
    if (mRoot == NULL)
    {
        mRoot = CreateNode(0, NULL);
    }
    
    uint8_t height = mRoot->GetHeight();
    if (NeedGrowUp(height))
    {
        RadixTreeNode* node = CreateNode(height + 1, mRoot);
        mRoot = node;
    }
    
    mRoot->Insert(slice, mSliceNum, mByteSlicePool);
    mSliceNum++;
}

RadixTreeNode* RadixTree::CreateNode(uint8_t height,
                                     RadixTreeNode* firstSubNode)
{
    void* buffer = mByteSlicePool->allocate(sizeof(RadixTreeNode));
    RadixTreeNode* node = (RadixTreeNode* )new (buffer) RadixTreeNode(
            mPowerOfSlotNum, height);
    node->Init(mByteSlicePool, firstSubNode);
    return node;
}

IE_NAMESPACE_END(common);

