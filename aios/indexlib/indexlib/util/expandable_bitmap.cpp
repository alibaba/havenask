
#include "indexlib/util/expandable_bitmap.h"
#include "indexlib/util/profiling.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, ExpandableBitmap);

ExpandableBitmap::ExpandableBitmap(bool bSet, PoolBase* pool)
    : Bitmap(bSet, pool)
{
    mValidItemCount = 0;
}

ExpandableBitmap::ExpandableBitmap(uint32_t nItemCount, bool bSet,
                                   PoolBase* pool)
    : Bitmap(nItemCount, bSet, pool)
{
    mValidItemCount = mItemCount;
}

ExpandableBitmap::ExpandableBitmap(const ExpandableBitmap& rhs)
    : Bitmap(rhs)
{
    mValidItemCount = rhs.mValidItemCount;
}

void ExpandableBitmap::Set(uint32_t nIndex)
{
    if (nIndex >= mItemCount)
    {
        Expand(nIndex);
        mValidItemCount = mItemCount;
    }

    Bitmap::Set(nIndex);
}

void ExpandableBitmap::Reset(uint32_t nIndex)
{
    if (nIndex >= mItemCount)
    {
        Expand(nIndex);
    }
    
    Bitmap::Reset(nIndex);
}

void ExpandableBitmap::ReSize(uint32_t nItemCount)
{
    if (nItemCount >= mItemCount)
    {
        Expand(nItemCount);
        mValidItemCount = mItemCount;
    } 
    else
    {
        mValidItemCount = nItemCount;
    }
}

void ExpandableBitmap::Expand(uint32_t nIndex)
{
    assert(nIndex >= mItemCount);
    
    uint32_t allocSlotCount = mSlotCount;

    uint32_t addSlotCount = (mSlotCount / 2) > 0 ? (mSlotCount / 2) : 1;
    do
    {
        allocSlotCount += addSlotCount;
    } while (nIndex >= allocSlotCount * SLOT_SIZE);

    uint32_t* data = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, uint32_t, allocSlotCount);
    PROCESS_PROFILE_COUNT(memory, ExpandableBitmap,
                          allocSlotCount * sizeof(uint32_t));

    IE_LOG(DEBUG, "expand from %u to %u", mSlotCount, allocSlotCount);


    if (mData != NULL)
    {
        memcpy(data, mData, mSlotCount * sizeof(uint32_t));

        uint8_t initValue = mInitSet ? 0xFF : 0;
        memset(data + mSlotCount, initValue, (allocSlotCount - mSlotCount) * sizeof(uint32_t));
        
        PROCESS_PROFILE_COUNT(memory, ExpandableBitmap,
                              -mSlotCount * sizeof(uint32_t));
        // only release mData when mPool is NULL
        IE_POOL_COMPATIBLE_DELETE_VECTOR(mPool, mData, mSlotCount);
        mData = data;

        mSlotCount = allocSlotCount;
        mItemCount = mSlotCount * SLOT_SIZE;
    }
    else
    {
        uint8_t initValue = mInitSet ? 0xFF : 0;
        memset(data + mSlotCount, initValue, (allocSlotCount - mSlotCount) * sizeof(uint32_t));
        
        PROCESS_PROFILE_COUNT(memory, ExpandableBitmap,
                              -mSlotCount * sizeof(uint32_t));
        mData = data;
        mSlotCount = allocSlotCount;
        mItemCount = mSlotCount * SLOT_SIZE;
    }
}

ExpandableBitmap* ExpandableBitmap::CloneWithOnlyValidData() const
{
    ExpandableBitmap* newBitmap = new ExpandableBitmap(false);
    newBitmap->mItemCount = mValidItemCount;
    newBitmap->mValidItemCount = mValidItemCount;
    newBitmap->mSetCount = mSetCount;
    newBitmap->mSlotCount = (newBitmap->mItemCount + SLOT_SIZE - 1) >> 5;
    newBitmap->mInitSet = mInitSet;
    newBitmap->mMount = mMount;

    if (GetData() != NULL)
    {
        newBitmap->mData = IE_POOL_COMPATIBLE_NEW_VECTOR(newBitmap->mPool, uint32_t, newBitmap->mSlotCount);
        PROCESS_PROFILE_COUNT(memory, Bitmap, newBitmap->mSlotCount * sizeof(uint32_t));
        memcpy(newBitmap->mData, mData, newBitmap->mSlotCount * sizeof(uint32_t));
    }
    else
    {
        newBitmap->mData = NULL;
    }

    newBitmap->mMount = false;
    return newBitmap;
}

void ExpandableBitmap::Mount(uint32_t nItemCount, uint32_t* pData, bool doNotDelete)
{
    mValidItemCount = nItemCount;
    Bitmap::Mount(nItemCount, pData, doNotDelete);
}

void ExpandableBitmap::MountWithoutRefreshSetCount(uint32_t nItemCount,
        uint32_t* pData, bool doNotDelete)
{
    mValidItemCount = nItemCount;
    Bitmap::MountWithoutRefreshSetCount(nItemCount, pData, doNotDelete);
}

ExpandableBitmap* ExpandableBitmap::Clone() const
{
    return new ExpandableBitmap(*this);
}

IE_NAMESPACE_END(util);

