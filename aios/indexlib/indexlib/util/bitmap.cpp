#include "indexlib/util/bitmap.h"
#include "indexlib/util/profiling.h"

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, Bitmap);

const uint32_t Bitmap::BITMAPOPMASK[SLOT_SIZE] = 
{
    0x80000000, 0x40000000, 0x20000000, 0x10000000,
    0x08000000, 0x04000000, 0x02000000, 0x01000000,
    0x00800000, 0x00400000, 0x00200000, 0x00100000,
    0x00080000, 0x00040000, 0x00020000, 0x00010000,
    0x00008000, 0x00004000, 0x00002000, 0x00001000,
    0x00000800, 0x00000400, 0x00000200, 0x00000100,
    0x00000080, 0x00000040, 0x00000020, 0x00000010,
    0x00000008, 0x00000004, 0x00000002, 0x00000001
};

Bitmap::Bitmap(bool bSet, autil::mem_pool::PoolBase* pool)
{
    if (bSet)
        mItemCount = mSlotCount = INVALID_INDEX;
    else
        mItemCount = mSlotCount = 0;
    mInitSet = bSet;
    mData = NULL;
    mSetCount = 0;
    mMount = false;
    mPool = pool;
}

Bitmap::Bitmap(uint32_t nItemCount, bool bSet, autil::mem_pool::PoolBase* pool)
{
    mPool = pool;
    mSlotCount = (nItemCount + SLOT_SIZE - 1) >> 5;
    mData = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, uint32_t, mSlotCount);

    PROCESS_PROFILE_COUNT(memory, Bitmap, mSlotCount * sizeof(uint32_t));

    mItemCount = nItemCount;
    memset(mData, bSet ? 0xFF : 0x0, mSlotCount * sizeof(uint32_t));
    mSetCount = bSet ? nItemCount : 0;
    mMount = false;
    mInitSet = bSet;
}

Bitmap::Bitmap(const Bitmap& rhs)
{
    mItemCount = rhs.mItemCount;
    mSetCount = rhs.mSetCount;
    mSlotCount = rhs.mSlotCount;
    mInitSet = rhs.mInitSet;
    mPool = NULL;

    if (rhs.GetData() != NULL)
    {
        mData = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, uint32_t, mSlotCount);
        PROCESS_PROFILE_COUNT(memory, Bitmap, mSlotCount * sizeof(uint32_t));
        memcpy(mData, rhs.mData, mSlotCount * sizeof(uint32_t));
    }
    else
    {
        mData = NULL;
    }
    mMount = false;
}

Bitmap& Bitmap::operator=(const Bitmap& rhs)
{
    if (this != &rhs )
    {
        mPool = rhs.mPool;
        if(!mMount && mData!=NULL)
        {
            // only release mData when mPool is NULL
            IE_POOL_COMPATIBLE_DELETE_VECTOR(mPool, mData, mSlotCount);
            PROCESS_PROFILE_COUNT(memory, Bitmap, 
                    -mSlotCount * sizeof(uint32_t));
        }

        mItemCount = rhs.mItemCount;
        mSetCount = rhs.mSetCount;
        mSlotCount = rhs.mSlotCount;
        mInitSet = rhs.mInitSet;

        if (rhs.GetData() != NULL)
        {
            mData = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, uint32_t, mSlotCount);
            PROCESS_PROFILE_COUNT(memory, Bitmap,
                    mSlotCount * sizeof(uint32_t));
            memcpy(mData, rhs.mData, mSlotCount*sizeof(uint32_t));
        }
        else
        {
            mData = NULL;
        }
        mMount = false;
    }
    return *this;
}

Bitmap::~Bitmap(void)
{
    Clear();
}

Bitmap* Bitmap::Clone() const
{
    return new Bitmap(*this);
}

bool Bitmap::Alloc(uint32_t nItemCount, bool bSet)
{
    Clear();

    mSlotCount = (nItemCount + SLOT_SIZE - 1) >> 5;
    mData = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, uint32_t, mSlotCount);
    PROCESS_PROFILE_COUNT(memory, Bitmap, mSlotCount * sizeof(uint32_t));

    mItemCount = nItemCount;
    memset(mData, bSet ? 0xFF : 0x0, mSlotCount * sizeof(uint32_t));
    mSetCount = bSet ? nItemCount : 0;

    return true;
}

void Bitmap::Clear()
{
    if (!mMount&&mData != NULL)
    {
        // only release mData when mPool is NULL
        IE_POOL_COMPATIBLE_DELETE_VECTOR(mPool, mData, mSlotCount);
        PROCESS_PROFILE_COUNT(memory, Bitmap, -mSlotCount * sizeof(uint32_t));
    }
    mData = NULL;

    
    mItemCount = 0;
    mSetCount = 0;
}

uint32_t Bitmap::Size() const
{
    return mSlotCount * sizeof(uint32_t);
}

void Bitmap::RefreshSetCountByScanning() const
{
    if (mData == NULL)
    {
        mSetCount = 0;
        return;
    }

    uint32_t nData;
    mSetCount = 0;

    uint32_t testSlot = mSlotCount - 1;
    uint32_t testBitInLastSlot = (mItemCount & SLOT_SIZE_BIT_MASK);
    mSetCount = CountBits(mData[testSlot], testBitInLastSlot);

    for (uint32_t i = 0; i < testSlot; ++i) 
    {
        if ((nData = mData[i]) == 0)
        {
            continue;
        }
        mSetCount += CountBits(nData);
    }
}

uint32_t Bitmap::GetSetCount() const
{
    return mSetCount;
}

void Bitmap::SetSetCount(uint32_t nCount)
{
    mSetCount = nCount;
}

uint32_t Bitmap::GetUnsetCount() const
{
    return mItemCount - GetSetCount();
}

uint32_t Bitmap::Begin() const
{
    uint32_t nData;
    for (uint32_t i = 0; i < mSlotCount; ++i) 
    {
        if ((nData=mData[i]) != 0) 
        {
            for (uint32_t j = 0; j < SLOT_SIZE&&((i << SLOT_SIZE_BIT_NUM) + j) < mItemCount; ++j)
            {
                if ((nData & BITMAPOPMASK[j]) != 0)
                {
                    return (i << SLOT_SIZE_BIT_NUM) + j;
                }
            }
        }
    }
    return INVALID_INDEX;
}

uint32_t Bitmap::Next(uint32_t nIndex) const
{
    if (++nIndex < mItemCount)
    {
        uint32_t quot = nIndex >> SLOT_SIZE_BIT_NUM;
        uint32_t rem = nIndex & SLOT_SIZE_BIT_MASK;

        for (uint32_t i = quot; i < mSlotCount; ++i)
        {
            uint32_t nData = mData[i];
            if (i==quot) 
            {
                for (uint32_t j = rem; j < SLOT_SIZE; ++j)
                {
                    if ((nData & BITMAPOPMASK[j]) !=0)
                        return i * SLOT_SIZE + j;
                }
            }
            else 
            {
                if(nData == 0)
                    continue;
                for (uint32_t j = 0; j < 32; ++j)
                {
                    if ((nData & BITMAPOPMASK[j]) !=0)
                        return i * SLOT_SIZE + j;
                }
            }
        }
    }
    return INVALID_INDEX;
}

void Bitmap::Set(uint32_t nIndex)
{
    assert(nIndex < mItemCount);

    uint32_t quot = nIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t rem = nIndex & SLOT_SIZE_BIT_MASK;
    
    if (!(mData[quot] & BITMAPOPMASK[rem]))
    {
        mData[quot] |= BITMAPOPMASK[rem];
        ++mSetCount;
    }
}

void Bitmap::Reset(uint32_t nIndex)
{
    if(nIndex >= mItemCount)
    {
        return;
    }
    uint32_t quot = nIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t rem = nIndex & SLOT_SIZE_BIT_MASK;

    if (mData[quot] & BITMAPOPMASK[rem])
    {
        mData[quot] &= ~BITMAPOPMASK[rem];
        --mSetCount;
    }
}

void Bitmap::ResetAll()
{
    memset(mData, 0x0, mSlotCount*sizeof(uint32_t));
    mSetCount = 0;
}

void Bitmap::ResetAllAfter(uint32_t nIndex)
{
    assert(nIndex < mItemCount);

    uint32_t quot = nIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t rem = nIndex & SLOT_SIZE_BIT_MASK;

    for (uint32_t i = rem; i < SLOT_SIZE; i++)
    {
        mData[quot] &= ~BITMAPOPMASK[i]; 
    }

    if (quot < mSlotCount - 1)
    {
        uint32_t siz = mSlotCount-quot-1;
        memset(((char*)mData)+(quot+1)*sizeof(uint32_t), 
               0, siz*sizeof(uint32_t));
    }

    RefreshSetCountByScanning();
}

void Bitmap::Mount(uint32_t nItemCount, uint32_t* pData, bool doNotDelete)
{
    MountWithoutRefreshSetCount(nItemCount, pData, doNotDelete);
    RefreshSetCountByScanning();
}

void Bitmap::Copy(uint32_t startIndex, uint32_t *data, uint32_t itemCount)
{
    assert(mData != NULL);
    if (data == NULL || itemCount == 0)
    {
        return;
    }
    uint32_t *dstSlotAddr = mData + (startIndex >> 5);
    startIndex &= 31;
    Copy(dstSlotAddr, startIndex, data, itemCount);
}

void Bitmap::Copy(uint32_t *dstSlotAddr, uint32_t startIndex, 
                  uint32_t *data, uint32_t itemCount)
{
    assert(startIndex < 32);
    uint32_t offset = startIndex;
    uint32_t remainBitsInDstSlot = 32 - offset;

    uint32_t numSlotsInSrc = (itemCount + 31) >> 5;
    for (uint32_t i = 0; i < numSlotsInSrc - 1; i++)
    {
        uint32_t value = data[i];
        // copy first #remainBitsInDstSlot# bits from value to dstSlotAddr
        *dstSlotAddr |= (value >> offset);
        dstSlotAddr++;
        // copy next #offset# bits from value to dstSlotAddr
        if (offset != 0)
        {
            *dstSlotAddr |= (value << remainBitsInDstSlot);
        }
        mSetCount += CountBits(value);
    }
    // copy remain bits in last slot of data
    uint32_t value = data[numSlotsInSrc - 1];
    uint32_t remainBitsInSrcSlot = itemCount - 32 * (numSlotsInSrc - 1);
    mSetCount += CountBits(value, remainBitsInSrcSlot);
    if (remainBitsInSrcSlot > 0)
    {
        if (remainBitsInSrcSlot <= remainBitsInDstSlot)
        {
            uint32_t tmp = (value >> (32 - remainBitsInSrcSlot)) << 
                           (remainBitsInDstSlot - remainBitsInSrcSlot);
            *dstSlotAddr |= tmp;
        }
        else
        {
            *dstSlotAddr |= (value >> offset);
            dstSlotAddr++;
            uint32_t tmp = (value >> (32 - remainBitsInSrcSlot)) << 
                           (32 - remainBitsInSrcSlot + remainBitsInDstSlot);
            *dstSlotAddr |= tmp;
        }
    }
}

void Bitmap::CopySlice(uint32_t index, uint32_t *data, 
                       uint32_t startIndex, uint32_t itemCount)
{
    if (data == NULL || itemCount == 0)
    {
        return;
    }

    if ((startIndex & 31) == 0)
    {
        Copy(index, data + (startIndex >> 5), itemCount);
        return;
    }

    uint32_t *srcStartAddr = data + (startIndex >> 5);
    uint32_t *dstStartAddr = mData + (index >> 5);
    uint32_t leftBitsInFirstSrcSlot = 32 - (startIndex & 31);
    uint32_t leftBitsInFirstDstSlot = 32 - (index & 31);
    uint32_t newIndex = 0;

    uint32_t value = *srcStartAddr;
    uint32_t mask = (1 << leftBitsInFirstSrcSlot) - 1;
    value &= mask;

    mSetCount += CountBits(value);

    if (leftBitsInFirstSrcSlot <= leftBitsInFirstDstSlot)
    {
        value <<= (leftBitsInFirstDstSlot - leftBitsInFirstSrcSlot);
        *dstStartAddr |= value;
        if (leftBitsInFirstSrcSlot == leftBitsInFirstDstSlot)
        {
            dstStartAddr++;
            newIndex = 0;
        }
        else
        {
            newIndex = (index & 31) + leftBitsInFirstSrcSlot;
        }
    }
    else
    {
        uint32_t first = (value >> 
                          (leftBitsInFirstSrcSlot - leftBitsInFirstDstSlot));
        *dstStartAddr |= first;
        dstStartAddr++;
        mask = (1 << (leftBitsInFirstSrcSlot - leftBitsInFirstDstSlot)) - 1;
        value &= mask;
        value <<= (32 - leftBitsInFirstSrcSlot + leftBitsInFirstDstSlot);
        *dstStartAddr |= value;
        newIndex = leftBitsInFirstSrcSlot - leftBitsInFirstDstSlot;
    }
    if (itemCount > leftBitsInFirstSrcSlot)
    {
        Copy(dstStartAddr, newIndex, srcStartAddr + 1, 
             itemCount - leftBitsInFirstSrcSlot);
    }
}

uint32_t& Bitmap::operator[](uint32_t i)
{
    assert(i < mSlotCount);
    return mData[i];
}

Bitmap& Bitmap::operator&(Bitmap& bitmap)
{
    assert(bitmap.GetItemCount() == GetItemCount());

    uint32_t *b = bitmap.GetData();
    for(uint32_t i = 0; i < mSlotCount; ++i)
    {
        mData[i] &= b[i];
    }
    RefreshSetCountByScanning();

    return *this;
}

Bitmap& Bitmap::operator&=(Bitmap& bitmap)
{
    return *this & bitmap;
}

Bitmap& Bitmap::operator|(const Bitmap& bitmap)
{
    assert(bitmap.GetItemCount() == GetItemCount());
    uint32_t* targetAddr = mData;
    uint32_t* sourceAddr = bitmap.GetData();
    uint32_t batchNum = mSlotCount / 32;
    uint32_t remainCount = mSlotCount % 32;
    for (size_t i = 0; i < batchNum; i++)
    {
        targetAddr[0] |= sourceAddr[0];
        targetAddr[1] |= sourceAddr[1];
        targetAddr[2] |= sourceAddr[2];
        targetAddr[3] |= sourceAddr[3];
        targetAddr[4] |= sourceAddr[4];
        targetAddr[5] |= sourceAddr[5];
        targetAddr[6] |= sourceAddr[6];
        targetAddr[7] |= sourceAddr[7];
        targetAddr[8] |= sourceAddr[8];
        targetAddr[9] |= sourceAddr[9];
        targetAddr[10] |= sourceAddr[10];
        targetAddr[11] |= sourceAddr[11];
        targetAddr[12] |= sourceAddr[12];
        targetAddr[13] |= sourceAddr[13];
        targetAddr[14] |= sourceAddr[14];
        targetAddr[15] |= sourceAddr[15];
        targetAddr[16] |= sourceAddr[16];
        targetAddr[17] |= sourceAddr[17];
        targetAddr[18] |= sourceAddr[18];
        targetAddr[19] |= sourceAddr[19];
        targetAddr[20] |= sourceAddr[20];
        targetAddr[21] |= sourceAddr[21];
        targetAddr[22] |= sourceAddr[22];
        targetAddr[23] |= sourceAddr[23];
        targetAddr[24] |= sourceAddr[24];
        targetAddr[25] |= sourceAddr[25];
        targetAddr[26] |= sourceAddr[26];
        targetAddr[27] |= sourceAddr[27];
        targetAddr[28] |= sourceAddr[28];
        targetAddr[29] |= sourceAddr[29];
        targetAddr[30] |= sourceAddr[30];
        targetAddr[31] |= sourceAddr[31];
        targetAddr += 32;
        sourceAddr += 32;
    }
    for (size_t i = 0; i < remainCount; i++)
    {
        targetAddr[i] |= sourceAddr[i];
    }    
    RefreshSetCountByScanning();

    return *this;
}

Bitmap& Bitmap::operator|=(const Bitmap& bitmap)
{
    return *this | bitmap;
}

Bitmap& Bitmap::operator~()
{
    mSetCount = GetUnsetCount();
    uint32_t nLastSlot = mSlotCount - 1;
    for(uint32_t i = 0; i < nLastSlot; ++i)
    {
        mData[i] = ~mData[i];
    }
    
    uint32_t nCount = mItemCount - nLastSlot * SLOT_SIZE;
    uint32_t nData = mData[nLastSlot];
    for (uint32_t i = 0; i < nCount; i++)
    {
        if ((nData & BITMAPOPMASK[i]) != 0) 
        {
            mData[nLastSlot] &= ~BITMAPOPMASK[i];
        }
        else
        {
            mData[nLastSlot] |= BITMAPOPMASK[i];
        }
    }
    
    return *this;
}

Bitmap& Bitmap::operator-=(Bitmap& bitmap)
{
    return *this - bitmap;
}

Bitmap& Bitmap::operator-(Bitmap& bitmap)
{
    assert(bitmap.GetItemCount() == GetItemCount());
    for (uint32_t i = 0; i < mSlotCount; ++i)
    {
        mData[i] &= ~bitmap[i];
    }
    RefreshSetCountByScanning();

    return *this;
}

bool Bitmap::operator==(Bitmap& bitmap)
{
    if(this == &bitmap) { return true; }
    if(mItemCount != bitmap.mItemCount) { return false; }
    if(mSlotCount != bitmap.mSlotCount) { return false; }
    if(mSetCount != bitmap.mSetCount) { return false; }
    if(mData == bitmap.mData) { return true; }
    return memcmp(mData, bitmap.mData, mSlotCount*sizeof(uint32_t)) == 0;
}

bool Bitmap::operator!=(Bitmap& bitmap)
{
    return !(*this == bitmap);
}

uint32_t Bitmap::GetSetCount(uint32_t begin, uint32_t end) const
{
    uint32_t nData;
    uint32_t setCount = 0;

    if (end >= mItemCount)
    {
        end = mItemCount - 1;
    }

    if (mData == NULL || end < begin)
    {
        return 0;
    }

    uint32_t testBeginSlot = begin / SLOT_SIZE;
    uint32_t testBitInBeginSlot = SLOT_SIZE - (begin & SLOT_SIZE_BIT_MASK);

    uint32_t testEndSlot = (end + 1) / SLOT_SIZE;
    uint32_t testBitInEndSlot = ((end + 1) & SLOT_SIZE_BIT_MASK);

    if (testBitInBeginSlot != SLOT_SIZE)
    {
        nData = mData[testBeginSlot];
        if (testBeginSlot == testEndSlot)
        {
            nData &= ( 0xFFFFFFFFU << (SLOT_SIZE - testBitInEndSlot));
        }
        nData <<= (begin & SLOT_SIZE_BIT_MASK);

        testBeginSlot++;
        while (nData != 0)
        {
            if ((nData & BITMAPOPMASK[0]) != 0)
            {
                setCount++;
            }
            nData <<= 1;
        }
    }

    if (testBeginSlot <= testEndSlot && testBitInEndSlot != 0)
    {
        nData = mData[testEndSlot];
        setCount += CountBits(nData, testBitInEndSlot);
    }

    for (uint32_t i = testBeginSlot; i < testEndSlot; ++i) 
    {
        if ((nData = mData[i]) == 0)
        {
            continue;
        }
        setCount += CountBits(nData);
    }

    return setCount;
}

bool Bitmap::HasSetData(uint32_t beginIndex, uint32_t endIndex) const
{
    assert(beginIndex <= endIndex);
    assert(endIndex < mItemCount);
    
    uint32_t quotBegin = beginIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t remBegin = beginIndex & SLOT_SIZE_BIT_MASK;
    uint32_t quotEnd = endIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t remEnd = endIndex & SLOT_SIZE_BIT_MASK;
    
    int32_t testStart = (remBegin == 0) ? (int32_t)quotBegin : (int32_t)quotBegin + 1;
    int32_t testEnd = (remEnd == SLOT_SIZE_BIT_MASK) ? (int32_t)quotEnd : (int32_t)quotEnd - 1;
    int32_t testCursor = testStart;
    while (testCursor <= testEnd)
    {
        if (mData[testCursor])
        {
            return true;
        }
        testCursor++;
    }

    if (quotBegin != quotEnd)
    {
        if (remBegin != 0)
        {
            uint32_t mask = (BITMAPOPMASK[remBegin] - 1) | BITMAPOPMASK[remBegin];
            if (mData[quotBegin] & mask)
            {
                return true;
            }
        }

        if (remEnd != SLOT_SIZE_BIT_MASK)
        {
            uint32_t mask = ~(BITMAPOPMASK[remEnd] - 1);
            if (mData[quotEnd] & mask)
            {
                return true;
            }
        }
    }
    else
    {
        if (remBegin != 0 || remEnd != SLOT_SIZE_BIT_MASK)
        {
            uint32_t mask = ((BITMAPOPMASK[remBegin] - 1) | BITMAPOPMASK[remBegin])
                            -  (BITMAPOPMASK[remEnd] - 1);;
            if (mData[quotBegin] & mask)
            {
                return true;
            }
        }
    }

    return false;
}

void Bitmap::ResetBetween(uint32_t beginIndex, uint32_t endIndex) const
{
    assert(beginIndex <= endIndex);
    assert(endIndex < mItemCount);
    
    uint32_t quotBegin = beginIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t remBegin = beginIndex & SLOT_SIZE_BIT_MASK;
    uint32_t quotEnd = endIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t remEnd = endIndex & SLOT_SIZE_BIT_MASK;
    
    int32_t testStart = (remBegin == 0) ? (int32_t)quotBegin : (int32_t)quotBegin + 1;
    int32_t testEnd = (remEnd == SLOT_SIZE_BIT_MASK) ? (int32_t)quotEnd : (int32_t)quotEnd - 1;
    int32_t testCursor = testStart;
    while (testCursor <= testEnd)
    {
        mData[testCursor] = 0;
        testCursor++;
    }

    if (quotBegin != quotEnd)
    {
        if (remBegin != 0)
        {
            uint32_t mask = (BITMAPOPMASK[remBegin] - 1) | BITMAPOPMASK[remBegin];
            mData[quotBegin] &= ~mask;
        }

        if (remEnd != SLOT_SIZE_BIT_MASK)
        {
            uint32_t mask = (BITMAPOPMASK[remEnd] - 1);
            mData[quotEnd] &= mask;
        }
    }
    else
    {
        if (remBegin != 0 || remEnd != SLOT_SIZE_BIT_MASK)
        {
            uint32_t mask = ((BITMAPOPMASK[remBegin] - 1) | BITMAPOPMASK[remBegin])
                            -  (BITMAPOPMASK[remEnd] - 1);
            mData[quotBegin] &= ~mask;
        }
    }
}

IE_NAMESPACE_END(util);

