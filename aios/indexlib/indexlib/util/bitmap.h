#ifndef __INDEXLIB_BITMAP_H
#define __INDEXLIB_BITMAP_H

#include <tr1/memory>
#include "indexlib/common_define.h"

#include "indexlib/indexlib.h"
#include "indexlib/util/numeric_util.h"
#include <autil/mem_pool/Pool.h>

IE_NAMESPACE_BEGIN(util);

class Bitmap
{
public:
    Bitmap(bool bSet = false,
           autil::mem_pool::PoolBase* pool = NULL);
    Bitmap(uint32_t nItemCount, bool bSet = false,
           autil::mem_pool::PoolBase* pool = NULL);
    Bitmap(const Bitmap& rhs);
    Bitmap& operator=(const Bitmap& rhs);
    virtual ~Bitmap();

public:
    bool Alloc(uint32_t nItemCount, bool bSet = false);
    void Clear();

    uint32_t Begin() const;
    uint32_t Next(uint32_t nIndex) const;
    void Set(uint32_t nIndex);
    void Reset(uint32_t nIndex);
    void ResetAll();
    void ResetAllAfter(uint32_t nIndex);
    uint32_t GetItemCount() const { return mItemCount; }
    virtual uint32_t GetValidItemCount() const { return mItemCount; }
    uint32_t Size() const;

    uint32_t GetSetCount() const;
    uint32_t GetSetCount(uint32_t begin, uint32_t end) const;

    void SetSetCount(uint32_t nCount);
    uint32_t GetUnsetCount() const;
    uint32_t GetSlotCount() const {return mSlotCount;}
    uint32_t* GetData() const {return mData; }
    void Mount(uint32_t nItemCount, uint32_t* pData, bool doNotDelete = true);

    // copy bitmap in *data to *mData from startIndex
    void Copy(uint32_t startIndex, uint32_t *data, uint32_t itemCount);
    void CopySlice(uint32_t index, uint32_t *data, 
                   uint32_t startIndex, uint32_t itemCount);

    // not refreshing setcount in read-only mode
    // can improve performance
    void MountWithoutRefreshSetCount(uint32_t nItemCount, uint32_t* pData, 
            bool doNotDelete = true);

    inline bool Test(uint32_t nIndex) const;
    inline bool IsEmpty() const;
    inline uint32_t CountBits(uint32_t value) const;
    inline uint32_t CountBits(uint32_t value, uint32_t validCount) const;
    bool HasSetData(uint32_t beginIndex, uint32_t endIndex) const;
    void ResetBetween(uint32_t beginIndex, uint32_t endIndex) const;

    Bitmap* Clone() const;

private:
    void Copy(uint32_t *dstSlotAddr, uint32_t startIndex, 
              uint32_t *data, uint32_t itemCount);

public:
    uint32_t& operator[](uint32_t i);
    Bitmap& operator&(Bitmap& bitmap);
    Bitmap& operator&=(Bitmap& bitmap);
    Bitmap& operator|(const Bitmap& bitmap);
    Bitmap& operator|=(const Bitmap& bitmap);
    Bitmap& operator~();
    Bitmap& operator-=(Bitmap& bitmap);
    Bitmap& operator-(Bitmap& bitmap);
    bool operator==(Bitmap& bitmap);
    bool operator!=(Bitmap& bitmap);
protected:
    friend class BitmapTest;
    void  RefreshSetCountByScanning() const;

public:
    static const uint32_t BYTE_SLOT_NUM = 8;
    static const uint32_t SLOT_SIZE = BYTE_SLOT_NUM * sizeof(uint32_t);
    static const uint32_t SLOT_SIZE_BIT_NUM = 5;
    static const uint32_t SLOT_SIZE_BIT_MASK = 0x1F;
    static const uint32_t BITMAPOPMASK[SLOT_SIZE];
    static const uint32_t INVALID_INDEX = 0xFFFFFFFF;

    static int32_t GetSlotCount(int32_t docCount)
    {
        return (docCount + SLOT_SIZE - 1) / SLOT_SIZE;
    }

    static uint32_t GetDumpSize(uint32_t bitCount) 
    {
        return NumericUtil::UpperPack(bitCount, SLOT_SIZE) / BYTE_SLOT_NUM;
    }

protected:
    uint32_t mItemCount;
    uint32_t mSlotCount;
    uint32_t* mData;
    autil::mem_pool::PoolBase* mPool;
    mutable uint32_t mSetCount;
    bool mMount;
    bool mInitSet;

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<Bitmap> BitmapPtr;

///////////////////////////////////////////////////
//inline functions
inline bool Bitmap::Test(uint32_t nIndex) const
{
    assert(nIndex < mItemCount);

    uint32_t quot = nIndex >> SLOT_SIZE_BIT_NUM;
    uint32_t rem = nIndex & SLOT_SIZE_BIT_MASK;
    
    return (mData[quot] & BITMAPOPMASK[rem]) != 0;
}

inline bool Bitmap::IsEmpty() const 
{
    return mSetCount == 0;
}

inline uint32_t Bitmap::CountBits(uint32_t value) const
{
    value = (value & 0x55555555) + ((value >> 1) & 0x55555555);
    value = (value & 0x33333333) + ((value >> 2) & 0x33333333);
    value = (value & 0x0f0f0f0f) + ((value >> 4) & 0x0f0f0f0f);
    value = (value & 0x00ff00ff) + ((value >> 8) & 0x00ff00ff);
    value = (value & 0x0000ffff) + ((value >> 16) & 0x0000ffff);
    return value;
}

inline uint32_t Bitmap::CountBits(uint32_t value, uint32_t validCount) const
{
    uint32_t count = 0;
    for (uint32_t i = 0; value != 0 && i < validCount; i++)
    {
        if ((value & BITMAPOPMASK[0]) != 0)
        {
            count++;
        }
        value <<= 1;
    }
    return count;
}

inline void Bitmap::MountWithoutRefreshSetCount(
        uint32_t nItemCount, 
        uint32_t* pData, 
        bool doNotDelete)
{
    if (!mMount)
        assert(mData==NULL);
    mData = pData;
    mMount = doNotDelete;
    mItemCount = nItemCount;
    mSlotCount = (nItemCount + SLOT_SIZE - 1) >> 5;
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BITMAP_H
