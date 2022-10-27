#ifndef __INDEXLIB_EQUIVALENT_COMPRESS_DEFINE_H
#define __INDEXLIB_EQUIVALENT_COMPRESS_DEFINE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);

enum SlotItemType
{
    SIT_EQUAL = 1,
    SIT_DELTA_UINT8 = 0,
    SIT_DELTA_UINT16 = 2,
    SIT_DELTA_UINT32 = 4,
    SIT_DELTA_BIT1 = 5,
    SIT_DELTA_BIT2 = 6,
    SIT_DELTA_BIT4 = 7,
    SIT_DELTA_UINT64 = 3,
};

template <typename UT>
SlotItemType GetSlotItemType(UT maxDelta)
{
    if (maxDelta <= (UT)1)
    {
        return SIT_DELTA_BIT1;
    }
    if (maxDelta <= (UT)3)
    {
        return SIT_DELTA_BIT2;
    }
    if (maxDelta <= (UT)15)
    {
        return SIT_DELTA_BIT4;
    }
    if (maxDelta <= (UT)std::numeric_limits<uint8_t>::max())
    {
        return SIT_DELTA_UINT8;
    }
    if (maxDelta <= (UT)std::numeric_limits<uint16_t>::max())
    {
        return SIT_DELTA_UINT16;
    }
    if (maxDelta <= (UT)std::numeric_limits<uint32_t>::max())
    {
        return SIT_DELTA_UINT32;
    }
    return SIT_DELTA_UINT64;
}

inline SlotItemType DeltaFlagToSlotItemType(uint64_t flag)
{
    switch (flag)
    {
    case 0:
        return SIT_DELTA_UINT8;
    case 1:
        return SIT_DELTA_UINT16;
    case 2:
        return SIT_DELTA_UINT32;
    case 3:
        return SIT_DELTA_UINT64;
    case 4:
        return SIT_DELTA_BIT1;
    case 5:
        return SIT_DELTA_BIT2;
    case 6:
        return SIT_DELTA_BIT4;
    default:
        assert(false);
    }
    return SIT_EQUAL;
}

inline uint64_t SlotItemTypeToDeltaFlag(SlotItemType slotType)
{
    switch (slotType)
    {
    case SIT_DELTA_UINT8:
    {
        return 0;
    }
    case SIT_DELTA_UINT16:
    {
        return 1;
    }
    case SIT_DELTA_UINT32:
    {
        return 2;
    }
    case SIT_DELTA_UINT64:
    {
        return 3;
    }
    case SIT_DELTA_BIT1:
    {
        return 4;
    }
    case SIT_DELTA_BIT2:
    {
        return 5;
    }
    case SIT_DELTA_BIT4:
    {
        return 6;
    }
    default:
        assert(false);
    }
    return 7;
}

inline size_t GetCompressDeltaBufferSize(SlotItemType slotType, uint32_t bufferItemCount)
{
    uint32_t alignSize = 8; // 8 bit
    switch (slotType)
    {
    case SIT_DELTA_BIT1:
    {
        return (bufferItemCount + alignSize - 1) / alignSize;
    }
    case SIT_DELTA_BIT2:
    {
        return (bufferItemCount * 2 + alignSize - 1) / alignSize;
    }
    case SIT_DELTA_BIT4:
    {
        return (bufferItemCount * 4 + alignSize - 1) / alignSize;
    }
    case SIT_DELTA_UINT8:
    {
        return bufferItemCount * sizeof(uint8_t);
    }
    case SIT_DELTA_UINT16:
    {
        return bufferItemCount * sizeof(uint16_t);
    }
    case SIT_DELTA_UINT32:
    {
        return bufferItemCount * sizeof(uint32_t);
    }
    case SIT_DELTA_UINT64:
    {
        return bufferItemCount * sizeof(uint64_t);
    }
    default:
        assert(false);
    }
    return 0;
}


struct SlotItem
{
    SlotItemType slotType : 3;
    uint64_t value        : 61;
};

template <typename UT>
struct DeltaValueArrayTyped
{
    UT baseValue;
    uint8_t delta[0];
};

/* uint64_t/int64_t type only*/
struct LongSlotItem
{
    uint64_t isValue : 1;
    uint64_t value : 63;
};

/* uint64_t/int64_t type only*/
struct LongValueArrayHeader
{
    uint64_t baseValue;
    uint64_t deltaType;  // delta type
};

template <typename T>
class ItemIteratorTyped
{
public:
    ItemIteratorTyped(const T* data, uint32_t count)
        : mData(data)
        , mCount(count)
        , mCursor(0)
    {}

    bool HasNext() const
    {
        return mCursor < mCount;
    }

    T Next()
    {
        assert(mData);
        assert(mCursor < mCount);
        return mData[mCursor++];
    }

private:
    const T* mData;
    uint32_t mCount;
    uint32_t mCursor;
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EQUIVALENT_COMPRESS_DEFINE_H
