#ifndef __INDEXLIB_POSITION_BITMAP_READER_H
#define __INDEXLIB_POSITION_BITMAP_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/util/numeric_util.h"
#include "indexlib/util/bitmap.h"

IE_NAMESPACE_BEGIN(index);

struct PosCountInfo
{
    uint32_t preDocAggPosCount;
    uint32_t currentDocPosCount;
};

class PositionBitmapReader
{
public:
    PositionBitmapReader();
    ~PositionBitmapReader();
public:
    uint32_t Init(const util::ByteSliceList* sliceList, uint32_t offset);
    
    inline PosCountInfo GetPosCountInfo(uint32_t seekedDocCount);

    uint32_t GetBitmapListSize() const
    {
        return mBitmapListSize;
    }
        
private:
    inline uint32_t GetKthSignedBitPos(uint32_t k);

    inline uint32_t GetNextSignedBitDistance(uint32_t begin);
    
    inline int32_t KthHighBitIdx(uint8_t value, uint32_t k);
    
    inline int32_t KthHighBitIdx(uint32_t value, uint32_t &signedBitCount,
                                 uint32_t &k);
    
    inline uint32_t PopulationCount(uint32_t value)
    {
        return PopulationCount((uint8_t)value)
            + PopulationCount((uint8_t)(value >> 8))
            + PopulationCount((uint8_t)(value >> 16))
            + PopulationCount((uint8_t)(value >> 24));            
    }

    inline uint8_t PopulationCount(uint8_t value)
    {
        static const uint8_t countTable[256] =
            {
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3,
            2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4,
            2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5,
            4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4,
            3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6,
            4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4,
            3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5,
            4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5,
            3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6,
            5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
            };
        return countTable[value];
    }
private:
    friend class BitmapPositionReaderTest;
private:
    common::ByteSliceReader mSliceReader;
    util::Bitmap mBitmap;
    uint32_t mBlockCount;
    uint32_t *mBlockOffsets;
    bool mOwnBlockOffsets;
    
    uint32_t *mBitmapSlots;
    bool mOwnBitmapSlots;
    
    uint32_t mBitmapListSize;
    uint32_t mTotalBitCount;

    int32_t mLastBlockIndex;
    uint32_t mCurrentSlotIndex;
    uint32_t mPreSlotSignedBitSum;
private:
    IE_LOG_DECLARE();
};

inline PosCountInfo PositionBitmapReader::GetPosCountInfo(
        uint32_t seekedDocCount)
{
    PosCountInfo info;
    info.preDocAggPosCount = GetKthSignedBitPos(seekedDocCount);
    info.currentDocPosCount = GetNextSignedBitDistance(
            info.preDocAggPosCount + 1);
    return info;
}

inline int32_t PositionBitmapReader::KthHighBitIdx(uint8_t value, uint32_t k)
{
    for (int32_t i = 7; i >= 0 ; --i)
    {
        if (value & (1 << i))
        {
            k--;
            if (k == 0)
            {
                return i;
            }
        }
    }
    return 0;
}

inline int32_t PositionBitmapReader::KthHighBitIdx(uint32_t value,
        uint32_t &signedBitCount, uint32_t &k)
{
    uint32_t totalCount = 0;
    for (int32_t shift = 24; shift >= 0; shift -= 8)
    {
        uint8_t byte = value >> shift;
        uint32_t count = PopulationCount(byte);
        totalCount += count;
        if (count >= k)
        {
            return KthHighBitIdx(byte, k) + shift;
        }
        else
        {
            k -= count;
        }
    }
    signedBitCount += totalCount;
    return -1;
}

inline uint32_t PositionBitmapReader::GetKthSignedBitPos(uint32_t k)
{
    uint32_t position = 0;
    uint32_t blockIndex = (k - 1) / MAX_DOC_PER_BITMAP_BLOCK;
    assert(blockIndex <= mBlockCount);

    mLastBlockIndex = blockIndex;
    uint32_t blockBeginPos = blockIndex == 0
                             ? 0 : mBlockOffsets[blockIndex - 1] + 1;

    mCurrentSlotIndex = blockBeginPos >> util::Bitmap::SLOT_SIZE_BIT_NUM; 
    uint32_t bitmapSlot = mBitmapSlots[mCurrentSlotIndex];
    // since we treat bitmap as 32 bit integers, so when a block's begin
    // position not aligned to 32, we will have som extra bit belong to
    // pre block
    uint32_t extraBit = blockBeginPos & util::Bitmap::SLOT_SIZE_BIT_MASK;

    mPreSlotSignedBitSum =
        blockIndex << MAX_DOC_PER_BITMAP_BLOCK_BIT_NUM;
    if (extraBit != 0)
    {
        mPreSlotSignedBitSum -= PopulationCount(
                bitmapSlot >> (util::Bitmap::SLOT_SIZE - extraBit));
    }
    position = blockBeginPos - extraBit;
    uint32_t remainSignedBit = k - mPreSlotSignedBitSum;

    while (true)
    {
        // attempt to find kth high signed bit in this slot
        // if we can't find, we will skip to next slot, and add current
        // slot's signed bit count to mPreSlotSignedBitSum, and subtract
        // it from remain signed bit count
        int32_t bitPos = KthHighBitIdx(bitmapSlot, mPreSlotSignedBitSum,
                remainSignedBit);
        if (bitPos == -1)
        {
            position += util::Bitmap::SLOT_SIZE;
            bitmapSlot = mBitmapSlots[++mCurrentSlotIndex];
        }
        else
        {
            position += util::Bitmap::SLOT_SIZE - bitPos - 1;
            break;
        }
    }
    return position;
}

inline uint32_t PositionBitmapReader::GetNextSignedBitDistance(
        uint32_t begin)
{
    uint32_t dist = 1;
    uint32_t curBit = begin;
    uint32_t blockEndPos = mLastBlockIndex == (int32_t)mBlockCount
                           ? mTotalBitCount - 1
                           : mBlockOffsets[mLastBlockIndex];

    while (curBit <= blockEndPos)
    {
        if (mBitmap.Test(curBit))
        {
            break;
        }
        dist++;
        curBit++;
    }
    return dist;
}

DEFINE_SHARED_PTR(PositionBitmapReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSITION_BITMAP_READER_H
