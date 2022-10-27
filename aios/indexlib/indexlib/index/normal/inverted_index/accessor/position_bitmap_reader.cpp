#include "indexlib/index/normal/inverted_index/accessor/position_bitmap_reader.h"
#include "indexlib/util/bitmap.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PositionBitmapReader);

PositionBitmapReader::PositionBitmapReader()
    : mBlockCount(0)
    , mBlockOffsets(NULL)
    , mOwnBlockOffsets(false)
    , mBitmapSlots(NULL)
    , mOwnBitmapSlots(false)
    , mBitmapListSize(0)
    , mTotalBitCount(0)
    , mLastBlockIndex(-1)
    , mCurrentSlotIndex(0)
    , mPreSlotSignedBitSum(0)
{
}

PositionBitmapReader::~PositionBitmapReader() 
{
    if (mOwnBlockOffsets && mBlockOffsets)
    {
        delete []mBlockOffsets;
        mBlockOffsets = NULL;
    }
    if (mOwnBitmapSlots && mBitmapSlots)
    {
        delete []mBitmapSlots;
        mBitmapSlots = NULL;
    }
}

uint32_t PositionBitmapReader::Init(const ByteSliceList* sliceList,
                                    uint32_t offset)
{
    mSliceReader.Open(const_cast<ByteSliceList*>(sliceList));
    mSliceReader.Seek(offset);

    mBlockCount = mSliceReader.ReadVUInt32();
    mTotalBitCount = mSliceReader.ReadVUInt32();
    uint32_t bitmapSizeInByte = Bitmap::GetDumpSize(mTotalBitCount);
    assert(bitmapSizeInByte % sizeof(uint32_t) == 0);
        
    mBitmapListSize = VByteCompressor::GetVInt32Length(mBlockCount)
                      + VByteCompressor::GetVInt32Length(mTotalBitCount)
                      + mBlockCount * sizeof(uint32_t)
                      + bitmapSizeInByte;

    uint32_t blockOffsetsSizeInByte = mBlockCount * sizeof(uint32_t);
    if (mSliceReader.CurrentSliceEnough(blockOffsetsSizeInByte))
    {
        mOwnBlockOffsets = false;
        mBlockOffsets = (uint32_t*)mSliceReader.GetCurrentSliceData();
        mSliceReader.Seek(mSliceReader.Tell() + blockOffsetsSizeInByte);
    }
    else
    {
        mOwnBlockOffsets = true;
        mBlockOffsets = new uint32_t[mBlockCount];
        for (uint32_t i = 0; i < mBlockCount; ++i)
        {
            mBlockOffsets[i] = mSliceReader.ReadUInt32();
        }
    }

    if (mSliceReader.CurrentSliceEnough(bitmapSizeInByte))
    {
        mOwnBitmapSlots = false;
        mBitmapSlots = (uint32_t*)mSliceReader.GetCurrentSliceData();
        mSliceReader.Seek(mSliceReader.Tell() + bitmapSizeInByte);
    }
    else
    {
        mOwnBitmapSlots = true;
        mBitmapSlots = (uint32_t*)(new uint8_t[bitmapSizeInByte]);
        mSliceReader.Read((void*)mBitmapSlots, bitmapSizeInByte);
    }
    mBitmap.MountWithoutRefreshSetCount(mTotalBitCount, mBitmapSlots);
    
    return mTotalBitCount;
}

IE_NAMESPACE_END(index);

