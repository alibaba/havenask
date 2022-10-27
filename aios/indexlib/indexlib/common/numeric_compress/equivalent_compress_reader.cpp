#include "indexlib/common/numeric_compress/equivalent_compress_reader.h"
#include "indexlib/file_system/file_reader.h"

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, EquivalentCompressReaderBase);

EquivalentCompressReaderBase::EquivalentCompressReaderBase(
        const uint8_t *buffer, int64_t compLen)
    : mItemCount(0)
    , mSlotBitNum(0)
    , mSlotMask(0)
    , mSlotNum(0)
    , mSlotBaseAddr(NULL)
    , mValueBaseAddr(NULL)
    , mSlotBaseOffset(0u)      
    , mValueBaseOffset(0u)      
    , mCompressLen(INVALID_COMPRESS_LEN)
    , mExpandDataBuffer(NULL)
    , mUpdateMetrics(NULL)
{
    Init(buffer, compLen);
}

EquivalentCompressReaderBase::EquivalentCompressReaderBase() 
    : mItemCount(0)
    , mSlotBitNum(0)
    , mSlotMask(0)
    , mSlotNum(0)
    , mSlotBaseAddr(NULL)
    , mValueBaseAddr(NULL)
    , mSlotBaseOffset(0u) 
    , mValueBaseOffset(0u) 
    , mCompressLen(INVALID_COMPRESS_LEN)
    , mExpandDataBuffer(NULL)
    , mUpdateMetrics(NULL)
{}

EquivalentCompressReaderBase::~EquivalentCompressReaderBase() 
{
    ARRAY_DELETE_AND_SET_NULL(mExpandDataBuffer);
}

void EquivalentCompressReaderBase::Init(
        const uint8_t *buffer, int64_t compLen,
        const BytesAlignedSliceArrayPtr& expandSliceArray)
{
    const uint8_t* cursor = buffer;
    mItemCount = *(uint32_t*)cursor;
    cursor += sizeof(uint32_t);
    mSlotBitNum = *(uint32_t*)cursor;
    mSlotMask = (1 << mSlotBitNum) - 1;
    cursor += sizeof(uint32_t);
    mSlotNum = (mItemCount + (1 << mSlotBitNum) - 1) >> mSlotBitNum;
    if (mSlotNum > 0)
    {
        mSlotBaseAddr = (uint64_t*)cursor;
        cursor += (mSlotNum * sizeof(uint64_t));
        mValueBaseAddr = (uint8_t*)cursor;
    }
    InitInplaceUpdateFlags();
    mCompressLen = compLen;
    mExpandSliceArray = expandSliceArray;
    InitUpdateMetrics();
}

bool EquivalentCompressReaderBase::Init(const IE_NAMESPACE(file_system)::FileReaderPtr& fileReader)
{
    if (!fileReader)
    {
        AUTIL_LOG(ERROR, "fileReader is NULL");
        return false;
    }
    mFileReader = fileReader;
    size_t cursor = 0;
    if (sizeof(mItemCount) != fileReader->Read(&mItemCount, sizeof(uint32_t), cursor))
    {
        return false;
    }
    cursor += sizeof(mItemCount);
    if (sizeof(mSlotBitNum) != fileReader->Read(&mSlotBitNum, sizeof(uint32_t), cursor))
    {
        return false;
    }
    cursor += sizeof(mSlotBitNum);
    mSlotMask = (1 << mSlotBitNum) - 1;
    mSlotNum = (mItemCount + (1 << mSlotBitNum) - 1) >> mSlotBitNum;
    if (mSlotNum > 0)
    {
        mSlotBaseOffset = cursor;
        cursor += (mSlotNum * sizeof(uint64_t));
        mValueBaseOffset = cursor;
    }
    return true;
}

EquivalentCompressUpdateMetrics EquivalentCompressReaderBase::GetUpdateMetrics()
{
    if (unlikely(!mUpdateMetrics))
    {
        InitUpdateMetrics();
    }

    if (mUpdateMetrics)
    {
        return *mUpdateMetrics;
    }

    static EquivalentCompressUpdateMetrics emptyUpdateMetrics;
    return emptyUpdateMetrics;
}

void EquivalentCompressReaderBase::InitInplaceUpdateFlags()
{
    for (size_t i = 0; i < 8; i++)
    {
        for (size_t j = 0; j < 8; j++)
        {
            INPLACE_UPDATE_FLAG_ARRAY[i][j] = false;
        }
    }

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_BIT1][SIT_DELTA_BIT1] = true;

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_BIT2][SIT_DELTA_BIT2] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_BIT2][SIT_DELTA_BIT1] = true;

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_BIT4][SIT_DELTA_BIT4] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_BIT4][SIT_DELTA_BIT2] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_BIT4][SIT_DELTA_BIT1] = true;

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT8][SIT_DELTA_UINT8] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT8][SIT_DELTA_BIT4] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT8][SIT_DELTA_BIT2] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT8][SIT_DELTA_BIT1] = true;

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT16][SIT_DELTA_UINT16] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT16][SIT_DELTA_UINT8] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT16][SIT_DELTA_BIT4] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT16][SIT_DELTA_BIT2] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT16][SIT_DELTA_BIT1] = true;

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT32][SIT_DELTA_UINT32] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT32][SIT_DELTA_UINT16] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT32][SIT_DELTA_UINT8] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT32][SIT_DELTA_BIT4] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT32][SIT_DELTA_BIT2] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT32][SIT_DELTA_BIT1] = true;

    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_UINT64] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_UINT32] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_UINT16] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_UINT8] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_BIT4] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_BIT2] = true;
    INPLACE_UPDATE_FLAG_ARRAY[SIT_DELTA_UINT64][SIT_DELTA_BIT1] = true;
}

void EquivalentCompressReaderBase::InitUpdateMetrics()
{
    if (!mExpandSliceArray)
    {
        return;
    }
        
    size_t sliceUsedBytes = mExpandSliceArray->GetTotalUsedBytes();
    if (sliceUsedBytes == 0)
    {
        return;
    }

    assert(sliceUsedBytes >= sizeof(EquivalentCompressUpdateMetrics));
    mUpdateMetrics = (EquivalentCompressUpdateMetrics*)mExpandSliceArray->GetValueAddress(0);
}

void EquivalentCompressReaderBase::IncreaseNoUsedMemorySize(size_t noUsedBytes)
{
    if (unlikely(!mUpdateMetrics && mExpandSliceArray))
    {
        WriteUpdateMetricsToEmptySlice();
        InitUpdateMetrics();
    }

    if (mUpdateMetrics)
    {
        mUpdateMetrics->noUsedBytesSize = 
            mUpdateMetrics->noUsedBytesSize + noUsedBytes;
    }
}

void EquivalentCompressReaderBase::IncreaseInplaceUpdateCount()
{
    if (unlikely(!mUpdateMetrics && mExpandSliceArray))
    {
        WriteUpdateMetricsToEmptySlice();
        InitUpdateMetrics();
    }

    if (mUpdateMetrics)
    {
        mUpdateMetrics->inplaceUpdateCount++;
    }
}

void EquivalentCompressReaderBase::IncreaseExpandUpdateCount()
{
    if (unlikely(!mUpdateMetrics && mExpandSliceArray))
    {
        WriteUpdateMetricsToEmptySlice();
        InitUpdateMetrics();
    }

    if (mUpdateMetrics)
    {
        mUpdateMetrics->expandUpdateCount++;
    }
}

void EquivalentCompressReaderBase::WriteUpdateMetricsToEmptySlice()
{
    if (mExpandSliceArray->GetTotalUsedBytes() != 0)
    {
        return;
    }

    EquivalentCompressUpdateMetrics updateMetrics;
    int64_t offset = mExpandSliceArray->Insert(
            &updateMetrics, sizeof(EquivalentCompressUpdateMetrics));
    if (offset != 0)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "noUsedMemoryBytes should store in slice array offset 0!");
    }
}

IE_NAMESPACE_END(common);

