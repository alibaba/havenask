#ifndef __INDEXLIB_EQUIVALENT_COMPRESS_WRITER_H
#define __INDEXLIB_EQUIVALENT_COMPRESS_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/slice_array/byte_aligned_slice_array.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/common/numeric_compress/equivalent_compress_reader.h"

IE_NAMESPACE_BEGIN(common);

class EquivalentCompressWriterBase
{
public:
    typedef util::AlignedSliceArray<uint64_t> IndexArray;
    typedef util::ByteAlignedSliceArray       DataArray;
    DEFINE_SHARED_PTR(IndexArray);
    DEFINE_SHARED_PTR(DataArray);

public:
    EquivalentCompressWriterBase(autil::mem_pool::PoolBase *pool = NULL)
        : mSlotItemPowerNum(0)
        , mSlotItemCount(0)
        , mItemCount(0)
        , mDataArrayCursor(0)
        , mIndexArray(NULL)
        , mDataArray(NULL)
        , mPool(pool)
    {}

    virtual ~EquivalentCompressWriterBase()
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mIndexArray);
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mDataArray);
    }

public:
    void Init(uint32_t slotItemCount)
    {
        Reset();
        mSlotItemPowerNum = GetUpperPowerNumber(slotItemCount);
        mSlotItemCount = (1 << mSlotItemPowerNum);
        mIndexArray = IE_POOL_COMPATIBLE_NEW_CLASS(mPool, IndexArray, 
                INDEX_SLICE_LEN, 1, NULL, mPool);
        mDataArray = IE_POOL_COMPATIBLE_NEW_CLASS(mPool, DataArray,
                DATA_SLICE_LEN, 1, NULL, mPool);
        InitSlotBuffer();
    }

    size_t GetCompressLength()
    {
        FlushSlotBuffer();
        size_t compLen = 2 * sizeof(uint32_t);
        if (mItemCount == 0)
        {
            return compLen;
        }

        uint32_t slotCount = (mItemCount + mSlotItemCount - 1) / mSlotItemCount;
        compLen += (slotCount * sizeof(uint64_t));
        compLen += mDataArrayCursor;
        return compLen;
    }

    size_t DumpBuffer(uint8_t *buffer, size_t bufferLen)
    {
        FlushSlotBuffer();
        assert(bufferLen >= GetCompressLength());

        uint8_t* cursor = buffer;
        *(uint32_t*)cursor = mItemCount;
        cursor += sizeof(uint32_t);
        *(uint32_t*)cursor = mSlotItemPowerNum;
        cursor += sizeof(uint32_t);

        for (uint32_t i = 0; i < mIndexArray->GetSliceNum(); ++i)
        {
            uint32_t sliceLen = mIndexArray->GetSliceDataLen(i);
            uint64_t* slice = mIndexArray->GetSlice(i);
            memcpy(cursor, slice, sliceLen * sizeof(uint64_t));
            cursor += (sliceLen * sizeof(uint64_t));
        }
        for (uint32_t i = 0; i < mDataArray->GetSliceNum(); ++i)
        {
            uint32_t sliceLen = mDataArray->GetSliceDataLen(i);
            char* slice = mDataArray->GetSlice(i);
            memcpy(cursor, slice, sliceLen);
            cursor += sliceLen;
        }
        return cursor - buffer;
    }

    void Dump(const file_system::FileWriterPtr& file)
    {
        FlushSlotBuffer();

        file->Write(&mItemCount, sizeof(uint32_t));
        file->Write(&mSlotItemPowerNum, sizeof(uint32_t));
        for (uint32_t i = 0; i < mIndexArray->GetSliceNum(); ++i)
        {
            uint32_t sliceLen = mIndexArray->GetSliceDataLen(i);
            uint64_t* slice = mIndexArray->GetSlice(i);
            file->Write(slice, sliceLen * sizeof(uint64_t));
        }
        for (uint32_t i = 0; i < mDataArray->GetSliceNum(); ++i)
        {
            uint32_t sliceLen = mDataArray->GetSliceDataLen(i);
            char* slice = mDataArray->GetSlice(i);
            file->Write(slice, sliceLen);
        }
    }

protected:
    virtual void InitSlotBuffer() = 0;
    virtual void FreeSlotBuffer() = 0;
    virtual void FlushSlotBuffer() = 0;

    void Reset() 
    {
        FreeSlotBuffer();
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mIndexArray);
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mDataArray);
        mSlotItemPowerNum = 0;
        mSlotItemCount = 0;
        mItemCount = 0;
        mDataArrayCursor = 0;
    }

    static uint32_t GetUpperPowerNumber(uint32_t slotItemCount)
    {
        uint32_t powerNum = 6;   // min slotItemNum is 2^6 = 64
        while ((uint32_t)(1 << powerNum) < slotItemCount)
        {
            ++powerNum;
        }
        return powerNum;
    }

private:
    const static uint32_t INDEX_SLICE_LEN = 8 * 1024;
    const static uint32_t DATA_SLICE_LEN  = 256 * 1024;

protected:
    uint32_t      mSlotItemPowerNum;
    uint32_t      mSlotItemCount;
    uint32_t      mItemCount;
    size_t        mDataArrayCursor;
    IndexArray*   mIndexArray;
    DataArray*    mDataArray;
    autil::mem_pool::PoolBase* mPool;

protected:
    friend class EquivalentCompressWriterTest;
};

DEFINE_SHARED_PTR(EquivalentCompressWriterBase);

///////////////////////////////////////////////////////////

template <typename T>
class EquivalentCompressWriter : public EquivalentCompressWriterBase
{
public:
    typedef typename EquivalentCompressTraits<T>::ZigZagEncoder ZigZagEncoder;
    typedef typename EquivalentCompressTraits<T>::EncoderType UT;

public:
    EquivalentCompressWriter(autil::mem_pool::PoolBase *pool = NULL)
        : EquivalentCompressWriterBase(pool)
        , mSlotBuffer(NULL)
        , mCursorInBuffer(0)
    {}

    ~EquivalentCompressWriter() 
    {
        FreeSlotBuffer();
    }

public:
    // could be called repeatedly before calling FlushSlotBuffer
    //    eg: GetCompressLength / DumpBuffer/ DumpFile / Dump
    void CompressData(T* data, uint32_t count);

    void CompressData(const std::vector<T> &dataVec)
    {
        CompressData((T*)dataVec.data(), dataVec.size());
    }

    void CompressData(const EquivalentCompressReader<T> &reader);

public:
    static size_t CalculateCompressLength(
            T* data, uint32_t count, uint32_t slotItemCount)
    {
        ItemIteratorTyped<T> itemIter(data, count);
        return CalculateCompressLength(itemIter, slotItemCount);
    }

    static size_t CalculateCompressLength(
            const std::vector<T> &dataVec, uint32_t slotItemCount)
    {
        ItemIteratorTyped<T> itemIter((const T*)dataVec.data(), 
                (uint32_t)dataVec.size());
        return CalculateCompressLength(itemIter, slotItemCount);
    }

    static size_t CalculateCompressLength(
            const EquivalentCompressReader<T> &reader, 
            uint32_t slotItemCount)
    {
        return CalculateCompressLength(reader.CreateIterator(), slotItemCount);
    }

    template <typename ItemIterator>
    static size_t CalculateCompressLength(
            ItemIterator iter, uint32_t slotItemCount)
    {
        size_t compSize = sizeof(uint32_t) * 2;
        uint32_t powerNum = 
            EquivalentCompressWriter<T>::GetUpperPowerNumber(slotItemCount);
        slotItemCount = (1 << powerNum);

        UT * buffer = new UT[slotItemCount];
        uint32_t cursorInBuffer = 0;

        uint32_t count = 0;
        while(iter.HasNext())
        {
            buffer[cursorInBuffer++] = ZigZagEncoder::Encode(iter.Next());
            if (cursorInBuffer == slotItemCount)
            {
                compSize += CalculateDeltaBufferSize(buffer, cursorInBuffer);
                cursorInBuffer = 0;
            }
            ++count;
        }

        if (cursorInBuffer > 0)
        {
            compSize += CalculateDeltaBufferSize(buffer, cursorInBuffer);        
        }
        delete []buffer;

        // calculate slotItems size
        uint32_t slotMask = (1 << powerNum) - 1;
        uint32_t slotCount = (count + slotMask) >> powerNum;
        compSize += sizeof(uint64_t) * slotCount;
        return compSize;
    }

protected:
    void InitSlotBuffer() override
    {
        assert(!mSlotBuffer);
        mSlotBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, UT, mSlotItemCount);
        mCursorInBuffer = 0;
    }

    void FreeSlotBuffer() override
    {
        IE_POOL_COMPATIBLE_DELETE_VECTOR(mPool, mSlotBuffer, mSlotItemCount);
        mCursorInBuffer = 0;
    }

    void FlushSlotBuffer() override
    {
        if (mCursorInBuffer == 0)
        {
            return;
        }

        UT minValue = 0;
        UT maxDelta = GetMaxDeltaInBuffer(mSlotBuffer, mCursorInBuffer, minValue);
        if (NeedStoreDelta(maxDelta, minValue))
        {
            ConvertToDeltaBuffer(mSlotBuffer, mCursorInBuffer, minValue);
            SlotItemType slotType = GetSlotItemType(maxDelta);
            AppendSlotItem(slotType, mDataArrayCursor);
            mDataArrayCursor += FlushDeltaBuffer(mSlotBuffer, mCursorInBuffer, 
                    minValue, slotType);
        }
        else
        {
            AppendSlotItem(SIT_EQUAL, minValue);
        }
        mItemCount += mCursorInBuffer;
        mCursorInBuffer = 0;
    }

private:
    static UT GetMaxDeltaInBuffer(UT* slotBuffer, uint32_t itemCount, UT &minValue)
    {
        assert(itemCount > 0);

        UT targetValue = slotBuffer[0];
        UT baseValue = targetValue;
        UT maxDelta  = 0;

        for (size_t idx = 1; idx < itemCount; ++idx)
        {
            UT curValue = slotBuffer[idx];
            if (curValue != targetValue)
            {
                if (curValue <= baseValue)
                {
                    maxDelta += (baseValue - curValue);
                    baseValue = curValue;
                }
                else
                {
                    UT delta = curValue - baseValue;
                    if (delta > maxDelta)
                    {
                        maxDelta = delta;
                    }
                }
            }
        }
        minValue = baseValue;
        return maxDelta;
    }

    static void ConvertToDeltaBuffer(UT *slotBuffer, uint32_t itemCount, UT minValue)
    {
        for (size_t i = 0; i < itemCount; ++i)
        {
            assert(slotBuffer[i] >= minValue);
            slotBuffer[i] = slotBuffer[i] - minValue;
        }
    }

    static size_t GetMinDeltaBufferSize(UT maxDelta, uint32_t bufferItemCount)
    {
        SlotItemType slotType = GetSlotItemType(maxDelta);
        return GetCompressDeltaBufferSize(slotType, bufferItemCount);
    }

    static size_t CalculateDeltaBufferSize(UT *buffer, uint32_t cursorInBuffer);

    static bool NeedStoreDelta(UT maxDelta, UT minValue);

    size_t FlushDeltaBuffer(UT *deltaBuffer, uint32_t itemCount, 
                            UT minValue, SlotItemType slotType);

    size_t CompressDeltaBuffer(UT *deltaBuffer, uint32_t itemCount, 
                               SlotItemType slotType);

    void AppendSlotItem(SlotItemType slotType, uint64_t value);

    // for long value
    static size_t CalculateLongValueDeltaBufferSize(
            UT *buffer, uint32_t cursorInBuffer);

    static bool NeedStoreLongValueDelta(UT maxDelta, UT minValue);

    size_t FlushLongValueDeltaBuffer(UT *deltaBuffer, uint32_t itemCount, 
            UT minValue, SlotItemType slotType);
    void AppendLongValueSlotItem(SlotItemType slotType, uint64_t value);

private:
    UT* mSlotBuffer;
    uint32_t mCursorInBuffer;

private:
    friend class EquivalentCompressReader<T>;
    friend class EquivalentCompressWriterTest;
};

///////////////////////////////////////////////////////////////
template <typename T>
inline size_t EquivalentCompressWriter<T>::CompressDeltaBuffer(
        UT *deltaBuffer, uint32_t itemCount, SlotItemType slotType)
{
    for (uint32_t i = 0; i < itemCount; i++)
    {
        EquivalentCompressReader<T>::InplaceUpdateDeltaArray(
                (uint8_t*)deltaBuffer, slotType, i, deltaBuffer[i]);
    }
    return GetCompressDeltaBufferSize(slotType, itemCount);
}

template <typename T>
inline size_t EquivalentCompressWriter<T>::FlushDeltaBuffer(
        UT *deltaBuffer, uint32_t itemCount, 
        UT minValue, SlotItemType slotType)
{
    size_t idx = 0;
    mDataArray->SetTypedValue<UT>(mDataArrayCursor + idx, minValue);
    idx += sizeof(UT);

    size_t compsize = CompressDeltaBuffer(deltaBuffer, itemCount, slotType);
    mDataArray->SetList(mDataArrayCursor + idx,
                        (const char*)deltaBuffer, compsize);
    idx += compsize;
    return idx;
}

template <typename T>
inline size_t EquivalentCompressWriter<T>::FlushLongValueDeltaBuffer(
        UT *deltaBuffer, uint32_t itemCount, 
        UT minValue, SlotItemType slotType)
{
#define DumpDeltaFlag(DELTA_FLAG)                                       \
    UT deltaFlag = DELTA_FLAG;                                          \
    mDataArray->SetTypedValue<UT>(mDataArrayCursor + idx, deltaFlag);   \
    idx += sizeof(UT);                                                  \

    size_t idx = 0;
    mDataArray->SetTypedValue<UT>(mDataArrayCursor + idx, minValue);
    idx += sizeof(UT);
    DumpDeltaFlag(SlotItemTypeToDeltaFlag(slotType));

    size_t compsize = CompressDeltaBuffer(deltaBuffer, itemCount, slotType);
    mDataArray->SetList(mDataArrayCursor + idx,
                        (const char*)deltaBuffer, compsize);
    idx += compsize;
    return idx;
}

template <>
inline size_t EquivalentCompressWriter<uint64_t>::FlushDeltaBuffer(
        uint64_t *deltaBuffer, uint32_t itemCount, 
        uint64_t minValue, SlotItemType slotType)
{
    return FlushLongValueDeltaBuffer(deltaBuffer, itemCount, minValue, slotType);
}

template <>
inline size_t EquivalentCompressWriter<int64_t>::FlushDeltaBuffer(
        uint64_t *deltaBuffer, uint32_t itemCount, 
        uint64_t minValue, SlotItemType slotType)
{
    return FlushLongValueDeltaBuffer(deltaBuffer, itemCount, minValue, slotType);
}

template <>
inline size_t EquivalentCompressWriter<double>::FlushDeltaBuffer(
        uint64_t *deltaBuffer, uint32_t itemCount, 
        uint64_t minValue, SlotItemType slotType)
{
    return FlushLongValueDeltaBuffer(deltaBuffer, itemCount, minValue, slotType);
}

template <typename T>
inline bool EquivalentCompressWriter<T>::NeedStoreDelta(UT maxDelta, UT minValue)
{
    return (maxDelta > 0);
}

template <typename T>
inline bool EquivalentCompressWriter<T>::NeedStoreLongValueDelta(UT maxDelta, UT minValue)
{
    return (maxDelta > 0 || minValue > (uint64_t)std::numeric_limits<int64_t>::max());
}

template <>
inline bool EquivalentCompressWriter<uint64_t>::NeedStoreDelta(
        uint64_t maxDelta, uint64_t minValue)
{
    return NeedStoreLongValueDelta(maxDelta, minValue);
}

template <>
inline bool EquivalentCompressWriter<int64_t>::NeedStoreDelta(
        uint64_t maxDelta, uint64_t minValue)
{
    return NeedStoreLongValueDelta(maxDelta, minValue);
}

template <>
inline bool EquivalentCompressWriter<double>::NeedStoreDelta(
        uint64_t maxDelta, uint64_t minValue)
{
    return NeedStoreLongValueDelta(maxDelta, minValue);
}

template <typename T>
inline void EquivalentCompressWriter<T>::AppendSlotItem(SlotItemType slotType, uint64_t value)
{
    union SlotItemUnion
    {
        SlotItem slotItem;
        uint64_t slotValue;
    };

    SlotItemUnion slotUnion;
    slotUnion.slotItem.slotType = slotType;
    slotUnion.slotItem.value = value;
    mIndexArray->Set(mItemCount >> mSlotItemPowerNum, slotUnion.slotValue);
}

template <typename T>
inline void EquivalentCompressWriter<T>::AppendLongValueSlotItem(
        SlotItemType slotType, uint64_t value)
{
    union SlotItemUnion
    {
        LongSlotItem slotItem;
        uint64_t slotValue;
    };

    SlotItemUnion slotUnion;
    slotUnion.slotItem.isValue = (slotType == SIT_EQUAL) ? 1 : 0;
    slotUnion.slotItem.value = value;
    mIndexArray->Set(mItemCount >> mSlotItemPowerNum, slotUnion.slotValue);
}

template <>
inline void EquivalentCompressWriter<uint64_t>::AppendSlotItem(
        SlotItemType slotType, uint64_t value)
{
    AppendLongValueSlotItem(slotType, value);
}

template <>
inline void EquivalentCompressWriter<int64_t>::AppendSlotItem(
        SlotItemType slotType, uint64_t value)
{
    AppendLongValueSlotItem(slotType, value);
}

template <>
inline void EquivalentCompressWriter<double>::AppendSlotItem(
        SlotItemType slotType, uint64_t value)
{
    AppendLongValueSlotItem(slotType, value);
}

template <typename T>
inline void EquivalentCompressWriter<T>::CompressData(
        T* data, uint32_t count)
{
    for (uint32_t idx = 0; idx < count; ++idx)
    {
        assert(mCursorInBuffer < mSlotItemCount);
        mSlotBuffer[mCursorInBuffer++] = ZigZagEncoder::Encode(data[idx]);
        if (mCursorInBuffer == mSlotItemCount)
        {
            FlushSlotBuffer();
        }
    }
}

template <typename T>
inline void EquivalentCompressWriter<T>::CompressData(
        const EquivalentCompressReader<T> &reader)
{
    uint32_t count = reader.Size();
    for (size_t idx = 0; idx < count; ++idx)
    {
        assert(mCursorInBuffer < mSlotItemCount);
        mSlotBuffer[mCursorInBuffer++] = ZigZagEncoder::Encode(reader[idx]);
        if (mCursorInBuffer == mSlotItemCount)
        {
            FlushSlotBuffer();
        }
    }
}

template <typename T>
inline size_t EquivalentCompressWriter<T>::CalculateDeltaBufferSize(
        UT *buffer, uint32_t bufferSize)
{
    UT minValue = 0;
    UT maxDelta = 
        GetMaxDeltaInBuffer(buffer, bufferSize, minValue);

    if (!NeedStoreDelta(maxDelta, minValue))
    {
        return 0;
    }

    return sizeof(UT) + GetMinDeltaBufferSize(maxDelta, bufferSize);
}

template <typename T>
inline size_t EquivalentCompressWriter<T>::CalculateLongValueDeltaBufferSize(
        UT *buffer, uint32_t cursorInBuffer)
{
    UT minValue = 0;
    UT maxDelta = 
        GetMaxDeltaInBuffer(buffer, cursorInBuffer, minValue);

    if (!NeedStoreDelta(maxDelta, minValue))
    {
        return 0;
    }
    return sizeof(UT)*2 + GetMinDeltaBufferSize(maxDelta, cursorInBuffer);
}

template <>
inline size_t EquivalentCompressWriter<uint64_t>::CalculateDeltaBufferSize(
        uint64_t *buffer, uint32_t cursorInBuffer)
{
    return CalculateLongValueDeltaBufferSize(buffer, cursorInBuffer);
}

template <>
inline size_t EquivalentCompressWriter<int64_t>::CalculateDeltaBufferSize(
        uint64_t *buffer, uint32_t cursorInBuffer)
{
    return CalculateLongValueDeltaBufferSize(buffer, cursorInBuffer);
}

template <>
inline size_t EquivalentCompressWriter<double>::CalculateDeltaBufferSize(
        uint64_t *buffer, uint32_t cursorInBuffer)
{
    return CalculateLongValueDeltaBufferSize(buffer, cursorInBuffer);
}

////////////////////////////////not support type/////////////////////
template <>
inline size_t EquivalentCompressWriter<autil::uint128_t>::CalculateDeltaBufferSize(
        autil::uint128_t *buffer, uint32_t cursorInBuffer)
{
    assert(false);
    return 0;
}

template <>
inline void EquivalentCompressWriter<autil::uint128_t>::CompressData(autil::uint128_t* data, uint32_t count)
{
    assert(false);
}

template <>
inline void EquivalentCompressWriter<autil::uint128_t>::CompressData(
        const EquivalentCompressReader<autil::uint128_t> &reader)
{
    assert(false);
}

template <>
inline size_t EquivalentCompressWriter<autil::uint128_t>::CalculateCompressLength(
        autil::uint128_t* data, uint32_t count, 
        uint32_t slotItemCount)
{
    assert(false);
    return 0;
}

template <>
inline size_t EquivalentCompressWriter<autil::uint128_t>::CalculateCompressLength(
        const EquivalentCompressReader<autil::uint128_t> &reader, 
        uint32_t slotItemCount)
{
    assert(false);
    return 0;
}

template <>
inline void EquivalentCompressWriter<autil::uint128_t>::FlushSlotBuffer()
{
    assert(false);
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EQUIVALENT_COMPRESS_WRITER_H
