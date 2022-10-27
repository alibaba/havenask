#ifndef __INDEXLIB_EQUIVALENT_COMPRESS_READER_H
#define __INDEXLIB_EQUIVALENT_COMPRESS_READER_H

#include <tr1/memory>
#include <autil/LongHashValue.h>
#include <autil/MultiValueType.h>
#include <autil/CountedMultiValueType.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/util/slice_array/bytes_aligned_slice_array.h"
#include "indexlib/common/numeric_compress/equivalent_compress_traits.h"
#include "indexlib/common/numeric_compress/equivalent_compress_define.h"

DECLARE_REFERENCE_CLASS(file_system, FileReader);
IE_NAMESPACE_BEGIN(common);

struct EquivalentCompressUpdateMetrics
{
public:
    EquivalentCompressUpdateMetrics()
        : noUsedBytesSize(0)
        , inplaceUpdateCount(0)
        , expandUpdateCount(0)
    {}

    bool operator==(const EquivalentCompressUpdateMetrics& other) const
    {
        return noUsedBytesSize == other.noUsedBytesSize
            && inplaceUpdateCount == other.inplaceUpdateCount
            && expandUpdateCount == other.expandUpdateCount;
    }

    size_t noUsedBytesSize;
    uint32_t inplaceUpdateCount;
    uint32_t expandUpdateCount;
};

class EquivalentCompressReaderBase
{
public:
    // std::numeric_limits<uint64_t>::max();
    static const uint64_t INVALID_COMPRESS_LEN = 0xFFFFFFFFFFFFFFFF;

public:
    EquivalentCompressReaderBase(const uint8_t *buffer, 
                                 int64_t compLen = INVALID_COMPRESS_LEN);
    EquivalentCompressReaderBase();
    virtual ~EquivalentCompressReaderBase();

    uint32_t Size() const { return mItemCount; }
    uint32_t GetSlotItemCount() const { return (uint32_t)1 << mSlotBitNum; }
    
    void Init(const uint8_t *buffer, int64_t compLen = INVALID_COMPRESS_LEN,
              const util::BytesAlignedSliceArrayPtr& expandSliceArray = 
              util::BytesAlignedSliceArrayPtr());
    bool Init(const IE_NAMESPACE(file_system)::FileReaderPtr& fileReader);

    bool IsReadFromMemory() const  __ALWAYS_INLINE { return !mFileReader;}
    
    EquivalentCompressUpdateMetrics GetUpdateMetrics();

    size_t GetSlotOffset(size_t pos) const __ALWAYS_INLINE
    {
        return pos >> mSlotBitNum;
    }

    void DecodePosition(size_t pos, size_t& slotIdx, size_t& valueIdx)
    {
        slotIdx = pos >> mSlotBitNum;
        valueIdx = pos & mSlotMask;
    }


    inline SlotItem GetSlotItem(size_t slotIdx) const __ALWAYS_INLINE
    {
        SlotItem slotItemVal;
        if (mSlotBaseAddr)
        {
            slotItemVal = *((SlotItem*)mSlotBaseAddr + slotIdx);
            return slotItemVal;
        }
        assert(mFileReader);
        mFileReader->Read(&slotItemVal, sizeof(SlotItem),
                          mSlotBaseOffset + slotIdx * sizeof(SlotItem));
        return slotItemVal;
    }

    inline LongSlotItem GetLongSlotItem(size_t slotIdx) const __ALWAYS_INLINE
    {
        LongSlotItem slotItemVal;
        if (mSlotBaseAddr)
        {
            slotItemVal = *((LongSlotItem*)mSlotBaseAddr + slotIdx);
            return slotItemVal;
        }
        assert(mFileReader);
        mFileReader->Read(&slotItemVal, sizeof(LongSlotItem),
                          mSlotBaseOffset + slotIdx  * sizeof(LongSlotItem));
        return slotItemVal;        
    }            
    
protected:
    void InitInplaceUpdateFlags();


    inline uint8_t* GetDeltaBlockAddress(size_t offset) const __ALWAYS_INLINE
    {
        if (offset < mCompressLen)
        {
            return mValueBaseAddr + offset;
        }

        size_t expandOffset = offset - mCompressLen;
        assert(mExpandSliceArray);
        return (uint8_t*)mExpandSliceArray->GetValueAddress(expandOffset);
    }

    void InitUpdateMetrics();
    void WriteUpdateMetricsToEmptySlice();

    void IncreaseNoUsedMemorySize(size_t noUsedBytes);
    void IncreaseInplaceUpdateCount();
    void IncreaseExpandUpdateCount();

protected:
    uint32_t mItemCount;
    uint32_t mSlotBitNum;
    uint32_t mSlotMask;
    uint32_t mSlotNum;
    uint64_t *mSlotBaseAddr;
    uint8_t  *mValueBaseAddr;
    uint64_t mSlotBaseOffset;
    uint64_t mValueBaseOffset;
    IE_NAMESPACE(file_system)::FileReaderPtr mFileReader;
    uint64_t mCompressLen;
    util::BytesAlignedSliceArrayPtr mExpandSliceArray;
    uint8_t* mExpandDataBuffer;

    EquivalentCompressUpdateMetrics* mUpdateMetrics;

    bool INPLACE_UPDATE_FLAG_ARRAY[8][8];

protected:
    friend class EquivalentCompressReaderTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(EquivalentCompressReaderBase);

///////////////////////////////////////////////////////////////////////////////
template <typename T>
class EquivalentCompressSessionReader;

template <typename T>
class EquivalentCompressReader : public EquivalentCompressReaderBase
{
public:
    typedef typename EquivalentCompressTraits<T>::ZigZagEncoder ZigZagEncoder;
    typedef typename EquivalentCompressTraits<T>::EncoderType   UT;
    /*    T: uint8_t  / int8_t   UT: uint8_t;    */
    /*    T: uint16_t / int16_t  UT: uint16_t;   */
    /*    T: uint32_t / int32_t / float   UT: uint32_t;   */
    /*    T: uint64_t / int64_t / double  UT: uint64_t;   */

    typedef DeltaValueArrayTyped<UT> DeltaValueArray;

    class Iterator
    {
    public:
        Iterator(const EquivalentCompressReader<T>* reader)
            : mReader(reader)
            , mCursor(0)
        {
        }

        bool HasNext() const
        {
            return mCursor < mReader->Size();
        }

        T Next()
        {
            assert(mReader);
            return mReader->Get(mCursor++);
        }

    private:
        const EquivalentCompressReader<T>* mReader;
        uint32_t mCursor;
    };

public:
    EquivalentCompressReader(const uint8_t *buffer,
                             int64_t compLen = INVALID_COMPRESS_LEN)
        : EquivalentCompressReaderBase(buffer, compLen)
    {}

    EquivalentCompressReader() {}

    ~EquivalentCompressReader() {}

    inline T operator[] (size_t pos) const __ALWAYS_INLINE;

    inline T Get(size_t pos) const __ALWAYS_INLINE
    { return (*this)[pos]; }

    bool Update(size_t pos, T value);

    Iterator CreateIterator() const
    {
        Iterator iter(this);
        return iter;
    }
    EquivalentCompressSessionReader<T> CreateSessionReader()
    {
        return EquivalentCompressSessionReader<T>(this);
    }

    size_t GetDeltaArray(size_t slotIdx, SlotItem& slotItem, UT& baseValue, uint8_t* deltaArray);
    size_t GetLongValueDeltaArray(
        size_t slotIdx, LongSlotItem& slotItem, LongValueArrayHeader& header, uint8_t* deltaArray); 

public:
    static void InplaceUpdateDeltaArray(
            uint8_t* deltaArray, SlotItemType slotType, 
            uint32_t valueIdx, UT updateDelta);

private:
    // for uint64_t, int64_t, double
    bool UpdateLongValueType(size_t pos, T value);
    bool ExpandUpdateLongValueTypeDeltaArray(
            uint8_t *slotItem, uint8_t *slotData,
            size_t pos, UT updateEncodeValue);

    inline T ReadLongValueType(size_t pos) const __ALWAYS_INLINE;

    bool ExpandUpdateDeltaArray(uint8_t *slotItem, 
                                uint8_t *slotData,
                                size_t pos,
                                UT updateEncodeValue);

    inline UT ReadDeltaValueBySlotItem(SlotItemType slotType, size_t valueOffset, uint32_t valueIdx) const;

    inline UT ReadDeltaValueByDeltaType(
        uint64_t deltaType, uint64_t valueOffset, size_t valueIdx) const __ALWAYS_INLINE; 
    

    inline UT GetDeltaValueBySlotType(SlotItemType slotType, 
            uint8_t* deltaBuffer, size_t idx) const __ALWAYS_INLINE;

    inline UT GetDeltaValueByDeltaType(uint64_t deltaType, 
            uint8_t* deltaBuffer, size_t idx) const __ALWAYS_INLINE;

    inline size_t GetDeltaArraySizeBySlotType(SlotItemType slotType, size_t count) const __ALWAYS_INLINE;

    inline size_t GetDeltaArraySizeByDeltaType(uint64_t deltaType, size_t count) const __ALWAYS_INLINE;

    // for equal slot expand
    void GenerateExpandDeltaBuffer(
            UT equalValue, UT updateValue, size_t pos, 
            uint8_t* deltaBuffer, UT &expandBaseValue, SlotItemType &expandSlotType);

    // for non-equal slot expand, return deltaBuffer size before expand
    void GenerateExpandDeltaBuffer(
            UT curBaseValue, SlotItemType curSlotType,
            uint8_t* curDeltaAddress, UT updateEncodeValue, size_t pos,
            UT* deltaBuffer, UT &expandBaseValue, SlotItemType &expandSlotType);

    bool ReachMaxDeltaType(SlotItemType slotType);

    size_t GetExpandFreeMemoryUse(SlotItemType curSlotType,
                                  uint64_t slotValue, size_t pos);

    friend class EquivalentCompressSessionReader<T>;
};

template <typename T>
class EquivalentCompressSessionReader
{
public:
    typedef typename EquivalentCompressTraits<T>::ZigZagEncoder ZigZagEncoder;
    typedef typename EquivalentCompressTraits<T>::EncoderType   UT;
    /*    T: uint8_t  / int8_t   UT: uint8_t;    */
    /*    T: uint16_t / int16_t  UT: uint16_t;   */
    /*    T: uint32_t / int32_t / float   UT: uint32_t;   */
    /*    T: uint64_t / int64_t / double  UT: uint64_t;   */

    typedef DeltaValueArrayTyped<UT> DeltaValueArray;

public:
    EquivalentCompressSessionReader(EquivalentCompressReader<T>* compressReader)
        : mSlotIdx(-1)
        , mDeltaArray(NULL)
    {
        mCompressReader = compressReader;
        mDeltaArray = new uint8_t[mCompressReader->GetSlotItemCount() * sizeof(T)];
    }
    ~EquivalentCompressSessionReader()
    {
        if (mDeltaArray) { delete [] mDeltaArray; }
    }
    
public:
    inline T operator[] (size_t pos)  __ALWAYS_INLINE
    {
        if (mCompressReader->IsReadFromMemory())
        {
            return mCompressReader->Get(pos);
        }
        assert(pos < mCompressReader->Size());
        size_t slotIdx, valueIdx;
        mCompressReader->DecodePosition(pos, slotIdx, valueIdx);
        if (slotIdx != mSlotIdx)
        {
            mSlotIdx = slotIdx;
            Prefetch(mSlotIdx);
        }
        return GetValueFromBuffer(valueIdx);
    }
    
    inline T Get(size_t pos)  __ALWAYS_INLINE
    { return (*this)[pos]; }
    
private:
    inline T GetValueFromBuffer(size_t valueIdx) const __ALWAYS_INLINE
    {
        return GetNormalValueFromBuffer(valueIdx);
    }
    
    inline T GetNormalValueFromBuffer(size_t valueIdx) const __ALWAYS_INLINE
    {
        if (mSlotItem.slotType == SIT_EQUAL)
        {
            return ZigZagEncoder::Decode((UT)(mSlotItem.value));
        }
        UT deltaValue =  mCompressReader->GetDeltaValueBySlotType(
            mSlotItem.slotType, mDeltaArray, valueIdx);
        return ZigZagEncoder::Decode(mBaseValue + deltaValue);
    }

    inline T GetLongValueFromBuffer(size_t valueIdx) const __ALWAYS_INLINE
    {
        if (mLongSlotItem.isValue == 1)
        {
            return ZigZagEncoder::Decode(mLongSlotItem.value);
        }
        UT deltaValue = mCompressReader->GetDeltaValueByDeltaType(
            mLongValueHeader.deltaType, mDeltaArray, valueIdx);
        return ZigZagEncoder::Decode(mBaseValue + deltaValue);
    }
    
    inline void Prefetch(size_t slotIdx) __ALWAYS_INLINE
    {
        PrefetchValue(slotIdx);
    }

    inline void PrefetchValue(size_t slotIdx) __ALWAYS_INLINE
    {
        mSlotItem = mCompressReader->GetSlotItem(slotIdx);
        if (mSlotItem.slotType == SIT_EQUAL)
        {
            return;
        }
        mCompressReader->GetDeltaArray(
            slotIdx, mSlotItem, mBaseValue, mDeltaArray);
    }

    inline void PrefetchLongValue(size_t slotIdx) __ALWAYS_INLINE
    {
        mLongSlotItem = mCompressReader->GetLongSlotItem(slotIdx);
        if (mLongSlotItem.isValue == 1)
        {
            return;
        }
        mCompressReader->GetLongValueDeltaArray(
            slotIdx, mLongSlotItem, mLongValueHeader, mDeltaArray);
        mBaseValue = mLongValueHeader.baseValue;
    }
private:
    EquivalentCompressReader<T>* mCompressReader;
    size_t mSlotIdx;
    SlotItem mSlotItem;
    LongSlotItem mLongSlotItem;
    LongValueArrayHeader mLongValueHeader;
    UT mBaseValue;
    uint8_t* mDeltaArray;
};

template<>
inline void EquivalentCompressSessionReader<uint64_t>::Prefetch(size_t slotIdx)
{
    PrefetchLongValue(slotIdx);
}

template<>
inline void EquivalentCompressSessionReader<int64_t>::Prefetch(size_t slotIdx)
{
    PrefetchLongValue(slotIdx);
}

template<>
inline void EquivalentCompressSessionReader<double>::Prefetch(size_t slotIdx)
{
    PrefetchLongValue(slotIdx);
}

template<>
inline uint64_t EquivalentCompressSessionReader<uint64_t>::GetValueFromBuffer(size_t valueIdx) const 
{
    return GetLongValueFromBuffer(valueIdx);
}

template<>
inline int64_t EquivalentCompressSessionReader<int64_t>::GetValueFromBuffer(size_t valueIdx) const 
{
    return GetLongValueFromBuffer(valueIdx);
}

template<>
inline double EquivalentCompressSessionReader<double>::GetValueFromBuffer(size_t valueIdx) const
{
    return GetLongValueFromBuffer(valueIdx);
}


///////////////////////////////////////////////////////////////

#define DECLARE_UNSUPPORT_TYPE_IMPL(type)                               \
    template <>                                                         \
    inline type EquivalentCompressReader<type>::operator[] (size_t pos) const \
    {                                                                   \
        assert(false);                                                  \
        return 0;                                                       \
    }                                                                   \
    template <>                                                         \
    inline bool EquivalentCompressReader<type>::Update(size_t pos, type value) \
    {                                                                   \
        assert(false);                                                  \
        return 0;                                                       \
    }

DECLARE_UNSUPPORT_TYPE_IMPL(autil::uint128_t)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::MultiInt8)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::MultiInt16)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::MultiInt32)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::MultiInt64)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::MultiUInt8)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::MultiUInt16)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::MultiUInt32)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::MultiUInt64)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::MultiFloat)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::MultiDouble)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::MultiChar)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::MultiString)

DECLARE_UNSUPPORT_TYPE_IMPL(autil::CountedMultiInt8)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::CountedMultiInt16)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::CountedMultiInt32)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::CountedMultiInt64)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::CountedMultiUInt8)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::CountedMultiUInt16)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::CountedMultiUInt32)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::CountedMultiUInt64)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::CountedMultiFloat)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::CountedMultiDouble)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::CountedMultiChar)
DECLARE_UNSUPPORT_TYPE_IMPL(autil::CountedMultiString)

template <typename T>
inline T EquivalentCompressReader<T>::operator[] (size_t pos) const
{
    assert(pos < mItemCount);
    SlotItem slotItem = GetSlotItem(pos >> mSlotBitNum); 
    if (slotItem.slotType == SIT_EQUAL)
    {
        return ZigZagEncoder::Decode((UT)(slotItem.value));
    }
    uint32_t valueIdx = pos & mSlotMask;
    UT deltaValue = 0;
    typedef DeltaValueArrayTyped<UT> DeltaValueArray;
    if (mValueBaseAddr)
    {
        uint8_t *valueItemsAddr = GetDeltaBlockAddress(slotItem.value);
        DeltaValueArray *valueArray = (DeltaValueArray*)valueItemsAddr;
        deltaValue = GetDeltaValueBySlotType(
            slotItem.slotType, valueArray->delta, valueIdx);
        return ZigZagEncoder::Decode(valueArray->baseValue + deltaValue);        
    }
    DeltaValueArray arrayHeader;
    size_t valueOffset = mValueBaseOffset + slotItem.value;
    valueOffset += mFileReader->Read(&arrayHeader, sizeof(DeltaValueArray), valueOffset);
    deltaValue = ReadDeltaValueBySlotItem(slotItem.slotType, valueOffset, valueIdx);
    return ZigZagEncoder::Decode(arrayHeader.baseValue + deltaValue);
}

template <typename T>
inline T EquivalentCompressReader<T>::ReadLongValueType(size_t pos) const
{
    assert(pos < mItemCount);
    LongSlotItem slotItem = GetLongSlotItem(pos >> mSlotBitNum);
    if (slotItem.isValue == 1)
    {
        return ZigZagEncoder::Decode(slotItem.value);
    }
    uint32_t valueIdx = pos & mSlotMask;
    if (mValueBaseAddr)
    {
        uint8_t *valueItemsAddr = GetDeltaBlockAddress(slotItem.value);
        LongValueArrayHeader *valueArray = (LongValueArrayHeader*)valueItemsAddr;
        uint8_t *deltaArray = valueItemsAddr + sizeof(LongValueArrayHeader);
        UT deltaValue = GetDeltaValueByDeltaType(
            valueArray->deltaType, deltaArray, valueIdx);
        return ZigZagEncoder::Decode(valueArray->baseValue + deltaValue);        
    }
    LongValueArrayHeader arrayHeader;
    size_t valueOffset = mValueBaseOffset + slotItem.value;
    valueOffset += mFileReader->Read(&arrayHeader, sizeof(arrayHeader), valueOffset);
    UT deltaValue = ReadDeltaValueByDeltaType(arrayHeader.deltaType, valueOffset, valueIdx); 
    return ZigZagEncoder::Decode(arrayHeader.baseValue + deltaValue); 
}

template <>
inline uint64_t EquivalentCompressReader<uint64_t>::operator[] (size_t pos) const
{
    return ReadLongValueType(pos);
}

template <>
inline int64_t EquivalentCompressReader<int64_t>::operator[] (size_t pos) const
{
    return ReadLongValueType(pos);
}

template <>
inline double EquivalentCompressReader<double>::operator[] (size_t pos) const
{
    return ReadLongValueType(pos);
}

template <typename T>
inline bool EquivalentCompressReader<T>::ExpandUpdateDeltaArray(
        uint8_t *slotItem, uint8_t *slotData, 
        size_t pos, UT updateEncodeValue)
{
    if (!mExpandSliceArray || mCompressLen == INVALID_COMPRESS_LEN)
    {
        return false;
    }

    uint32_t slotItemCount = (uint32_t)1 << mSlotBitNum;
    if (!mExpandDataBuffer)
    {
        size_t maxExpandSize = sizeof(UT) + slotItemCount * sizeof(UT);
        mExpandDataBuffer = new uint8_t[maxExpandSize];
    }
    UT* deltaBuffer = (UT*)(mExpandDataBuffer + sizeof(UT));

    SlotItemType expandSlotType;
    UT expandBaseValue;
    SlotItem* slotItemTyped = (SlotItem*)slotItem;
    if (slotData == NULL)
    {
        assert(slotItemTyped->slotType == SIT_EQUAL);
        GenerateExpandDeltaBuffer((UT)slotItemTyped->value, 
                updateEncodeValue, pos, (uint8_t*)deltaBuffer, 
                expandBaseValue, expandSlotType);
    }
    else
    {
        assert(slotItemTyped->slotType != SIT_EQUAL);
        uint8_t *valueItemsAddr = GetDeltaBlockAddress(slotItemTyped->value);
        typedef DeltaValueArrayTyped<UT> DeltaValueArray;
        DeltaValueArray *valueArray = (DeltaValueArray*)valueItemsAddr;

        GenerateExpandDeltaBuffer(
                valueArray->baseValue, slotItemTyped->slotType,
                valueArray->delta, updateEncodeValue, pos, 
                deltaBuffer, expandBaseValue, expandSlotType);
    }

    size_t freeMemorySize = GetExpandFreeMemoryUse(
            slotItemTyped->slotType, slotItemTyped->value, pos);
    IncreaseNoUsedMemorySize(freeMemorySize);
    IncreaseExpandUpdateCount();

    size_t compSize = GetCompressDeltaBufferSize(expandSlotType, slotItemCount);
    *(UT*)mExpandDataBuffer = expandBaseValue;
    compSize += sizeof(UT);
    int64_t expandOffset = mExpandSliceArray->Insert(mExpandDataBuffer, compSize);

    SlotItem newSlotItem;
    newSlotItem.slotType = expandSlotType;
    newSlotItem.value = expandOffset + mCompressLen;
    char *newSlotItemAddr = (char*)(&newSlotItem);
    MEMORY_BARRIER();
    *(uint64_t*)slotItem = *(uint64_t*)newSlotItemAddr;
    return true;
}

template <typename T>
inline bool EquivalentCompressReader<T>::ExpandUpdateLongValueTypeDeltaArray(
        uint8_t *slotItem, uint8_t *slotData, 
        size_t pos, UT updateEncodeValue)
{
    if (!mExpandSliceArray || mCompressLen == INVALID_COMPRESS_LEN)
    {
        return false;
    }

    uint32_t slotItemCount = (uint32_t)1 << mSlotBitNum;
    size_t headerLen = sizeof(UT) * 2;
    if (!mExpandDataBuffer)
    {
        size_t maxExpandSize = headerLen + slotItemCount * sizeof(UT);
        mExpandDataBuffer = new uint8_t[maxExpandSize];
    }
    UT* deltaBuffer = (UT*)(mExpandDataBuffer + headerLen);

    SlotItemType expandSlotType;
    UT expandBaseValue;
    LongSlotItem* slotItemTyped = (LongSlotItem*)slotItem;
    SlotItemType slotType = SIT_EQUAL;
    if (slotData == NULL)
    {
        assert(slotItemTyped->isValue == 1);
        GenerateExpandDeltaBuffer((UT)slotItemTyped->value,
                updateEncodeValue, pos, (uint8_t*)deltaBuffer, 
                expandBaseValue, expandSlotType);
    }
    else
    {
        assert(slotItemTyped->isValue != 1);
        uint8_t *valueItemsAddr = GetDeltaBlockAddress(slotItemTyped->value);
        LongValueArrayHeader *valueArray = (LongValueArrayHeader*)valueItemsAddr;
        slotType = DeltaFlagToSlotItemType(valueArray->deltaType);
        uint8_t *deltaArray = valueItemsAddr + sizeof(LongValueArrayHeader);
        GenerateExpandDeltaBuffer(
                valueArray->baseValue, slotType,
                deltaArray, updateEncodeValue, pos, 
                deltaBuffer, expandBaseValue, expandSlotType);
    }

    size_t freeMemorySize = GetExpandFreeMemoryUse(
            slotType, slotItemTyped->value, pos);
    IncreaseNoUsedMemorySize(freeMemorySize);
    IncreaseExpandUpdateCount();

    size_t compSize = GetCompressDeltaBufferSize(expandSlotType, slotItemCount);
    UT* blockHeader = (UT*)mExpandDataBuffer;
    *blockHeader = expandBaseValue;
    *(blockHeader + 1) = SlotItemTypeToDeltaFlag(expandSlotType);
    compSize += headerLen;
    int64_t expandOffset = mExpandSliceArray->Insert(mExpandDataBuffer, compSize);

    LongSlotItem newSlotItem;
    newSlotItem.isValue = 0;
    newSlotItem.value = expandOffset + mCompressLen;
    char *newSlotItemAddr = (char*)(&newSlotItem);
    MEMORY_BARRIER();
    *(uint64_t*)slotItem = *(uint64_t*)newSlotItemAddr;
    return true;
}

template <typename T>
inline bool EquivalentCompressReader<T>::Update(size_t pos, T value)
{
    if (pos >= mItemCount || !mSlotBaseAddr)
    {
        return false;
    }

    UT updateEncodeValue = ZigZagEncoder::Encode(value);
    SlotItem *slotItem = (SlotItem*)mSlotBaseAddr + (pos >> mSlotBitNum);
    if (slotItem->slotType == SIT_EQUAL)
    {
        if ((UT)slotItem->value == updateEncodeValue)
        {
            return true;
        }
        return ExpandUpdateDeltaArray((uint8_t*)slotItem, 
                NULL, pos, updateEncodeValue);
    }

    uint8_t *valueItemsAddr = GetDeltaBlockAddress(slotItem->value);
    DeltaValueArray *valueArray = (DeltaValueArray*)valueItemsAddr;
    if (updateEncodeValue < valueArray->baseValue)
    {
        return ExpandUpdateDeltaArray((uint8_t*)slotItem, 
                valueItemsAddr, pos, updateEncodeValue);
    }

    UT updateDelta = updateEncodeValue - valueArray->baseValue;
    SlotItemType updatedSlotItemType = GetSlotItemType(updateDelta);
    if (!INPLACE_UPDATE_FLAG_ARRAY[slotItem->slotType][updatedSlotItemType])
    {
        return ExpandUpdateDeltaArray((uint8_t*)slotItem, 
                valueItemsAddr, pos, updateEncodeValue);
    }

    uint32_t valueIdx = pos & mSlotMask;
    InplaceUpdateDeltaArray(valueArray->delta, slotItem->slotType,
                            valueIdx, updateDelta);
    IncreaseInplaceUpdateCount();
    return true;
}

template <typename T>
inline bool EquivalentCompressReader<T>::UpdateLongValueType(size_t pos, T value)
{
    if (pos >= mItemCount)
    {
        return false;
    }

    uint64_t updateEncodeValue = ZigZagEncoder::Encode(value);
    LongSlotItem *slotItem = (LongSlotItem*)mSlotBaseAddr + (pos >> mSlotBitNum);
    if (slotItem->isValue == 1)
    {
        if (slotItem->value == updateEncodeValue)
        {
            return true;
        }
        return ExpandUpdateLongValueTypeDeltaArray((uint8_t*)slotItem, 
                NULL, pos, updateEncodeValue);
    }

    uint8_t *valueItemsAddr = GetDeltaBlockAddress(slotItem->value);
    LongValueArrayHeader *valueArray = (LongValueArrayHeader*)valueItemsAddr;
    if (updateEncodeValue < valueArray->baseValue)
    {
        return ExpandUpdateLongValueTypeDeltaArray((uint8_t*)slotItem, 
                valueItemsAddr, pos, updateEncodeValue);
    }

    SlotItemType slotItemType = DeltaFlagToSlotItemType(valueArray->deltaType);
    uint64_t updateDelta = updateEncodeValue - valueArray->baseValue;
    SlotItemType updatedSlotItemType = GetSlotItemType(updateDelta);
    if (!INPLACE_UPDATE_FLAG_ARRAY[slotItemType][updatedSlotItemType])
    {
        return ExpandUpdateLongValueTypeDeltaArray((uint8_t*)slotItem, 
                valueItemsAddr, pos, updateEncodeValue);
    }

    uint8_t *deltaArray = valueItemsAddr + sizeof(LongValueArrayHeader);
    uint32_t valueIdx = pos & mSlotMask;
    InplaceUpdateDeltaArray(deltaArray, slotItemType, valueIdx, updateDelta);
    IncreaseInplaceUpdateCount();
    return true;
}

template <>
inline bool EquivalentCompressReader<int64_t>::Update(size_t pos, int64_t value)
{
    return UpdateLongValueType(pos, value);
}

template <>
inline bool EquivalentCompressReader<uint64_t>::Update(size_t pos, uint64_t value)
{
    return UpdateLongValueType(pos, value);
}

template <>
inline bool EquivalentCompressReader<double>::Update(size_t pos, double value)
{
    return UpdateLongValueType(pos, value);
}
template <typename T>
inline size_t EquivalentCompressReader<T>::GetDeltaArray(size_t slotIdx, SlotItem& slotItem,
                                                       UT& baseValue, uint8_t* deltaArray)
{
    typedef DeltaValueArrayTyped<UT> DeltaValueArray;
    DeltaValueArray arrayHeader;
    size_t valueOffset = mValueBaseOffset + slotItem.value;
    valueOffset += mFileReader->Read(&arrayHeader, sizeof(DeltaValueArray), valueOffset);
    baseValue = arrayHeader.baseValue;
    size_t slotSize = (slotIdx == ((mItemCount-1) >> mSlotBitNum)) ?
        ((mItemCount-1) & mSlotMask) + 1 : GetSlotItemCount();
    size_t deltaArraySize = GetDeltaArraySizeBySlotType(slotItem.slotType, slotSize);
    return mFileReader->Read(deltaArray, deltaArraySize, valueOffset);
}

template <typename T>
inline size_t EquivalentCompressReader<T>::GetLongValueDeltaArray(
    size_t slotIdx, LongSlotItem& slotItem, LongValueArrayHeader& header, uint8_t* deltaArray)
{
    size_t valueOffset = mValueBaseOffset + slotItem.value;
    valueOffset += mFileReader->Read(&header, sizeof(LongValueArrayHeader), valueOffset);
    size_t slotSize = (slotIdx == ((mItemCount-1) >> mSlotBitNum)) ?
        ((mItemCount-1) & mSlotMask) + 1 : GetSlotItemCount();
    size_t deltaArraySize = GetDeltaArraySizeByDeltaType(header.deltaType, slotSize);
    return mFileReader->Read(deltaArray, deltaArraySize, valueOffset);
}

#define UpdateBitValue(itemCountMask, itemCountPowerNum, bitCountPowerNum, bitMask, deltaArray) \
    uint32_t alignSize = 8;                                             \
    uint32_t byteIndex = valueIdx >> itemCountPowerNum;                 \
    uint32_t posInByte = valueIdx & itemCountMask;                      \
    uint32_t bitMoveCount = alignSize - ((posInByte + 1) << bitCountPowerNum); \
    uint8_t readMask = ~(bitMask << bitMoveCount);                      \
    uint8_t writeMask = (updateDelta & bitMask) << bitMoveCount;        \
    uint8_t byteValue = (deltaArray[byteIndex] & readMask) | writeMask; \
    deltaArray[byteIndex] = byteValue;                                  \
    
template <typename T>
inline void EquivalentCompressReader<T>::InplaceUpdateDeltaArray(
        uint8_t* deltaArray, SlotItemType slotType, 
        uint32_t valueIdx, UT updateDelta)
{
    switch (slotType)
    {
    case SIT_DELTA_UINT8:  // uint8_t
    {
        deltaArray[valueIdx] = (uint8_t)updateDelta;
        break;
    }
    case SIT_DELTA_UINT16:   // uint16_t 
    {
        ((uint16_t*)deltaArray)[valueIdx] = (uint16_t)updateDelta;
        break;
    }
    case SIT_DELTA_UINT32:  // uint32_t
    {
        ((uint32_t*)deltaArray)[valueIdx] = (uint32_t)updateDelta;
        break;
    }
    case SIT_DELTA_UINT64:  // uint64_t
    {
        ((uint64_t*)deltaArray)[valueIdx] = (uint64_t)updateDelta;
        break;
    }

    case SIT_DELTA_BIT1:
    {
        UpdateBitValue(7, 3, 0, 1, deltaArray);
        break;
    }
    case SIT_DELTA_BIT2:
    {
        UpdateBitValue(3, 2, 1, 3, deltaArray);
        break;
    }
    case SIT_DELTA_BIT4:
    {
        UpdateBitValue(1, 1, 2, 15, deltaArray);
        break;
    }
    default:
        assert(false);
    }
}

#define DecodeBitValueInOneByte(posInByte, bitCountPowerNum, bitMask, byteValue) \
    uint32_t alignSize = 8;                                             \
    uint32_t bitMoveCount = alignSize - ((posInByte + 1) << bitCountPowerNum); \
    byteValue = byteValue >> bitMoveCount;                              \
    byteValue &= bitMask;                                               \
    return byteValue;


#define DecodeBitValue(itemCountMask, itemCountPowerNum, bitCountPowerNum, bitMask, deltaArray) \
    uint32_t alignSize = 8;                                             \
    uint32_t byteIndex = valueIdx >> itemCountPowerNum;                 \
    uint32_t posInByte = valueIdx & itemCountMask;                      \
    uint8_t byteValue = deltaArray[byteIndex];                          \
    uint32_t bitMoveCount = alignSize - ((posInByte + 1) << bitCountPowerNum); \
    byteValue = byteValue >> bitMoveCount;                              \
    byteValue &= bitMask;                                               \
    return byteValue;


template <typename T>
inline typename EquivalentCompressReader<T>::UT
EquivalentCompressReader<T>::ReadDeltaValueBySlotItem(
    SlotItemType slotType, size_t valueOffset, uint32_t valueIdx) const
{    
    switch(slotType)
    {
    case SIT_DELTA_UINT8:  // uint8_t
    {
        valueOffset += sizeof(uint8_t) * valueIdx;
        uint8_t typedValue;
        mFileReader->Read(&typedValue, sizeof(uint8_t), valueOffset);
        return typedValue;
    }
    case SIT_DELTA_UINT16:   // uint16_t 
    {
        uint16_t typedValue;
        valueOffset += sizeof(uint16_t) * valueIdx;
        mFileReader->Read(&typedValue, sizeof(uint16_t), valueOffset);
        return typedValue;        
    }
    case SIT_DELTA_UINT32:  // uint32_t
    {
        uint32_t typedValue;
        valueOffset += sizeof(uint32_t) * valueIdx;
        mFileReader->Read(&typedValue, sizeof(uint32_t), valueOffset);
        return typedValue;
    }
    case SIT_DELTA_UINT64:  // uint64_t
    {
        uint64_t typedValue;
        valueOffset += sizeof(uint64_t) * valueIdx;
        mFileReader->Read(&typedValue, sizeof(uint64_t), valueOffset);
        return typedValue;
    }
    case SIT_DELTA_BIT1:
    {
        valueOffset += (valueIdx >> 3);
        uint8_t byteValue;
        mFileReader->Read(&byteValue, sizeof(uint8_t), valueOffset);
        DecodeBitValueInOneByte((valueIdx & 7), 0, 1, byteValue);
    }
    case SIT_DELTA_BIT2:
    {
        valueOffset += (valueIdx >> 2);
        uint8_t byteValue;
        mFileReader->Read(&byteValue, sizeof(uint8_t), valueOffset);
        DecodeBitValueInOneByte((valueIdx & 3), 1, 3, byteValue);
    }
    case SIT_DELTA_BIT4:
    {
        valueOffset += (valueIdx >> 1);
        uint8_t byteValue;
        mFileReader->Read(&byteValue, sizeof(uint8_t), valueOffset);
        DecodeBitValueInOneByte((valueIdx & 1),  2, 15, byteValue);        
    }
    default:
        assert(false);
    }        
    return 0;
}

template <typename T>
inline typename EquivalentCompressReader<T>::UT
EquivalentCompressReader<T>::GetDeltaValueBySlotType(
        SlotItemType slotType, uint8_t* deltaBuffer, size_t valueIdx) const
{
    switch (slotType)
    {
    case SIT_DELTA_UINT8:  // uint8_t
    {
        return deltaBuffer[valueIdx];
    }
    case SIT_DELTA_UINT16:   // uint16_t 
    {
        return ((uint16_t*)deltaBuffer)[valueIdx];
    }
    case SIT_DELTA_UINT32:  // uint32_t
    {
        return ((uint32_t*)deltaBuffer)[valueIdx];
    }
    case SIT_DELTA_UINT64:  // uint64_t
    {
        return ((uint64_t*)deltaBuffer)[valueIdx];
    }
    case SIT_DELTA_BIT1:
    {
        DecodeBitValue(7, 3, 0, 1, deltaBuffer);
    }
    case SIT_DELTA_BIT2:
    {
        DecodeBitValue(3, 2, 1, 3, deltaBuffer);
    }
    case SIT_DELTA_BIT4:
    {
        DecodeBitValue(1, 1, 2, 15, deltaBuffer);
    }
    default:
        assert(false);
    }
    return 0;
}


template <typename T>
size_t EquivalentCompressReader<T>::GetDeltaArraySizeBySlotType(
    SlotItemType slotType, size_t count) const
{
    switch (slotType)
    {
    case SIT_DELTA_UINT8:  // uint8_t
    {
        return sizeof(uint8_t) * count;
    }
    case SIT_DELTA_UINT16:   // uint16_t 
    {
        return sizeof(uint16_t) * count;
    }
    case SIT_DELTA_UINT32:  // uint32_t
    {
        return sizeof(uint32_t) * count;
    }
    case SIT_DELTA_UINT64:  // uint64_t
    {
        return sizeof(uint64_t) * count;
    }
    case SIT_DELTA_BIT1:
    {
        return (count + 7) >> 3;
    }
    case SIT_DELTA_BIT2:
    {
        return ((count << 1) + 7) >> 3;
    }
    case SIT_DELTA_BIT4:
    {
        return ((count << 2) + 7) >> 3;
    }
    default:
        assert(false);
    }
    return 0;
}


template <typename T>
inline typename EquivalentCompressReader<T>::UT 
EquivalentCompressReader<T>::ReadDeltaValueByDeltaType(
        uint64_t deltaType, uint64_t valueOffset, size_t valueIdx) const
{
    switch (deltaType)
    {
    case 0:  // uint8_t
    {
        valueOffset += sizeof(uint8_t) * valueIdx;
        uint8_t typedValue;
        mFileReader->Read(&typedValue, sizeof(uint8_t), valueOffset);
        return typedValue;
    }
    case 1:  // uint16_t
    {
        valueOffset += sizeof(uint16_t) * valueIdx;
        uint16_t typedValue;
        mFileReader->Read(&typedValue, sizeof(uint16_t), valueOffset);
        return typedValue; 
    }
    case 2:  // uint32_t
    {
        valueOffset += sizeof(uint32_t) * valueIdx;
        uint32_t typedValue;
        mFileReader->Read(&typedValue, sizeof(uint32_t), valueOffset);
        return typedValue; 
    }
    case 3:  // uint64_t
    {
        valueOffset += sizeof(uint64_t) * valueIdx;
        uint64_t typedValue;
        mFileReader->Read(&typedValue, sizeof(uint64_t), valueOffset);
        return typedValue; 
    }
    case 4:  // 1 bit
    {
        valueOffset += (valueIdx >> 3);
        uint8_t byteValue;
        mFileReader->Read(&byteValue, sizeof(uint8_t), valueOffset);
        DecodeBitValueInOneByte((valueIdx & 7), 0, 1, byteValue); 
    }
    case 5:  // 2 bits
    {
        valueOffset += (valueIdx >> 2);
        uint8_t byteValue;
        mFileReader->Read(&byteValue, sizeof(uint8_t), valueOffset); 
        DecodeBitValueInOneByte((valueIdx & 3), 1, 3, byteValue); 
    }
    case 6:  // 4 bits
    {
        valueOffset += (valueIdx >> 1);
        uint8_t byteValue;
        mFileReader->Read(&byteValue, sizeof(uint8_t), valueOffset);
        DecodeBitValueInOneByte((valueIdx & 1),  2, 15, byteValue); 
    }
    default:
        assert(false);
    }
    return 0;
}

template <typename T>
inline typename EquivalentCompressReader<T>::UT 
EquivalentCompressReader<T>::GetDeltaValueByDeltaType(
        uint64_t deltaType, uint8_t* deltaArray, size_t valueIdx) const
{
    switch (deltaType)
    {
    case 0:  // uint8_t
    {
        return ((uint8_t*)deltaArray)[valueIdx];
    }
    case 1:  // uint16_t 
    {
        return ((uint16_t*)deltaArray)[valueIdx];
    }
    case 2:  // uint32_t
    {
        return ((uint32_t*)deltaArray)[valueIdx];
    }
    case 3:  // uint64_t
    {
        return ((uint64_t*)deltaArray)[valueIdx];
    }
    case 4:  // 1 bit
    {
        DecodeBitValue(7, 3, 0, 1, deltaArray);
    }
    case 5:  // 2 bits
    {
        DecodeBitValue(3, 2, 1, 3, deltaArray);
    }
    case 6:  // 4 bits
    {
        DecodeBitValue(1, 1, 2, 15, deltaArray);
    }
    default:
        assert(false);
    }
    return 0;

}

template <typename T>
size_t EquivalentCompressReader<T>::GetDeltaArraySizeByDeltaType(
    uint64_t deltaType, size_t count) const
{
    switch (deltaType)
    {
    case 0:  // uint8_t
    {
        return sizeof(uint8_t) * count;
    }
    case 1:  // uint16_t
    {
        return sizeof(uint16_t) * count;
    }
    case 2:  // uint32_t
    {
        return sizeof(uint32_t) * count;
    }
    case 3:  // uint64_t
    {
        return sizeof(uint64_t) * count;
    }
    case 4:  // 1 bit
    {
        return (count + 7) >> 3;
    }
    case 5:  // 2 bits
    {
        return ((count << 1) + 7) >> 3;
    }
    case 6:  // 4 bits
    {
        return ((count << 2) + 7) >> 3;
    }
    default:
        assert(false);
    }
    return 0;

}


template <typename T>
inline bool EquivalentCompressReader<T>::ReachMaxDeltaType(SlotItemType slotType)
{
    size_t slotTypeSize = 0;
    switch (slotType)
    {
    case SIT_DELTA_UINT8:
        slotTypeSize = sizeof(uint8_t);
        break;

    case SIT_DELTA_UINT16:
        slotTypeSize = sizeof(uint16_t);
        break;

    case SIT_DELTA_UINT32:
        slotTypeSize = sizeof(uint32_t);
        break;

    case SIT_DELTA_UINT64:
        slotTypeSize = sizeof(uint64_t);
        break;

    default:
        return false;
    }

    return slotTypeSize >= sizeof(UT);
}

template <typename T>
inline void EquivalentCompressReader<T>::GenerateExpandDeltaBuffer(
        UT equalValue, UT updateEncodeValue, size_t pos,
        uint8_t* deltaBuffer, UT &expandBaseValue, SlotItemType &expandSlotType)
{
    uint32_t updateValueIdx = pos & mSlotMask;
    expandBaseValue = std::min(equalValue, updateEncodeValue);
    UT maxDeltaValue = 
        std::max(equalValue, updateEncodeValue) - expandBaseValue;
    expandSlotType = GetSlotItemType(maxDeltaValue);
    if (ReachMaxDeltaType(expandSlotType))
    {
        expandBaseValue = 0;
    }

    uint32_t slotItemCount = 1 << mSlotBitNum;
    for (uint32_t i = 0; i < slotItemCount; i++)
    {
        UT deltaValue = (i == updateValueIdx) ? updateEncodeValue - expandBaseValue : 
                        equalValue - expandBaseValue;
        InplaceUpdateDeltaArray(deltaBuffer, expandSlotType, i, deltaValue);
    }
}

template <typename T>
inline void EquivalentCompressReader<T>::GenerateExpandDeltaBuffer(
        UT curBaseValue, SlotItemType curSlotType,
        uint8_t* curDeltaAddress, UT updateEncodeValue, size_t pos,
        UT* deltaBuffer, UT &expandBaseValue, SlotItemType &expandSlotType)
{
    uint32_t valueIdx = pos & mSlotMask;
    uint32_t slotItemCount = (uint32_t)1 << mSlotBitNum;
    uint32_t endIdx = slotItemCount;
    size_t beginPos = (pos >> mSlotBitNum) << mSlotBitNum;
    if (mItemCount < beginPos + slotItemCount)
    {
        endIdx = mItemCount - beginPos;
    }

    UT maxDelta = 0;
    for (uint32_t i = 0; i < endIdx; ++i)
    {
        deltaBuffer[i] = GetDeltaValueBySlotType(
                curSlotType, curDeltaAddress, i);
        if (deltaBuffer[i] > maxDelta)
        {
            maxDelta = deltaBuffer[i];
        }
    }

    expandBaseValue = curBaseValue;
    UT expandDelta = 0;
    if (updateEncodeValue < expandBaseValue)
    {
        expandDelta = expandBaseValue - updateEncodeValue;
        maxDelta += expandDelta;
        expandBaseValue = updateEncodeValue;
    }
    else
    {
        maxDelta = updateEncodeValue - expandBaseValue;
    }

    expandSlotType = GetSlotItemType(maxDelta);
    if (ReachMaxDeltaType(expandSlotType))
    {
        expandDelta += (expandBaseValue - 0);
        expandBaseValue = 0;
    }

    for (uint32_t i = 0; i < slotItemCount; ++i)
    {
        UT deltaValue = (i >= endIdx) ? 0 : deltaBuffer[i] + expandDelta;
        if (i == valueIdx)
        {
            deltaValue = updateEncodeValue - expandBaseValue;
        }
        InplaceUpdateDeltaArray((uint8_t*)deltaBuffer, expandSlotType, i, deltaValue);
    }
}



template <typename T>
inline size_t EquivalentCompressReader<T>::GetExpandFreeMemoryUse(
        SlotItemType curSlotType, uint64_t slotValue, size_t pos)
{
    if (curSlotType == SIT_EQUAL)
    {
        return 0;
    }

    uint32_t slotItemCount = (uint32_t)1 << mSlotBitNum;
    uint32_t itemCount = slotItemCount;
    if (slotValue < mCompressLen)
    {
        uint32_t endIdx = slotItemCount;
        size_t beginPos = (pos >> mSlotBitNum) << mSlotBitNum;
        if (mItemCount < beginPos + slotItemCount)
        {
            endIdx = mItemCount - beginPos;
        }
        itemCount = endIdx;
    }

    size_t headerSize = (sizeof(UT) == sizeof(uint64_t)) ? 
                        (sizeof(UT) * 2) : sizeof(UT);
    return headerSize + GetCompressDeltaBufferSize(curSlotType, itemCount);
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EQUIVALENT_COMPRESS_READER_H
