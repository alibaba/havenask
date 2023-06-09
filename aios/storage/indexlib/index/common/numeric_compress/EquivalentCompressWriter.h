/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <memory>

#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressReader.h"
#include "indexlib/util/slice_array/ByteAlignedSliceArray.h"

namespace indexlib::index {

class EquivalentCompressWriterBase
{
public:
    typedef indexlib::util::AlignedSliceArray<uint64_t> IndexArray;
    typedef indexlib::util::ByteAlignedSliceArray DataArray;

public:
    EquivalentCompressWriterBase()
        : _slotItemPowerNum(0)
        , _slotItemCount(0)
        , _itemCount(0)
        , _dataArrayCursor(0)
        , _indexArray(nullptr)
        , _dataArray(nullptr)
        , _pool(nullptr)
    {
    }

    explicit EquivalentCompressWriterBase(autil::mem_pool::PoolBase* pool)
        : _slotItemPowerNum(0)
        , _slotItemCount(0)
        , _itemCount(0)
        , _dataArrayCursor(0)
        , _indexArray(nullptr)
        , _dataArray(nullptr)
        , _pool(pool)
    {
    }

    virtual ~EquivalentCompressWriterBase()
    {
        indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _indexArray);
        indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _dataArray);
    }

public:
    void Init(uint32_t slotItemCount)
    {
        Reset();
        _slotItemPowerNum = GetUpperPowerNumber(slotItemCount);
        _slotItemCount = (1 << _slotItemPowerNum);
        _indexArray = IE_POOL_COMPATIBLE_NEW_CLASS(_pool, IndexArray, INDEX_SLICE_LEN, 1, nullptr, _pool);
        _dataArray = IE_POOL_COMPATIBLE_NEW_CLASS(_pool, DataArray, DATA_SLICE_LEN, 1, nullptr, _pool);
        InitSlotBuffer();
    }

    size_t GetCompressLength()
    {
        FlushSlotBuffer();
        size_t compLen = 2 * sizeof(uint32_t);
        if (_itemCount == 0) {
            return compLen;
        }

        uint32_t slotCount = (_itemCount + _slotItemCount - 1) / _slotItemCount;
        compLen += (slotCount * sizeof(uint64_t));
        compLen += _dataArrayCursor;
        return compLen;
    }

    size_t DumpBuffer(uint8_t* buffer, size_t bufferLen)
    {
        FlushSlotBuffer();
        assert(bufferLen >= GetCompressLength());

        uint8_t* cursor = buffer;
        *(uint32_t*)cursor = _itemCount;
        cursor += sizeof(uint32_t);
        *(uint32_t*)cursor = _slotItemPowerNum;
        cursor += sizeof(uint32_t);

        for (uint32_t i = 0; i < _indexArray->GetSliceNum(); ++i) {
            uint32_t sliceLen = _indexArray->GetSliceDataLen(i);
            uint64_t* slice = _indexArray->GetSlice(i);
            memcpy(cursor, slice, sliceLen * sizeof(uint64_t));
            cursor += (sliceLen * sizeof(uint64_t));
        }
        for (uint32_t i = 0; i < _dataArray->GetSliceNum(); ++i) {
            uint32_t sliceLen = _dataArray->GetSliceDataLen(i);
            char* slice = _dataArray->GetSlice(i);
            memcpy(cursor, slice, sliceLen);
            cursor += sliceLen;
        }
        return cursor - buffer;
    }

    Status Dump(const std::shared_ptr<indexlib::file_system::FileWriter>& file)
    {
        FlushSlotBuffer();

        auto [st, _] = file->Write(&_itemCount, sizeof(uint32_t)).StatusWith();
        RETURN_IF_STATUS_ERROR(st, "write item count failed, itemCount[%u]", _itemCount);
        std::tie(st, _) = file->Write(&_slotItemPowerNum, sizeof(uint32_t)).StatusWith();
        RETURN_IF_STATUS_ERROR(st, "write item power num failed, powerNum[%u]", _slotItemPowerNum);
        for (uint32_t i = 0; i < _indexArray->GetSliceNum(); ++i) {
            uint32_t sliceLen = _indexArray->GetSliceDataLen(i);
            uint64_t* slice = _indexArray->GetSlice(i);
            std::tie(st, _) = file->Write(slice, sliceLen * sizeof(uint64_t)).StatusWith();
            RETURN_IF_STATUS_ERROR(st, "write index slice len failed, len[%u]", sliceLen);
        }
        for (uint32_t i = 0; i < _dataArray->GetSliceNum(); ++i) {
            uint32_t sliceLen = _dataArray->GetSliceDataLen(i);
            char* slice = _dataArray->GetSlice(i);
            std::tie(st, _) = file->Write(slice, sliceLen).StatusWith();
            RETURN_IF_STATUS_ERROR(st, "write data slice len failed, len[%u]", sliceLen);
        }
        return Status::OK();
    }

protected:
    virtual void InitSlotBuffer() = 0;
    virtual void FreeSlotBuffer() = 0;
    virtual void FlushSlotBuffer() = 0;

    void Reset()
    {
        FreeSlotBuffer();
        indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _indexArray);
        indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _dataArray);
        _slotItemPowerNum = 0;
        _slotItemCount = 0;
        _itemCount = 0;
        _dataArrayCursor = 0;
    }

    static uint32_t GetUpperPowerNumber(uint32_t slotItemCount)
    {
        uint32_t powerNum = 6; // min slotItemNum is 2^6 = 64
        while ((uint32_t)(1 << powerNum) < slotItemCount) {
            ++powerNum;
        }
        return powerNum;
    }

private:
    constexpr static uint32_t INDEX_SLICE_LEN = 8 * 1024;
    constexpr static uint32_t DATA_SLICE_LEN = 256 * 1024;

protected:
    uint32_t _slotItemPowerNum;
    uint32_t _slotItemCount;
    uint32_t _itemCount;
    size_t _dataArrayCursor;
    IndexArray* _indexArray;
    DataArray* _dataArray;
    autil::mem_pool::PoolBase* _pool;

protected:
    friend class EquivalentCompressWriterTest;

    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////

template <typename T>
class EquivalentCompressWriter : public EquivalentCompressWriterBase
{
public:
    typedef typename EquivalentCompressTraits<T>::ZigZagEncoder ZigZagEncoder;
    typedef typename EquivalentCompressTraits<T>::EncoderType UT;

public:
    EquivalentCompressWriter() : EquivalentCompressWriterBase(nullptr), _slotBuffer(nullptr), _cursorInBuffer(0) {}

    explicit EquivalentCompressWriter(autil::mem_pool::PoolBase* pool)
        : EquivalentCompressWriterBase(pool)
        , _slotBuffer(nullptr)
        , _cursorInBuffer(0)
    {
    }

    ~EquivalentCompressWriter() { FreeSlotBuffer(); }

public:
    // could be called repeatedly before calling FlushSlotBuffer
    //    eg: GetCompressLength / DumpBuffer/ DumpFile / Dump
    void CompressData(T* data, uint32_t count);

    void CompressData(const std::vector<T>& dataVec) { CompressData((T*)dataVec.data(), dataVec.size()); }

    Status CompressData(const EquivalentCompressReader<T>& reader);

public:
    static std::pair<Status, size_t> CalculateCompressLength(T* data, uint32_t count, uint32_t slotItemCount)
    {
        ItemIteratorTyped<T> itemIter(data, count);
        // no exception here, so can we ignore the status here ?
        return CalculateCompressLength(itemIter, slotItemCount);
    }

    static std::pair<Status, size_t> CalculateCompressLength(const std::vector<T>& dataVec, uint32_t slotItemCount)
    {
        ItemIteratorTyped<T> itemIter((const T*)dataVec.data(), (uint32_t)dataVec.size());
        return CalculateCompressLength(itemIter, slotItemCount);
    }

    static std::pair<Status, size_t> CalculateCompressLength(const EquivalentCompressReader<T>& reader,
                                                             uint32_t slotItemCount)
    {
        return CalculateCompressLength(reader.CreateIterator(), slotItemCount);
    }

    template <typename ItemIterator>
    static std::pair<Status, size_t> CalculateCompressLength(ItemIterator iter, uint32_t slotItemCount)
    {
        size_t compSize = sizeof(uint32_t) * 2;
        uint32_t powerNum = EquivalentCompressWriter<T>::GetUpperPowerNumber(slotItemCount);
        slotItemCount = (1 << powerNum);

        UT* buffer = new UT[slotItemCount];
        uint32_t cursorInBuffer = 0;

        uint32_t count = 0;
        while (iter.HasNext()) {
            auto [status, nextVal] = iter.Next();
            RETURN2_IF_STATUS_ERROR(status, 0, "read next value fail when calculate compress length");
            buffer[cursorInBuffer++] = ZigZagEncoder::Encode(nextVal);
            if (cursorInBuffer == slotItemCount) {
                compSize += CalculateDeltaBufferSize(buffer, cursorInBuffer);
                cursorInBuffer = 0;
            }
            ++count;
        }

        if (cursorInBuffer > 0) {
            compSize += CalculateDeltaBufferSize(buffer, cursorInBuffer);
        }
        delete[] buffer;

        // calculate slotItems size
        uint32_t slotMask = (1 << powerNum) - 1;
        uint32_t slotCount = (count + slotMask) >> powerNum;
        compSize += sizeof(uint64_t) * slotCount;
        return std::make_pair(Status::OK(), compSize);
    }

protected:
    void InitSlotBuffer() override
    {
        assert(!_slotBuffer);
        _slotBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, UT, _slotItemCount);
        _cursorInBuffer = 0;
    }

    void FreeSlotBuffer() override
    {
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _slotBuffer, _slotItemCount);
        _cursorInBuffer = 0;
    }

    void FlushSlotBuffer() override
    {
        if (_cursorInBuffer == 0) {
            return;
        }

        UT minValue = 0;
        UT maxDelta = GetMaxDeltaInBuffer(_slotBuffer, _cursorInBuffer, minValue);
        if (NeedStoreDelta(maxDelta, minValue)) {
            ConvertToDeltaBuffer(_slotBuffer, _cursorInBuffer, minValue);
            SlotItemType slotType = GetSlotItemType(maxDelta);
            AppendSlotItem(slotType, _dataArrayCursor);
            _dataArrayCursor += FlushDeltaBuffer(_slotBuffer, _cursorInBuffer, minValue, slotType);
        } else {
            AppendSlotItem(SIT_EQUAL, minValue);
        }
        _itemCount += _cursorInBuffer;
        _cursorInBuffer = 0;
    }

private:
    static UT GetMaxDeltaInBuffer(UT* slotBuffer, uint32_t itemCount, UT& minValue)
    {
        assert(itemCount > 0);

        UT targetValue = slotBuffer[0];
        UT baseValue = targetValue;
        UT maxDelta = 0;

        for (size_t idx = 1; idx < itemCount; ++idx) {
            UT curValue = slotBuffer[idx];
            if (curValue != targetValue) {
                if (curValue <= baseValue) {
                    maxDelta += (baseValue - curValue);
                    baseValue = curValue;
                } else {
                    UT delta = curValue - baseValue;
                    if (delta > maxDelta) {
                        maxDelta = delta;
                    }
                }
            }
        }
        minValue = baseValue;
        return maxDelta;
    }

    static void ConvertToDeltaBuffer(UT* slotBuffer, uint32_t itemCount, UT minValue)
    {
        for (size_t i = 0; i < itemCount; ++i) {
            assert(slotBuffer[i] >= minValue);
            slotBuffer[i] = slotBuffer[i] - minValue;
        }
    }

    static size_t GetMinDeltaBufferSize(UT maxDelta, uint32_t bufferItemCount)
    {
        SlotItemType slotType = GetSlotItemType(maxDelta);
        return GetCompressDeltaBufferSize(slotType, bufferItemCount);
    }

    static size_t CalculateDeltaBufferSize(UT* buffer, uint32_t cursorInBuffer);

    static bool NeedStoreDelta(UT maxDelta, UT minValue);

    size_t FlushDeltaBuffer(UT* deltaBuffer, uint32_t itemCount, UT minValue, SlotItemType slotType);

    size_t CompressDeltaBuffer(UT* deltaBuffer, uint32_t itemCount, SlotItemType slotType);

    void AppendSlotItem(SlotItemType slotType, uint64_t value);

    // for long value
    static size_t CalculateLongValueDeltaBufferSize(UT* buffer, uint32_t cursorInBuffer);

    static bool NeedStoreLongValueDelta(UT maxDelta, UT minValue);

    size_t FlushLongValueDeltaBuffer(UT* deltaBuffer, uint32_t itemCount, UT minValue, SlotItemType slotType);
    void AppendLongValueSlotItem(SlotItemType slotType, uint64_t value);

private:
    UT* _slotBuffer;
    uint32_t _cursorInBuffer;

private:
    friend class EquivalentCompressReader<T>;
    friend class EquivalentCompressWriterTest;
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, EquivalentCompressWriter, T);

///////////////////////////////////////////////////////////////
template <typename T>
inline size_t EquivalentCompressWriter<T>::CompressDeltaBuffer(UT* deltaBuffer, uint32_t itemCount,
                                                               SlotItemType slotType)
{
    for (uint32_t i = 0; i < itemCount; i++) {
        EquivalentCompressReader<T>::InplaceUpdateDeltaArray((uint8_t*)deltaBuffer, slotType, i, deltaBuffer[i]);
    }
    return GetCompressDeltaBufferSize(slotType, itemCount);
}

template <typename T>
inline size_t EquivalentCompressWriter<T>::FlushDeltaBuffer(UT* deltaBuffer, uint32_t itemCount, UT minValue,
                                                            SlotItemType slotType)
{
    size_t idx = 0;
    _dataArray->SetTypedValue<UT>(_dataArrayCursor + idx, minValue);
    idx += sizeof(UT);

    size_t compsize = CompressDeltaBuffer(deltaBuffer, itemCount, slotType);
    _dataArray->SetList(_dataArrayCursor + idx, (const char*)deltaBuffer, compsize);
    idx += compsize;
    return idx;
}

template <typename T>
inline size_t EquivalentCompressWriter<T>::FlushLongValueDeltaBuffer(UT* deltaBuffer, uint32_t itemCount, UT minValue,
                                                                     SlotItemType slotType)
{
#undef DumpDeltaFlag
#define DumpDeltaFlag(DELTA_FLAG)                                                                                      \
    UT deltaFlag = DELTA_FLAG;                                                                                         \
    _dataArray->SetTypedValue<UT>(_dataArrayCursor + idx, deltaFlag);                                                  \
    idx += sizeof(UT);

    size_t idx = 0;
    _dataArray->SetTypedValue<UT>(_dataArrayCursor + idx, minValue);
    idx += sizeof(UT);
    DumpDeltaFlag(SlotItemTypeToDeltaFlag(slotType));
    size_t compsize = CompressDeltaBuffer(deltaBuffer, itemCount, slotType);
    _dataArray->SetList(_dataArrayCursor + idx, (const char*)deltaBuffer, compsize);
    idx += compsize;
    return idx;
}

template <>
inline size_t EquivalentCompressWriter<uint64_t>::FlushDeltaBuffer(uint64_t* deltaBuffer, uint32_t itemCount,
                                                                   uint64_t minValue, SlotItemType slotType)
{
    return FlushLongValueDeltaBuffer(deltaBuffer, itemCount, minValue, slotType);
}

template <>
inline size_t EquivalentCompressWriter<int64_t>::FlushDeltaBuffer(uint64_t* deltaBuffer, uint32_t itemCount,
                                                                  uint64_t minValue, SlotItemType slotType)
{
    return FlushLongValueDeltaBuffer(deltaBuffer, itemCount, minValue, slotType);
}

template <>
inline size_t EquivalentCompressWriter<double>::FlushDeltaBuffer(uint64_t* deltaBuffer, uint32_t itemCount,
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
inline bool EquivalentCompressWriter<uint64_t>::NeedStoreDelta(uint64_t maxDelta, uint64_t minValue)
{
    return NeedStoreLongValueDelta(maxDelta, minValue);
}

template <>
inline bool EquivalentCompressWriter<int64_t>::NeedStoreDelta(uint64_t maxDelta, uint64_t minValue)
{
    return NeedStoreLongValueDelta(maxDelta, minValue);
}

template <>
inline bool EquivalentCompressWriter<double>::NeedStoreDelta(uint64_t maxDelta, uint64_t minValue)
{
    return NeedStoreLongValueDelta(maxDelta, minValue);
}

template <typename T>
inline void EquivalentCompressWriter<T>::AppendSlotItem(SlotItemType slotType, uint64_t value)
{
    union SlotItemUnion {
        SlotItem slotItem;
        uint64_t slotValue;
    };

    SlotItemUnion slotUnion;
    slotUnion.slotItem.slotType = slotType;
    slotUnion.slotItem.value = value;
    _indexArray->Set(_itemCount >> _slotItemPowerNum, slotUnion.slotValue);
}

template <typename T>
inline void EquivalentCompressWriter<T>::AppendLongValueSlotItem(SlotItemType slotType, uint64_t value)
{
    union SlotItemUnion {
        LongSlotItem slotItem;
        uint64_t slotValue;
    };

    SlotItemUnion slotUnion;
    slotUnion.slotItem.isValue = (slotType == SIT_EQUAL) ? 1 : 0;
    slotUnion.slotItem.value = value;
    _indexArray->Set(_itemCount >> _slotItemPowerNum, slotUnion.slotValue);
}

template <>
inline void EquivalentCompressWriter<uint64_t>::AppendSlotItem(SlotItemType slotType, uint64_t value)
{
    AppendLongValueSlotItem(slotType, value);
}

template <>
inline void EquivalentCompressWriter<int64_t>::AppendSlotItem(SlotItemType slotType, uint64_t value)
{
    AppendLongValueSlotItem(slotType, value);
}

template <>
inline void EquivalentCompressWriter<double>::AppendSlotItem(SlotItemType slotType, uint64_t value)
{
    AppendLongValueSlotItem(slotType, value);
}

template <typename T>
inline void EquivalentCompressWriter<T>::CompressData(T* data, uint32_t count)
{
    for (uint32_t idx = 0; idx < count; ++idx) {
        assert(_cursorInBuffer < _slotItemCount);
        _slotBuffer[_cursorInBuffer++] = ZigZagEncoder::Encode(data[idx]);
        if (_cursorInBuffer == _slotItemCount) {
            FlushSlotBuffer();
        }
    }
}

template <typename T>
inline Status EquivalentCompressWriter<T>::CompressData(const EquivalentCompressReader<T>& reader)
{
    uint32_t count = reader.Size();
    for (size_t idx = 0; idx < count; ++idx) {
        assert(_cursorInBuffer < _slotItemCount);
        auto [status, curVal] = reader[idx];
        RETURN_IF_STATUS_ERROR(status, "read data fail from EquivalentCompressReader");
        _slotBuffer[_cursorInBuffer++] = ZigZagEncoder::Encode(curVal);
        if (_cursorInBuffer == _slotItemCount) {
            FlushSlotBuffer();
        }
    }
    return Status::OK();
}

template <typename T>
inline size_t EquivalentCompressWriter<T>::CalculateDeltaBufferSize(UT* buffer, uint32_t bufferSize)
{
    UT minValue = 0;
    UT maxDelta = GetMaxDeltaInBuffer(buffer, bufferSize, minValue);

    if (!NeedStoreDelta(maxDelta, minValue)) {
        return 0;
    }

    return sizeof(UT) + GetMinDeltaBufferSize(maxDelta, bufferSize);
}

template <typename T>
inline size_t EquivalentCompressWriter<T>::CalculateLongValueDeltaBufferSize(UT* buffer, uint32_t cursorInBuffer)
{
    UT minValue = 0;
    UT maxDelta = GetMaxDeltaInBuffer(buffer, cursorInBuffer, minValue);

    if (!NeedStoreDelta(maxDelta, minValue)) {
        return 0;
    }
    return sizeof(UT) * 2 + GetMinDeltaBufferSize(maxDelta, cursorInBuffer);
}

template <>
inline size_t EquivalentCompressWriter<uint64_t>::CalculateDeltaBufferSize(uint64_t* buffer, uint32_t cursorInBuffer)
{
    return CalculateLongValueDeltaBufferSize(buffer, cursorInBuffer);
}

template <>
inline size_t EquivalentCompressWriter<int64_t>::CalculateDeltaBufferSize(uint64_t* buffer, uint32_t cursorInBuffer)
{
    return CalculateLongValueDeltaBufferSize(buffer, cursorInBuffer);
}

template <>
inline size_t EquivalentCompressWriter<double>::CalculateDeltaBufferSize(uint64_t* buffer, uint32_t cursorInBuffer)
{
    return CalculateLongValueDeltaBufferSize(buffer, cursorInBuffer);
}

////////////////////////////////not support type/////////////////////
template <>
inline size_t EquivalentCompressWriter<autil::uint128_t>::CalculateDeltaBufferSize(autil::uint128_t* buffer,
                                                                                   uint32_t cursorInBuffer)
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
inline Status
EquivalentCompressWriter<autil::uint128_t>::CompressData(const EquivalentCompressReader<autil::uint128_t>& reader)
{
    assert(false);
    return Status::InternalError("un-support compress operation for type : [autil::uint128_t]");
}

template <>
inline std::pair<Status, size_t>
EquivalentCompressWriter<autil::uint128_t>::CalculateCompressLength(autil::uint128_t* data, uint32_t count,
                                                                    uint32_t slotItemCount)
{
    assert(false);
    return std::make_pair(Status::InternalError("un-support type [autil::uint128]"), 0);
}

template <>
inline std::pair<Status, size_t> EquivalentCompressWriter<autil::uint128_t>::CalculateCompressLength(
    const EquivalentCompressReader<autil::uint128_t>& reader, uint32_t slotItemCount)
{
    assert(false);
    return std::make_pair(Status::InternalError("un-support type [autil::uint128]"), 0);
}

template <>
inline void EquivalentCompressWriter<autil::uint128_t>::FlushSlotBuffer()
{
    assert(false);
}
} // namespace indexlib::index
