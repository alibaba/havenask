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

#include "autil/LongHashValue.h"
#include "autil/MultiValueType.h"
#include "future_lite/Future.h"
#include "indexlib/base/Define.h"
#include "indexlib/base/Status.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/file_system/stream/FileStreamCreator.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressDefine.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressTraits.h"
#include "indexlib/util/PooledUniquePtr.h"
#include "indexlib/util/slice_array/BytesAlignedSliceArray.h"

namespace indexlib::file_system {
class FileReader;
}
namespace indexlib::index {

struct EquivalentCompressUpdateMetrics {
public:
    EquivalentCompressUpdateMetrics() : noUsedBytesSize(0), inplaceUpdateCount(0), expandUpdateCount(0) {}

    bool operator==(const EquivalentCompressUpdateMetrics& other) const
    {
        return noUsedBytesSize == other.noUsedBytesSize && inplaceUpdateCount == other.inplaceUpdateCount &&
               expandUpdateCount == other.expandUpdateCount;
    }

    size_t noUsedBytesSize;
    uint32_t inplaceUpdateCount;
    uint32_t expandUpdateCount;
};

template <typename T>
class EquivalentCompressSessionReader;

class EquivalentCompressReaderBase
{
public:
    // std::numeric_limits<uint64_t>::max();
    static const uint64_t INVALID_COMPRESS_LEN = 0xFFFFFFFFFFFFFFFF;

public:
    EquivalentCompressReaderBase(const uint8_t* buffer, int64_t compLen);
    EquivalentCompressReaderBase();
    virtual ~EquivalentCompressReaderBase();

    uint32_t Size() const { return _itemCount; }

    void Init(const uint8_t* buffer, int64_t compLen,
              const std::shared_ptr<indexlib::util::BytesAlignedSliceArray>& expandSliceArray);
    bool Init(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader);

    EquivalentCompressUpdateMetrics GetUpdateMetrics();

    EquivalentCompressFileContent GetFileContent() const { return *_fileContent; }

    std::shared_ptr<indexlib::file_system::FileStream> GetFileStream() const { return _fileStream; }

protected:
    void DecodePosition(size_t pos, size_t& slotIdx, size_t& valueIdx) const
    {
        slotIdx = pos >> _slotBitNum;
        valueIdx = pos & _slotMask;
    }

    uint32_t GetSlotItemCount() const { return (uint32_t)1 << _slotBitNum; }

    bool IsReadFromMemory() const __ALWAYS_INLINE { return !_fileReader; }

    std::shared_ptr<indexlib::file_system::FileReader> GetFileReader() const __ALWAYS_INLINE { return _fileReader; }

    inline std::pair<Status, SlotItem> GetSlotItem(const std::shared_ptr<indexlib::file_system::FileStream>& fileStream,
                                                   size_t slotIdx) const __ALWAYS_INLINE
    {
        SlotItem slotItem;
        if (_slotBaseAddr) {
            slotItem = *((SlotItem*)_slotBaseAddr + slotIdx);
            return std::make_pair(Status::OK(), slotItem);
        }
        assert(fileStream);
        auto [status, _] = fileStream
                               ->Read(&slotItem, sizeof(SlotItem), _slotBaseOffset + slotIdx * sizeof(SlotItem),
                                      indexlib::file_system::ReadOption::LowLatency())
                               .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, slotItem, "read slot interval fail form FileStream");
        return std::make_pair(Status::OK(), slotItem);
    }

    inline future_lite::Future<size_t>
    AsyncGetSlotItem(const std::shared_ptr<indexlib::file_system::FileStream>& fileStream, size_t slotIdx,
                     SlotItem* slotItem, indexlib::file_system::ReadOption readOption) const __ALWAYS_INLINE
    {
        if (_slotBaseAddr) {
            *slotItem = *((SlotItem*)_slotBaseAddr + slotIdx);
            return future_lite::makeReadyFuture(size_t(0));
        }
        assert(fileStream);
        return fileStream
            ->ReadAsync(slotItem, sizeof(SlotItem), _slotBaseOffset + slotIdx * sizeof(SlotItem), readOption)
            .thenValue([](size_t readSize) { return future_lite::makeReadyFuture(size_t(readSize)); });
    }

    inline std::pair<Status, LongSlotItem>
    GetLongSlotItem(const std::shared_ptr<indexlib::file_system::FileStream>& fileStream,
                    size_t slotIdx) const __ALWAYS_INLINE
    {
        LongSlotItem slotItem;
        if (_slotBaseAddr) {
            slotItem = *((LongSlotItem*)_slotBaseAddr + slotIdx);
            return std::make_pair(Status::OK(), slotItem);
        }
        assert(fileStream);
        auto [status, _] = fileStream
                               ->Read(&slotItem, sizeof(LongSlotItem), _slotBaseOffset + slotIdx * sizeof(LongSlotItem),
                                      indexlib::file_system::ReadOption::LowLatency())
                               .StatusWith();
        return std::make_pair(status, slotItem);
    }

    inline future_lite::Future<size_t>
    AsyncGetLongSlotItem(const std::shared_ptr<indexlib::file_system::FileStream>& fileStream, size_t slotIdx,
                         LongSlotItem* longSlotItem, indexlib::file_system::ReadOption readOption) const __ALWAYS_INLINE
    {
        if (_slotBaseAddr) {
            *longSlotItem = *((LongSlotItem*)_slotBaseAddr + slotIdx);
            return future_lite::makeReadyFuture(size_t(0));
        }
        assert(fileStream);
        return fileStream->ReadAsync(longSlotItem, sizeof(LongSlotItem),
                                     _slotBaseOffset + slotIdx * sizeof(LongSlotItem), readOption);
    }

    void InitInplaceUpdateFlags();

    inline uint8_t* GetDeltaBlockAddress(size_t offset) const __ALWAYS_INLINE
    {
        if (offset < _compressLen) {
            return _valueBaseAddr + offset;
        }

        size_t expandOffset = offset - _compressLen;
        assert(_expandSliceArray);
        return (uint8_t*)_expandSliceArray->GetValueAddress(expandOffset);
    }

    void InitUpdateMetrics();
    Status WriteUpdateMetricsToEmptySlice();

    Status IncreaseNoUsedMemorySize(size_t noUsedBytes);
    Status IncreaseInplaceUpdateCount();
    Status IncreaseExpandUpdateCount();

private:
    template <typename T>
    friend class EquivalentCompressSessionReader;

protected:
    uint32_t _itemCount;
    uint32_t _slotBitNum;
    uint32_t _slotMask;
    uint32_t _slotNum; // TODO(xiuchen) not used ?
    uint64_t* _slotBaseAddr;
    uint8_t* _valueBaseAddr;
    uint64_t _slotBaseOffset;
    uint64_t _valueBaseOffset;

    std::unique_ptr<EquivalentCompressFileContent> _fileContent;

    std::shared_ptr<indexlib::file_system::FileStream> _fileStream;
    std::shared_ptr<indexlib::file_system::FileReader> _fileReader;
    uint64_t _compressLen;
    std::shared_ptr<indexlib::util::BytesAlignedSliceArray> _expandSliceArray;
    uint8_t* _expandDataBuffer;

    EquivalentCompressUpdateMetrics* _updateMetrics;

    bool INPLACE_UPDATE_FLAG_ARRAY[8][8];

protected:
    friend class EquivalentCompressReaderTest;

protected:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////////

template <typename T>
class EquivalentCompressReader : public EquivalentCompressReaderBase
{
public:
    typedef typename EquivalentCompressTraits<T>::ZigZagEncoder ZigZagEncoder;
    typedef typename EquivalentCompressTraits<T>::EncoderType UT;
    /*    T: uint8_t  / int8_t   UT: uint8_t;    */
    /*    T: uint16_t / int16_t  UT: uint16_t;   */
    /*    T: uint32_t / int32_t / float   UT: uint32_t;   */
    /*    T: uint64_t / int64_t / double  UT: uint64_t;   */

    typedef DeltaValueArrayTyped<UT> DeltaValueArray;

    class Iterator
    {
    public:
        Iterator(const EquivalentCompressReader<T>* reader) : _reader(reader), _cursor(0) {}

        bool HasNext() const { return _cursor < _reader->Size(); }

        std::pair<Status, T> Next()
        {
            assert(_reader);
            return _reader->Get(_cursor++);
        }

    private:
        const EquivalentCompressReader<T>* _reader;
        uint32_t _cursor;
    };

public:
    EquivalentCompressReader(const uint8_t* buffer, int64_t compLen = INVALID_COMPRESS_LEN)
        : EquivalentCompressReaderBase(buffer, compLen)
    {
    }

    EquivalentCompressReader() {}

    ~EquivalentCompressReader() {}

    inline std::pair<Status, T> operator[](size_t pos) const __ALWAYS_INLINE;

    inline std::pair<Status, T> Get(size_t pos) const __ALWAYS_INLINE { return (*this)[pos]; }

    bool Update(size_t pos, T value);

    Iterator CreateIterator() const
    {
        Iterator iter(this);
        return iter;
    }
    EquivalentCompressSessionReader<T>
    CreateSessionReader(autil::mem_pool::Pool* sessionPool,
                        const EquivalentCompressSessionOption& sessionOption) const __ALWAYS_INLINE
    {
        return EquivalentCompressSessionReader<T>(const_cast<EquivalentCompressReader*>(this), sessionOption,
                                                  sessionPool);
    }

    indexlib::util::PooledUniquePtr<EquivalentCompressSessionReader<T>>
    CreateSessionReaderPtr(autil::mem_pool::Pool* sessionPool, const EquivalentCompressSessionOption& sessionOption)
    {
        return indexlib::util::PooledUniquePtr<EquivalentCompressSessionReader<T>>(
            IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, EquivalentCompressSessionReader<T>, this, sessionOption,
                                         sessionPool),
            sessionPool);
    }

public:
    static void InplaceUpdateDeltaArray(uint8_t* deltaArray, SlotItemType slotType, uint32_t valueIdx, UT updateDelta);

private:
    future_lite::Future<size_t> AsyncGetDeltaArray(const std::shared_ptr<indexlib::file_system::FileStream>& fileStream,
                                                   size_t slotIdx, SlotItem* slotItem, UT* baseValue,
                                                   uint8_t* deltaArray, indexlib::file_system::ReadOption readOption);
    std::pair<Status, size_t> GetDeltaArray(const std::shared_ptr<indexlib::file_system::FileStream>& fileStream,
                                            size_t slotIdx, SlotItem& slotItem, UT& baseValue, uint8_t* deltaArray);
    std::pair<Status, size_t>
    GetLongValueDeltaArray(const std::shared_ptr<indexlib::file_system::FileStream>& fileStream, size_t slotIdx,
                           LongSlotItem& slotItem, LongValueArrayHeader& header, uint8_t* deltaArray);

    future_lite::Future<size_t>
    AsyncGetLongValueDeltaArray(const std::shared_ptr<indexlib::file_system::FileStream>& fileStream, size_t slotIdx,
                                LongSlotItem* slotItem, LongValueArrayHeader* header, uint8_t* deltaArray,
                                indexlib::file_system::ReadOption readOption);
    // for uint64_t, int64_t, double
    bool UpdateLongValueType(size_t pos, T value);
    bool ExpandUpdateLongValueTypeDeltaArray(uint8_t* slotItem, uint8_t* slotData, size_t pos, UT updateEncodeValue);

    inline std::pair<Status, T> ReadLongValueType(size_t pos) const __ALWAYS_INLINE;

    bool ExpandUpdateDeltaArray(uint8_t* slotItem, uint8_t* slotData, size_t pos, UT updateEncodeValue);

    inline std::pair<Status, UT> ReadDeltaValueBySlotItem(SlotItemType slotType, size_t valueOffset,
                                                          uint32_t valueIdx) const;

    inline std::pair<Status, UT> ReadDeltaValueByDeltaType(uint64_t deltaType, uint64_t valueOffset,
                                                           size_t valueIdx) const __ALWAYS_INLINE;

    inline size_t GetDeltaArraySizeBySlotType(SlotItemType slotType, size_t count) const __ALWAYS_INLINE;

    // for equal slot expand
    void GenerateExpandDeltaBuffer(UT equalValue, UT updateValue, size_t pos, uint8_t* deltaBuffer, UT& expandBaseValue,
                                   SlotItemType& expandSlotType);

    // for non-equal slot expand, return deltaBuffer size before expand
    void GenerateExpandDeltaBuffer(UT curBaseValue, SlotItemType curSlotType, uint8_t* curDeltaAddress,
                                   UT updateEncodeValue, size_t pos, UT* deltaBuffer, UT& expandBaseValue,
                                   SlotItemType& expandSlotType);

    bool ReachMaxDeltaType(SlotItemType slotType);

    size_t GetExpandFreeMemoryUse(SlotItemType curSlotType, uint64_t slotValue, size_t pos);

    friend class EquivalentCompressSessionReader<T>;
};

///////////////////////////////////////////////////////////////

#define DECLARE_UNSUPPORT_TYPE_IMPL(type)                                                                              \
    template <>                                                                                                        \
    inline std::pair<Status, type> EquivalentCompressReader<type>::operator[](size_t rowId) const                      \
    {                                                                                                                  \
        assert(false);                                                                                                 \
        return std::make_pair(Status::OK(), type());                                                                   \
    }                                                                                                                  \
    template <>                                                                                                        \
    inline bool EquivalentCompressReader<type>::Update(size_t pos, type value)                                         \
    {                                                                                                                  \
        assert(false);                                                                                                 \
        return false;                                                                                                  \
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

#undef DECLARE_UNSUPPORT_TYPE_IMPL

template <typename T>
inline std::pair<Status, T> EquivalentCompressReader<T>::operator[](size_t pos) const
{
    if constexpr (std::is_same<T, uint64_t>::value || std::is_same<T, int64_t>::value ||
                  std::is_same<T, double>::value) {
        return ReadLongValueType(pos);
    }
    assert(pos < _itemCount);
    auto [status, slotItem] = GetSlotItem(_fileStream, pos >> _slotBitNum);
    RETURN2_IF_STATUS_ERROR(status, 0, "get slot item fail");
    if (slotItem.slotType == SIT_EQUAL) {
        return std::make_pair(Status::OK(), ZigZagEncoder::Decode((UT)(slotItem.value)));
    }
    uint32_t valueIdx = pos & _slotMask;
    UT deltaValue = 0;
    typedef DeltaValueArrayTyped<UT> DeltaValueArray;
    if (_valueBaseAddr) {
        uint8_t* valueItemsAddr = GetDeltaBlockAddress(slotItem.value);
        DeltaValueArray* valueArray = (DeltaValueArray*)valueItemsAddr;
        deltaValue = EquivalentCompressFileFormat::GetDeltaValueBySlotItemType<UT>(slotItem.slotType, valueArray->delta,
                                                                                   valueIdx);
        return std::make_pair(Status::OK(), ZigZagEncoder::Decode(valueArray->baseValue + deltaValue));
    }
    DeltaValueArray arrayHeader;
    size_t valueOffset = _valueBaseOffset + slotItem.value;
    auto [readStatus, curLen] =
        _fileStream
            ->Read(&arrayHeader, sizeof(DeltaValueArray), valueOffset, indexlib::file_system::ReadOption::LowLatency())
            .StatusWith();
    RETURN2_IF_STATUS_ERROR(readStatus, 0, "read array header fail from FileStrem");
    valueOffset += curLen;
    std::tie(status, deltaValue) = ReadDeltaValueBySlotItem(slotItem.slotType, valueOffset, valueIdx);
    RETURN2_IF_STATUS_ERROR(status, 0, "read delta value by slot item fail");
    return std::make_pair(Status::OK(), ZigZagEncoder::Decode(arrayHeader.baseValue + deltaValue));
}

template <typename T>
inline std::pair<Status, T> EquivalentCompressReader<T>::ReadLongValueType(size_t pos) const
{
    assert(pos < _itemCount);
    auto [status, slotItem] = GetLongSlotItem(_fileStream, pos >> _slotBitNum);
    RETURN2_IF_STATUS_ERROR(status, 0, "get long slot item from FileStream fail");
    if (slotItem.isValue == 1) {
        return std::make_pair(Status::OK(), ZigZagEncoder::Decode(slotItem.value));
    }
    uint32_t valueIdx = pos & _slotMask;
    if (_valueBaseAddr) {
        uint8_t* valueItemsAddr = GetDeltaBlockAddress(slotItem.value);
        LongValueArrayHeader* valueArray = (LongValueArrayHeader*)valueItemsAddr;
        uint8_t* deltaArray = valueItemsAddr + sizeof(LongValueArrayHeader);
        UT deltaValue =
            EquivalentCompressFileFormat::GetDeltaValueByDeltaType<UT>(valueArray->deltaType, deltaArray, valueIdx);
        return std::make_pair(Status::OK(), ZigZagEncoder::Decode(valueArray->baseValue + deltaValue));
    }
    LongValueArrayHeader arrayHeader;
    size_t valueOffset = _valueBaseOffset + slotItem.value;
    auto [headerStatus, curLen] =
        _fileStream
            ->Read(&arrayHeader, sizeof(arrayHeader), valueOffset, indexlib::file_system::ReadOption::LowLatency())
            .StatusWith();
    RETURN2_IF_STATUS_ERROR(headerStatus, 0, "read array header fail");
    valueOffset += curLen;
    auto [deltaStatus, deltaValue] = ReadDeltaValueByDeltaType(arrayHeader.deltaType, valueOffset, valueIdx);
    RETURN2_IF_STATUS_ERROR(deltaStatus, 0, "read delta value by delta type fail");
    return std::make_pair(Status::OK(), ZigZagEncoder::Decode(arrayHeader.baseValue + deltaValue));
}

template <typename T>
inline bool EquivalentCompressReader<T>::ExpandUpdateDeltaArray(uint8_t* slotItem, uint8_t* slotData, size_t pos,
                                                                UT updateEncodeValue)
{
    if (!_expandSliceArray || _compressLen == INVALID_COMPRESS_LEN) {
        return false;
    }

    uint32_t slotItemCount = (uint32_t)1 << _slotBitNum;
    if (!_expandDataBuffer) {
        size_t maxExpandSize = sizeof(UT) + slotItemCount * sizeof(UT);
        _expandDataBuffer = new uint8_t[maxExpandSize];
    }
    UT* deltaBuffer = (UT*)(_expandDataBuffer + sizeof(UT));

    SlotItemType expandSlotType;
    UT expandBaseValue;
    SlotItem* slotItemTyped = (SlotItem*)slotItem;
    if (slotData == NULL) {
        assert(slotItemTyped->slotType == SIT_EQUAL);
        GenerateExpandDeltaBuffer((UT)slotItemTyped->value, updateEncodeValue, pos, (uint8_t*)deltaBuffer,
                                  expandBaseValue, expandSlotType);
    } else {
        assert(slotItemTyped->slotType != SIT_EQUAL);
        uint8_t* valueItemsAddr = GetDeltaBlockAddress(slotItemTyped->value);
        typedef DeltaValueArrayTyped<UT> DeltaValueArray;
        DeltaValueArray* valueArray = (DeltaValueArray*)valueItemsAddr;

        GenerateExpandDeltaBuffer(valueArray->baseValue, slotItemTyped->slotType, valueArray->delta, updateEncodeValue,
                                  pos, deltaBuffer, expandBaseValue, expandSlotType);
    }

    size_t freeMemorySize = GetExpandFreeMemoryUse(slotItemTyped->slotType, slotItemTyped->value, pos);
    auto st = IncreaseNoUsedMemorySize(freeMemorySize);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "increase no used memory failed.");
        return false;
    }
    st = IncreaseExpandUpdateCount();
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "increase expand update count failed.");
        return false;
    }

    size_t compSize = GetCompressDeltaBufferSize(expandSlotType, slotItemCount);
    *(UT*)_expandDataBuffer = expandBaseValue;
    compSize += sizeof(UT);
    int64_t expandOffset = _expandSliceArray->Insert(_expandDataBuffer, compSize);

    SlotItem newSlotItem;
    newSlotItem.slotType = expandSlotType;
    newSlotItem.value = expandOffset + _compressLen;
    char* newSlotItemAddr = (char*)(&newSlotItem);
    MEMORY_BARRIER();
    *(uint64_t*)slotItem = *(uint64_t*)newSlotItemAddr;
    return true;
}

template <typename T>
inline bool EquivalentCompressReader<T>::ExpandUpdateLongValueTypeDeltaArray(uint8_t* slotItem, uint8_t* slotData,
                                                                             size_t pos, UT updateEncodeValue)
{
    if (!_expandSliceArray || _compressLen == INVALID_COMPRESS_LEN) {
        return false;
    }

    uint32_t slotItemCount = (uint32_t)1 << _slotBitNum;
    size_t headerLen = sizeof(UT) * 2;
    if (!_expandDataBuffer) {
        size_t maxExpandSize = headerLen + slotItemCount * sizeof(UT);
        _expandDataBuffer = new uint8_t[maxExpandSize];
    }
    UT* deltaBuffer = (UT*)(_expandDataBuffer + headerLen);

    SlotItemType expandSlotType;
    UT expandBaseValue;
    LongSlotItem* slotItemTyped = (LongSlotItem*)slotItem;
    SlotItemType slotType = SIT_EQUAL;
    if (slotData == NULL) {
        assert(slotItemTyped->isValue == 1);
        GenerateExpandDeltaBuffer((UT)slotItemTyped->value, updateEncodeValue, pos, (uint8_t*)deltaBuffer,
                                  expandBaseValue, expandSlotType);
    } else {
        assert(slotItemTyped->isValue != 1);
        uint8_t* valueItemsAddr = GetDeltaBlockAddress(slotItemTyped->value);
        LongValueArrayHeader* valueArray = (LongValueArrayHeader*)valueItemsAddr;
        slotType = DeltaFlagToSlotItemType(valueArray->deltaType);
        uint8_t* deltaArray = valueItemsAddr + sizeof(LongValueArrayHeader);
        GenerateExpandDeltaBuffer(valueArray->baseValue, slotType, deltaArray, updateEncodeValue, pos, deltaBuffer,
                                  expandBaseValue, expandSlotType);
    }

    size_t freeMemorySize = GetExpandFreeMemoryUse(slotType, slotItemTyped->value, pos);
    auto st = IncreaseNoUsedMemorySize(freeMemorySize);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "increase no used memory failed.");
        return false;
    }
    st = IncreaseExpandUpdateCount();
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "increase expand update count failed.");
        return false;
    }

    size_t compSize = GetCompressDeltaBufferSize(expandSlotType, slotItemCount);
    UT* blockHeader = (UT*)_expandDataBuffer;
    *blockHeader = expandBaseValue;
    *(blockHeader + 1) = SlotItemTypeToDeltaFlag(expandSlotType);
    compSize += headerLen;
    int64_t expandOffset = _expandSliceArray->Insert(_expandDataBuffer, compSize);

    LongSlotItem newSlotItem;
    newSlotItem.isValue = 0;
    newSlotItem.value = expandOffset + _compressLen;
    char* newSlotItemAddr = (char*)(&newSlotItem);
    MEMORY_BARRIER();
    *(uint64_t*)slotItem = *(uint64_t*)newSlotItemAddr;
    return true;
}

template <typename T>
inline bool EquivalentCompressReader<T>::Update(size_t pos, T value)
{
    if constexpr (std::is_same<T, uint64_t>::value || std::is_same<T, int64_t>::value ||
                  std::is_same<T, double>::value) {
        return UpdateLongValueType(pos, value);
    }
    if (pos >= _itemCount || !_slotBaseAddr) {
        return false;
    }

    UT updateEncodeValue = ZigZagEncoder::Encode(value);
    SlotItem* slotItem = (SlotItem*)_slotBaseAddr + (pos >> _slotBitNum);
    if (slotItem->slotType == SIT_EQUAL) {
        if ((UT)slotItem->value == updateEncodeValue) {
            return true;
        }
        return ExpandUpdateDeltaArray((uint8_t*)slotItem, NULL, pos, updateEncodeValue);
    }

    uint8_t* valueItemsAddr = GetDeltaBlockAddress(slotItem->value);
    DeltaValueArray* valueArray = (DeltaValueArray*)valueItemsAddr;
    if (updateEncodeValue < valueArray->baseValue) {
        return ExpandUpdateDeltaArray((uint8_t*)slotItem, valueItemsAddr, pos, updateEncodeValue);
    }

    UT updateDelta = updateEncodeValue - valueArray->baseValue;
    SlotItemType updatedSlotItemType = GetSlotItemType(updateDelta);
    if (!INPLACE_UPDATE_FLAG_ARRAY[slotItem->slotType][updatedSlotItemType]) {
        return ExpandUpdateDeltaArray((uint8_t*)slotItem, valueItemsAddr, pos, updateEncodeValue);
    }

    uint32_t valueIdx = pos & _slotMask;
    InplaceUpdateDeltaArray(valueArray->delta, slotItem->slotType, valueIdx, updateDelta);
    auto st = IncreaseInplaceUpdateCount();
    return st.IsOK();
}

template <typename T>
inline bool EquivalentCompressReader<T>::UpdateLongValueType(size_t pos, T value)
{
    if (pos >= _itemCount) {
        return false;
    }

    uint64_t updateEncodeValue = ZigZagEncoder::Encode(value);
    LongSlotItem* slotItem = (LongSlotItem*)_slotBaseAddr + (pos >> _slotBitNum);
    if (slotItem->isValue == 1) {
        if (slotItem->value == updateEncodeValue) {
            return true;
        }
        return ExpandUpdateLongValueTypeDeltaArray((uint8_t*)slotItem, NULL, pos, updateEncodeValue);
    }

    uint8_t* valueItemsAddr = GetDeltaBlockAddress(slotItem->value);
    LongValueArrayHeader* valueArray = (LongValueArrayHeader*)valueItemsAddr;
    if (updateEncodeValue < valueArray->baseValue) {
        return ExpandUpdateLongValueTypeDeltaArray((uint8_t*)slotItem, valueItemsAddr, pos, updateEncodeValue);
    }

    SlotItemType slotItemType = DeltaFlagToSlotItemType(valueArray->deltaType);
    uint64_t updateDelta = updateEncodeValue - valueArray->baseValue;
    SlotItemType updatedSlotItemType = GetSlotItemType(updateDelta);
    if (!INPLACE_UPDATE_FLAG_ARRAY[slotItemType][updatedSlotItemType]) {
        return ExpandUpdateLongValueTypeDeltaArray((uint8_t*)slotItem, valueItemsAddr, pos, updateEncodeValue);
    }

    uint8_t* deltaArray = valueItemsAddr + sizeof(LongValueArrayHeader);
    uint32_t valueIdx = pos & _slotMask;
    InplaceUpdateDeltaArray(deltaArray, slotItemType, valueIdx, updateDelta);
    auto st = IncreaseInplaceUpdateCount();
    return st.IsOK();
    ;
}

template <typename T>
inline std::pair<Status, size_t>
EquivalentCompressReader<T>::GetDeltaArray(const std::shared_ptr<indexlib::file_system::FileStream>& fileStream,
                                           size_t slotIdx, SlotItem& slotItem, UT& baseValue, uint8_t* deltaArray)
{
    typedef DeltaValueArrayTyped<UT> DeltaValueArray;
    DeltaValueArray arrayHeader;
    size_t valueOffset = _valueBaseOffset + slotItem.value;
    auto [status, curLen] =
        fileStream
            ->Read(&arrayHeader, sizeof(DeltaValueArray), valueOffset, indexlib::file_system::ReadOption::LowLatency())
            .StatusWith();
    RETURN2_IF_STATUS_ERROR(status, 0, "read array header fail from FileStream");
    valueOffset += curLen;
    baseValue = arrayHeader.baseValue;
    size_t slotSize =
        (slotIdx == ((_itemCount - 1) >> _slotBitNum)) ? ((_itemCount - 1) & _slotMask) + 1 : GetSlotItemCount();
    size_t deltaArraySize = GetDeltaArraySizeBySlotType(slotItem.slotType, slotSize);
    std::tie(status, curLen) =
        fileStream->Read(deltaArray, deltaArraySize, valueOffset, indexlib::file_system::ReadOption::LowLatency())
            .StatusWith();
    RETURN2_IF_STATUS_ERROR(status, 0, "read delta array fail from FileStream");
    return std::make_pair(Status::OK(), curLen);
}

template <typename T>
inline future_lite::Future<size_t>
EquivalentCompressReader<T>::AsyncGetDeltaArray(const std::shared_ptr<indexlib::file_system::FileStream>& fileStream,
                                                size_t slotIdx, SlotItem* slotItem, UT* baseValue, uint8_t* deltaArray,
                                                indexlib::file_system::ReadOption readOption)
{
    size_t valueOffset = _valueBaseOffset + slotItem->value;
    return fileStream->ReadAsync(baseValue, sizeof(UT), valueOffset, readOption)
        .thenValue([this, fileStream, slotIdx, slotItem, readOption, deltaArray, valueOffset](size_t readSize) mutable {
            valueOffset += readSize;
            size_t slotSize = (slotIdx == ((_itemCount - 1) >> _slotBitNum)) ? ((_itemCount - 1) & _slotMask) + 1
                                                                             : GetSlotItemCount();
            size_t deltaArraySize = GetDeltaArraySizeBySlotType(slotItem->slotType, slotSize);
            return fileStream->ReadAsync(deltaArray, deltaArraySize, valueOffset, readOption);
        });
}

template <typename T>
inline std::pair<Status, size_t> EquivalentCompressReader<T>::GetLongValueDeltaArray(
    const std::shared_ptr<indexlib::file_system::FileStream>& fileStream, size_t slotIdx, LongSlotItem& slotItem,
    LongValueArrayHeader& header, uint8_t* deltaArray)
{
    size_t valueOffset = _valueBaseOffset + slotItem.value;
    auto [status, curLen] =
        fileStream
            ->Read(&header, sizeof(LongValueArrayHeader), valueOffset, indexlib::file_system::ReadOption::LowLatency())
            .StatusWith();
    RETURN2_IF_STATUS_ERROR(status, 0, "read header fail from FileStream");
    valueOffset += curLen;
    size_t slotSize =
        (slotIdx == ((_itemCount - 1) >> _slotBitNum)) ? ((_itemCount - 1) & _slotMask) + 1 : GetSlotItemCount();
    size_t deltaArraySize = EquivalentCompressFileFormat::GetDeltaArraySizeByDeltaType(header.deltaType, slotSize);
    std::tie(status, curLen) =
        fileStream->Read(deltaArray, deltaArraySize, valueOffset, indexlib::file_system::ReadOption::LowLatency())
            .StatusWith();
    RETURN2_IF_STATUS_ERROR(status, 0, "read deltaArray fail from FileStream");
    return std::make_pair(Status::OK(), curLen);
}

template <typename T>
inline future_lite::Future<size_t> EquivalentCompressReader<T>::AsyncGetLongValueDeltaArray(
    const std::shared_ptr<indexlib::file_system::FileStream>& fileStream, size_t slotIdx, LongSlotItem* slotItem,
    LongValueArrayHeader* header, uint8_t* deltaArray, indexlib::file_system::ReadOption readOption)
{
    size_t valueOffset = _valueBaseOffset + slotItem->value;
    return fileStream->ReadAsync(header, sizeof(LongValueArrayHeader), valueOffset, readOption)
        .thenValue([this, fileStream, slotIdx, header, deltaArray, readOption, valueOffset](size_t headerSize) mutable {
            valueOffset += headerSize;
            size_t slotSize = (slotIdx == ((_itemCount - 1) >> _slotBitNum)) ? ((_itemCount - 1) & _slotMask) + 1
                                                                             : GetSlotItemCount();
            size_t deltaArraySize =
                EquivalentCompressFileFormat::GetDeltaArraySizeByDeltaType(header->deltaType, slotSize);
            return fileStream->ReadAsync(deltaArray, deltaArraySize, valueOffset, readOption)
                .thenValue(
                    [headerSize](size_t size) { return future_lite::makeReadyFuture(size_t(size + headerSize)); });
        });
}

#define UpdateBitValue(itemCountMask, itemCountPowerNum, bitCountPowerNum, bitMask, deltaArray)                        \
    uint32_t alignSize = 8;                                                                                            \
    uint32_t byteIndex = valueIdx >> itemCountPowerNum;                                                                \
    uint32_t posInByte = valueIdx & itemCountMask;                                                                     \
    uint32_t bitMoveCount = alignSize - ((posInByte + 1) << bitCountPowerNum);                                         \
    uint8_t readMask = ~(bitMask << bitMoveCount);                                                                     \
    uint8_t writeMask = (updateDelta & bitMask) << bitMoveCount;                                                       \
    uint8_t byteValue = (deltaArray[byteIndex] & readMask) | writeMask;                                                \
    deltaArray[byteIndex] = byteValue;

template <typename T>
inline void EquivalentCompressReader<T>::InplaceUpdateDeltaArray(uint8_t* deltaArray, SlotItemType slotType,
                                                                 uint32_t valueIdx, UT updateDelta)
{
    switch (slotType) {
    case SIT_DELTA_UINT8: // uint8_t
    {
        deltaArray[valueIdx] = (uint8_t)updateDelta;
        break;
    }
    case SIT_DELTA_UINT16: // uint16_t
    {
        ((uint16_t*)deltaArray)[valueIdx] = (uint16_t)updateDelta;
        break;
    }
    case SIT_DELTA_UINT32: // uint32_t
    {
        ((uint32_t*)deltaArray)[valueIdx] = (uint32_t)updateDelta;
        break;
    }
    case SIT_DELTA_UINT64: // uint64_t
    {
        ((uint64_t*)deltaArray)[valueIdx] = (uint64_t)updateDelta;
        break;
    }

    case SIT_DELTA_BIT1: {
        UpdateBitValue(7, 3, 0, 1, deltaArray);
        break;
    }
    case SIT_DELTA_BIT2: {
        UpdateBitValue(3, 2, 1, 3, deltaArray);
        break;
    }
    case SIT_DELTA_BIT4: {
        UpdateBitValue(1, 1, 2, 15, deltaArray);
        break;
    }
    default:
        assert(false);
    }
}

#undef DecodeBitValueInOneByte
#define DecodeBitValueInOneByte(posInByte, bitCountPowerNum, bitMask, byteValue)                                       \
    uint32_t alignSize = 8;                                                                                            \
    uint32_t bitMoveCount = alignSize - ((posInByte + 1) << bitCountPowerNum);                                         \
    byteValue = byteValue >> bitMoveCount;                                                                             \
    byteValue &= bitMask;                                                                                              \
    return std::make_pair(Status::OK(), (UT)byteValue);

template <typename T>
inline std::pair<Status, typename EquivalentCompressReader<T>::UT>
EquivalentCompressReader<T>::ReadDeltaValueBySlotItem(SlotItemType slotType, size_t valueOffset,
                                                      uint32_t valueIdx) const
{
    switch (slotType) {
    case SIT_DELTA_UINT8: // uint8_t
    {
        valueOffset += sizeof(uint8_t) * valueIdx;
        uint8_t typedValue;
        auto [status, _] =
            _fileStream
                ->Read(&typedValue, sizeof(uint8_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read value from FileStream fail");
        return std::make_pair(Status::OK(), (UT)typedValue);
    }
    case SIT_DELTA_UINT16: // uint16_t
    {
        uint16_t typedValue;
        valueOffset += sizeof(uint16_t) * valueIdx;
        auto [status, _] =
            _fileStream
                ->Read(&typedValue, sizeof(uint16_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read value from FileStream fail");
        return std::make_pair(Status::OK(), (UT)typedValue);
    }
    case SIT_DELTA_UINT32: // uint32_t
    {
        uint32_t typedValue;
        valueOffset += sizeof(uint32_t) * valueIdx;
        auto [status, _] =
            _fileStream
                ->Read(&typedValue, sizeof(uint32_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read value from FileStream fail");
        return std::make_pair(Status::OK(), (UT)typedValue);
    }
    case SIT_DELTA_UINT64: // uint64_t
    {
        uint64_t typedValue;
        valueOffset += sizeof(uint64_t) * valueIdx;
        auto [status, _] =
            _fileStream
                ->Read(&typedValue, sizeof(uint64_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read value from FileStream fail");
        return std::make_pair(Status::OK(), (UT)typedValue);
    }
    case SIT_DELTA_BIT1: {
        valueOffset += (valueIdx >> 3);
        uint8_t byteValue;
        auto [status, _] =
            _fileStream->Read(&byteValue, sizeof(uint8_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read value from FileStream fail");
        DecodeBitValueInOneByte((valueIdx & 7), 0, 1, byteValue);
    }
    case SIT_DELTA_BIT2: {
        valueOffset += (valueIdx >> 2);
        uint8_t byteValue;
        auto [status, _] =
            _fileStream->Read(&byteValue, sizeof(uint8_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read value from FileStream fail");
        DecodeBitValueInOneByte((valueIdx & 3), 1, 3, byteValue);
    }
    case SIT_DELTA_BIT4: {
        valueOffset += (valueIdx >> 1);
        uint8_t byteValue;
        auto [status, _] =
            _fileStream->Read(&byteValue, sizeof(uint8_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read value from FileStream fail");
        DecodeBitValueInOneByte((valueIdx & 1), 2, 15, byteValue);
    }
    default:
        assert(false);
    }
    return std::make_pair(Status::OK(), 0);
}

template <typename T>
size_t EquivalentCompressReader<T>::GetDeltaArraySizeBySlotType(SlotItemType slotType, size_t count) const
{
    switch (slotType) {
    case SIT_DELTA_UINT8: // uint8_t
    {
        return sizeof(uint8_t) * count;
    }
    case SIT_DELTA_UINT16: // uint16_t
    {
        return sizeof(uint16_t) * count;
    }
    case SIT_DELTA_UINT32: // uint32_t
    {
        return sizeof(uint32_t) * count;
    }
    case SIT_DELTA_UINT64: // uint64_t
    {
        return sizeof(uint64_t) * count;
    }
    case SIT_DELTA_BIT1: {
        return (count + 7) >> 3;
    }
    case SIT_DELTA_BIT2: {
        return ((count << 1) + 7) >> 3;
    }
    case SIT_DELTA_BIT4: {
        return ((count << 2) + 7) >> 3;
    }
    default:
        assert(false);
    }
    return 0;
}

template <typename T>
inline std::pair<Status, typename EquivalentCompressReader<T>::UT>
EquivalentCompressReader<T>::ReadDeltaValueByDeltaType(uint64_t deltaType, uint64_t valueOffset, size_t valueIdx) const
{
    switch (deltaType) {
    case 0: // uint8_t
    {
        valueOffset += sizeof(uint8_t) * valueIdx;
        uint8_t typedValue;
        auto [status, _] =
            _fileStream
                ->Read(&typedValue, sizeof(uint8_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read typed value for uint8 fail");
        return std::make_pair(Status::OK(), (UT)typedValue);
    }
    case 1: // uint16_t
    {
        valueOffset += sizeof(uint16_t) * valueIdx;
        uint16_t typedValue;
        auto [status, _] =
            _fileStream
                ->Read(&typedValue, sizeof(uint16_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read typed value for uint8 fail");
        return std::make_pair(Status::OK(), (UT)typedValue);
    }
    case 2: // uint32_t
    {
        valueOffset += sizeof(uint32_t) * valueIdx;
        uint32_t typedValue;
        auto [status, _] =
            _fileStream
                ->Read(&typedValue, sizeof(uint32_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read typed value for uint8 fail");
        return std::make_pair(Status::OK(), (UT)typedValue);
    }
    case 3: // uint64_t
    {
        valueOffset += sizeof(uint64_t) * valueIdx;
        uint64_t typedValue;
        auto [status, _] =
            _fileStream
                ->Read(&typedValue, sizeof(uint64_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read typed value for uint8 fail");
        return std::make_pair(Status::OK(), (UT)typedValue);
    }
    case 4: // 1 bit
    {
        valueOffset += (valueIdx >> 3);
        uint8_t byteValue;
        auto [status, _] =
            _fileStream->Read(&byteValue, sizeof(uint8_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read typed value for uint8 fail");
        DecodeBitValueInOneByte((valueIdx & 7), 0, 1, byteValue);
    }
    case 5: // 2 bits
    {
        valueOffset += (valueIdx >> 2);
        uint8_t byteValue;
        auto [status, _] =
            _fileStream->Read(&byteValue, sizeof(uint8_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read typed value for uint8 fail");
        DecodeBitValueInOneByte((valueIdx & 3), 1, 3, byteValue);
    }
    case 6: // 4 bits
    {
        valueOffset += (valueIdx >> 1);
        uint8_t byteValue;
        auto [status, _] =
            _fileStream->Read(&byteValue, sizeof(uint8_t), valueOffset, indexlib::file_system::ReadOption::LowLatency())
                .StatusWith();
        RETURN2_IF_STATUS_ERROR(status, 0, "read typed value for uint8 fail");
        DecodeBitValueInOneByte((valueIdx & 1), 2, 15, byteValue);
    }
    default:
        assert(false);
    }
    return std::make_pair(Status::OK(), 0);
}

template <typename T>
inline bool EquivalentCompressReader<T>::ReachMaxDeltaType(SlotItemType slotType)
{
    size_t slotTypeSize = 0;
    switch (slotType) {
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
inline void EquivalentCompressReader<T>::GenerateExpandDeltaBuffer(UT equalValue, UT updateEncodeValue, size_t pos,
                                                                   uint8_t* deltaBuffer, UT& expandBaseValue,
                                                                   SlotItemType& expandSlotType)
{
    uint32_t updateValueIdx = pos & _slotMask;
    expandBaseValue = std::min(equalValue, updateEncodeValue);
    UT maxDeltaValue = std::max(equalValue, updateEncodeValue) - expandBaseValue;
    expandSlotType = GetSlotItemType(maxDeltaValue);
    if (ReachMaxDeltaType(expandSlotType)) {
        expandBaseValue = 0;
    }

    uint32_t slotItemCount = 1 << _slotBitNum;
    for (uint32_t i = 0; i < slotItemCount; i++) {
        UT deltaValue = (i == updateValueIdx) ? updateEncodeValue - expandBaseValue : equalValue - expandBaseValue;
        InplaceUpdateDeltaArray(deltaBuffer, expandSlotType, i, deltaValue);
    }
}

template <typename T>
inline void EquivalentCompressReader<T>::GenerateExpandDeltaBuffer(UT curBaseValue, SlotItemType curSlotType,
                                                                   uint8_t* curDeltaAddress, UT updateEncodeValue,
                                                                   size_t pos, UT* deltaBuffer, UT& expandBaseValue,
                                                                   SlotItemType& expandSlotType)
{
    uint32_t valueIdx = pos & _slotMask;
    uint32_t slotItemCount = (uint32_t)1 << _slotBitNum;
    uint32_t endIdx = slotItemCount;
    size_t beginPos = (pos >> _slotBitNum) << _slotBitNum;
    if (_itemCount < beginPos + slotItemCount) {
        endIdx = _itemCount - beginPos;
    }

    UT maxDelta = 0;
    for (uint32_t i = 0; i < endIdx; ++i) {
        deltaBuffer[i] = EquivalentCompressFileFormat::GetDeltaValueBySlotItemType<UT>(curSlotType, curDeltaAddress, i);
        if (deltaBuffer[i] > maxDelta) {
            maxDelta = deltaBuffer[i];
        }
    }

    expandBaseValue = curBaseValue;
    UT expandDelta = 0;
    if (updateEncodeValue < expandBaseValue) {
        expandDelta = expandBaseValue - updateEncodeValue;
        maxDelta += expandDelta;
        expandBaseValue = updateEncodeValue;
    } else {
        maxDelta = updateEncodeValue - expandBaseValue;
    }

    expandSlotType = GetSlotItemType(maxDelta);
    if (ReachMaxDeltaType(expandSlotType)) {
        expandDelta += (expandBaseValue - 0);
        expandBaseValue = 0;
    }

    for (uint32_t i = 0; i < slotItemCount; ++i) {
        UT deltaValue = (i >= endIdx) ? 0 : deltaBuffer[i] + expandDelta;
        if (i == valueIdx) {
            deltaValue = updateEncodeValue - expandBaseValue;
        }
        InplaceUpdateDeltaArray((uint8_t*)deltaBuffer, expandSlotType, i, deltaValue);
    }
}

template <typename T>
inline size_t EquivalentCompressReader<T>::GetExpandFreeMemoryUse(SlotItemType curSlotType, uint64_t slotValue,
                                                                  size_t pos)
{
    if (curSlotType == SIT_EQUAL) {
        return 0;
    }

    uint32_t slotItemCount = (uint32_t)1 << _slotBitNum;
    uint32_t itemCount = slotItemCount;
    if (slotValue < _compressLen) {
        uint32_t endIdx = slotItemCount;
        size_t beginPos = (pos >> _slotBitNum) << _slotBitNum;
        if (_itemCount < beginPos + slotItemCount) {
            endIdx = _itemCount - beginPos;
        }
        itemCount = endIdx;
    }

    size_t headerSize = (sizeof(UT) == sizeof(uint64_t)) ? (sizeof(UT) * 2) : sizeof(UT);
    return headerSize + GetCompressDeltaBufferSize(curSlotType, itemCount);
}
} // namespace indexlib::index
