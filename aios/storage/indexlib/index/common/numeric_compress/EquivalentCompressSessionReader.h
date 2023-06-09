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

#include "autil/Log.h"
#include "future_lite/Future.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/file_system/stream/FileStreamCreator.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressReader.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {

template <typename T>
class EquivalentCompressSessionReader
{
public:
    EquivalentCompressSessionReader();

    EquivalentCompressSessionReader(EquivalentCompressReader<T>* compressReader,
                                    const EquivalentCompressSessionOption& sessionOption,
                                    autil::mem_pool::Pool* sessionPool);
    EquivalentCompressSessionReader(EquivalentCompressSessionReader&& other);
    ~EquivalentCompressSessionReader();
    EquivalentCompressSessionReader& operator=(EquivalentCompressSessionReader&& other);
    EquivalentCompressSessionReader(const EquivalentCompressSessionReader&) = delete;
    EquivalentCompressSessionReader& operator=(const EquivalentCompressSessionReader&) = delete;

public:
    inline std::pair<Status, T> Get(size_t pos) __ALWAYS_INLINE { return (*this)[pos]; }
    std::pair<Status, T> operator[](size_t rowId) __ALWAYS_INLINE;

    future_lite::Future<size_t> ReadAsync(const std::vector<int32_t>& indiceVec, size_t beginIndice,
                                          std::vector<T>& resultBuffer,
                                          indexlib::file_system::ReadOption readOption) __ALWAYS_INLINE;

    // batch pos should be non-decreasing
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> BatchGet(const std::vector<int32_t>& batchPos,
                                                                    indexlib::file_system::ReadOption readOption,
                                                                    std::vector<T>* values) noexcept;

private:
    using ZigZagEncoder = typename EquivalentCompressTraits<T>::ZigZagEncoder;
    using UT = typename EquivalentCompressTraits<T>::EncoderType;
    /*    T: uint8_t  / int8_t   UT: uint8_t;    */
    /*    T: uint16_t / int16_t  UT: uint16_t;   */
    /*    T: uint32_t / int32_t / float   UT: uint32_t;   */
    /*    T: uint64_t / int64_t / double  UT: uint64_t;   */

    using SlotType = typename SlotItemTraits<T>::SlotArrayValueType;

    template <typename BaseValueType>
    struct DecodedState {
        int32_t slotId = -1;
        BaseValueType baseValue = 0;
        uint64_t longValueDeltaType = 0;
        uint8_t* deltaArray = nullptr;
        void Reset()
        {
            slotId = -1;
            baseValue = 0;
            longValueDeltaType = 0;
            deltaArray = nullptr;
        }
    };

    // [begin end)  : offset range
    // [beginSlotId, endSlotId]: slotId range
    struct ReadBatchInfo {
        size_t begin = 0;
        size_t end = 0;
        int32_t beginSlotId = -1;
        int32_t lastSlotId = -1;

        void Reset()
        {
            begin = 0;
            end = 0;
            beginSlotId = -1;
            lastSlotId = -1;
        }
    };

private:
    // precondition: 1. indiceVec[beginIndice] != -1 && beginIndice <= endIndice && indiceVec[endIndice] != -1
    //            or 2. beginIndice >= indiceVec.size()
    future_lite::Future<size_t> BatchReadDeltaArray(const std::vector<int32_t>& indiceVec, size_t beginIndice,
                                                    size_t endIndice, size_t depth, std::vector<T>& resultBuffer,
                                                    indexlib::file_system::ReadOption readOption) __ALWAYS_INLINE;

    indexlib::file_system::BatchIO GetSlotBatchIO(const std::vector<int32_t>& indiceVec, size_t beginIndice);

    indexlib::file_system::BatchIO
    GetArrayBatchIO(const std::vector<int32_t>& indiceVec, size_t beginIndice,
                    const indexlib::file_system::BatchIO& slotBatchIO,
                    const std::vector<indexlib::file_system::FSResult<size_t>>& slotResult);

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    BatchReadDeltaArray(const std::vector<int32_t>& indiceVec, size_t beginIndice,
                        indexlib::file_system::ReadOption readOption, std::vector<T>* values) noexcept;

    size_t GetDeltaArraySize(SlotType* item, size_t slotSize);

    T GetValueFromDeltaArray(uint8_t* slotItem, uint8_t* deltaArray, size_t inArrayIdx);

    T GetNormalValueFromDeltaArray(uint8_t* slotItem, uint8_t* deltaArray, size_t inArrayIdx);

    T GetLongValueFromDeltaArray(uint8_t* slotItem, uint8_t* deltaArray, size_t inArrayIdx);

    // prepare slotItems from range [beginPos, endPos]
    inline future_lite::Future<std::pair<Status, int32_t>>
    PrepareSlotItems(int32_t beginPos, int32_t endPos, indexlib::file_system::ReadOption readOption) __ALWAYS_INLINE;
    inline bool AppendToCurrentReadBatch(int32_t pos) __ALWAYS_INLINE;
    inline future_lite::Future<size_t>
    FetchCurrentReadBatch(indexlib::file_system::ReadOption readOption) __ALWAYS_INLINE;

    T GetValueFromBuffer(size_t rowId) __ALWAYS_INLINE;
    T GetValueFromBuffer(SlotType* slotItemBuffer, uint8_t* deltaArrayBuffer, size_t beginSlotId, size_t bufferOffset,
                         size_t rowId) __ALWAYS_INLINE;
    T GetNormalValueFromBuffer(size_t rowId) __ALWAYS_INLINE;
    T GetLongValueFromBuffer(size_t rowId) __ALWAYS_INLINE;
    size_t EstimateLongDeltaArraySize(size_t slotSize);
    void FillSlotBatchIOBuffer(indexlib::file_system::BatchIO& batchIO);
    void FillArrayBatchIOBuffer(indexlib::file_system::BatchIO& batchIO);
    bool TryFillFromFirstBuffer(int32_t rowId, T& value);
    int32_t GetSlotId(size_t rowId) const __ALWAYS_INLINE;
    int64_t GetBeginRowIdInSlot(int32_t slotId) const __ALWAYS_INLINE;
    int32_t GetSlotSize(int32_t slotId) const __ALWAYS_INLINE;
    uint32_t GetSlotItemCount() const __ALWAYS_INLINE;
    bool isEqualValueSlot(int32_t slotId) const __ALWAYS_INLINE;
    void GetDeltaArrayRange(int32_t slotId, std::pair<size_t, size_t>* slotRange) __ALWAYS_INLINE;
    void GetNormalDeltaArrayRange(int32_t slotId, std::pair<size_t, size_t>* slotRange) __ALWAYS_INLINE;

    // precondition: _slotItemBuffer[slotId - _beginSlotId] is not equal value slot
    void GetLongDeltaArrayRange(int32_t slotId, std::pair<size_t, size_t>* slotRange) __ALWAYS_INLINE;
    SlotType* GetSlotBuffer() __ALWAYS_INLINE
    {
        if (!_slotItemBuffer) {
            using SlotType = EquivalentCompressSessionReader<T>::SlotType;
            _slotItemBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, SlotType, _bufferedSlotCount);
        }
        return _slotItemBuffer;
    }
    uint8_t* GetDeltaArrayBuffer() __ALWAYS_INLINE
    {
        if (!_deltaArrayBuffer) {
            _deltaArrayBuffer = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, uint8_t, _sessionOption.valueBufferSize);
        }
        return _deltaArrayBuffer;
    }

private:
    // BufferedSlotItemRange: [_beginSlotId, _endSlotId)
    EquivalentCompressReader<T>* _compressReader;
    int32_t _beginSlotId;
    int32_t _endSlotId;
    size_t _ioBufferOffset;
    size_t _bufferedSlotCount;
    EquivalentCompressFileContent _fileContent;
    std::shared_ptr<indexlib::file_system::FileStream> _fileStream;
    std::shared_ptr<indexlib::file_system::FileStream> _slotFileStream;
    autil::mem_pool::Pool* _pool;
    EquivalentCompressSessionOption _sessionOption;

    // lastRead rowRange: [_lastReadRange.first, _lastReadRange.second)
    std::pair<int64_t, int64_t> _lastReadRange;
    ReadBatchInfo _currentBatch;
    DecodedState<UT> _decoded;
    SlotType* _slotItemBuffer;
    uint8_t* _deltaArrayBuffer;

    // for random batch read
    std::vector<SlotType*> _slotItemAllocatedBuffer;
    std::vector<uint8_t*> _deltaArrayAllocatedBuffer;
    int32_t _firstBufferSlotId;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, EquivalentCompressSessionReader, T);

template <typename T>
EquivalentCompressSessionReader<T>::EquivalentCompressSessionReader()
    : _compressReader(nullptr)
    , _beginSlotId(-1)
    , _endSlotId(-1)
    , _ioBufferOffset(0)
    , _bufferedSlotCount(0)
    , _fileStream(nullptr)
    , _pool(nullptr)
    , _lastReadRange({-1, -1})
    , _slotItemBuffer(nullptr)
    , _deltaArrayBuffer(nullptr)
    , _firstBufferSlotId(-1)
{
}

template <typename T>
EquivalentCompressSessionReader<T>::EquivalentCompressSessionReader(
    EquivalentCompressReader<T>* compressReader, const EquivalentCompressSessionOption& sessionOption,
    autil::mem_pool::Pool* sessionPool)
    : _compressReader(compressReader)
    , _beginSlotId(-1)
    , _endSlotId(-1)
    , _ioBufferOffset(0)
    , _bufferedSlotCount(0)
    , _pool(sessionPool)
    , _sessionOption(sessionOption)
    , _lastReadRange({-1, -1})
    , _slotItemBuffer(nullptr)
    , _deltaArrayBuffer(nullptr)
    , _firstBufferSlotId(-1)
{
    if (_compressReader && !_compressReader->IsReadFromMemory()) {
        _fileContent = compressReader->GetFileContent();
        auto fileReader = _compressReader->GetFileReader();
        _fileStream = indexlib::file_system::FileStreamCreator::CreateFileStream(fileReader, sessionPool);
        _slotFileStream = indexlib::file_system::FileStreamCreator::CreateFileStream(fileReader, sessionPool);
        using SlotType = EquivalentCompressSessionReader<T>::SlotType;
        _bufferedSlotCount = _sessionOption.slotBufferSize / sizeof(SlotType);

        auto reservedHeaderLength = sizeof(LongValueArrayHeader);
        _sessionOption.valueBufferSize =
            std::max(_sessionOption.valueBufferSize, reservedHeaderLength + GetSlotItemCount() * sizeof(T));
    }
}

template <typename T>
EquivalentCompressSessionReader<T>::EquivalentCompressSessionReader(EquivalentCompressSessionReader&& other)
    : _compressReader(other._compressReader)
    , _beginSlotId(other._beginSlotId)
    , _endSlotId(other._endSlotId)
    , _ioBufferOffset(other._ioBufferOffset)
    , _bufferedSlotCount(other._bufferedSlotCount)
    , _fileContent(other._fileContent)
    , _fileStream(other._fileStream)
    , _slotFileStream(other._slotFileStream)
    , _pool(std::exchange(other._pool, nullptr))
    , _sessionOption(other._sessionOption)
    , _lastReadRange(other._lastReadRange)
    , _currentBatch(other._currentBatch)
    , _decoded(other._decoded)
    , _slotItemBuffer(std::exchange(other._slotItemBuffer, nullptr))
    , _deltaArrayBuffer(std::exchange(other._deltaArrayBuffer, nullptr))
    , _slotItemAllocatedBuffer(std::move(other._slotItemAllocatedBuffer))
    , _deltaArrayAllocatedBuffer(std::move(other._deltaArrayAllocatedBuffer))
    , _firstBufferSlotId(std::exchange(other._firstBufferSlotId, _firstBufferSlotId))
{
}

template <typename T>
EquivalentCompressSessionReader<T>::~EquivalentCompressSessionReader()
{
    IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _slotItemBuffer, _bufferedSlotCount);
    IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _deltaArrayBuffer, _sessionOption.valueBufferSize);

    for (size_t i = 0; i < _slotItemAllocatedBuffer.size(); ++i) {
        indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _slotItemAllocatedBuffer[i]);
    }
    _slotItemAllocatedBuffer.clear();

    size_t bufferLen = sizeof(LongValueArrayHeader) + GetSlotItemCount() * sizeof(T);
    for (size_t i = 0; i < _deltaArrayAllocatedBuffer.size(); ++i) {
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, _deltaArrayAllocatedBuffer[i], bufferLen);
    }
    _deltaArrayAllocatedBuffer.clear();
}
template <typename T>
EquivalentCompressSessionReader<T>&
EquivalentCompressSessionReader<T>::operator=(EquivalentCompressSessionReader&& other)
{
    if (this != &other) {
        _compressReader = other._compressReader;
        _beginSlotId = other._beginSlotId;
        _endSlotId = other._endSlotId;
        _ioBufferOffset = other._ioBufferOffset;
        _bufferedSlotCount = other._bufferedSlotCount;
        _fileContent = other._fileContent;
        _fileStream = other._fileStream;
        _slotFileStream = other._slotFileStream;
        _pool = std::exchange(other._pool, nullptr);
        _sessionOption = other._sessionOption;
        _lastReadRange = other._lastReadRange;
        _currentBatch = other._currentBatch;
        _decoded = other._decoded;
        _slotItemBuffer = std::exchange(other._slotItemBuffer, nullptr);
        _deltaArrayBuffer = std::exchange(other._deltaArrayBuffer, nullptr);
        _slotItemAllocatedBuffer.swap(other._slotItemAllocatedBuffer);
        _deltaArrayAllocatedBuffer.swap(other._deltaArrayAllocatedBuffer);
        _firstBufferSlotId = std::exchange(other._firstBufferSlotId, _firstBufferSlotId);
    }
    return *this;
}

template <typename T>
inline future_lite::Future<std::pair<Status, int32_t>>
EquivalentCompressSessionReader<T>::PrepareSlotItems(int32_t beginPos, int32_t endPos,
                                                     indexlib::file_system::ReadOption readOption)
{
    auto beginSlotId = GetSlotId(beginPos);
    auto endSlotId = GetSlotId(endPos);

    if (endPos >= _fileContent.itemCount || beginPos >= _fileContent.itemCount) {
        AUTIL_LOG(ERROR, "Bad read range[%d, %d] for file with itemCount[%u]", beginPos, endPos,
                  _fileContent.itemCount);
        return future_lite::makeReadyFuture(std::make_pair(Status::OK(), /*invalid value*/ -1));
    }
    if (beginSlotId >= _beginSlotId && beginSlotId < _endSlotId) {
        return future_lite::makeReadyFuture(std::make_pair(Status::OK(), int32_t(_endSlotId)));
    }
    using SlotType = EquivalentCompressSessionReader<T>::SlotType;

    _beginSlotId = GetSlotId(beginPos);

    int32_t bufferEndSlotId = std::min(endSlotId, _beginSlotId + (int32_t)_bufferedSlotCount - 1);
    size_t bufferedSlotItemCount = bufferEndSlotId - _beginSlotId + 1;
    // _endSlotId is not availble
    _endSlotId = bufferEndSlotId + 1;

    return _slotFileStream
        ->ReadAsync(GetSlotBuffer(), sizeof(SlotType) * bufferedSlotItemCount,
                    _fileContent.slotBaseOffset + _beginSlotId * sizeof(SlotType), readOption)
        .thenValue([this](size_t) mutable {
            _lastReadRange = {-1, -1};
            return future_lite::makeReadyFuture(std::make_pair(Status::OK(), int32_t(_endSlotId)));
        });
}
template <typename T>
inline std::pair<Status, T> EquivalentCompressSessionReader<T>::operator[](size_t rowId)
{
    if (_compressReader->IsReadFromMemory()) {
        return _compressReader->Get(rowId);
    }

    assert(rowId < _fileContent.itemCount);
    if (rowId >= _lastReadRange.first && rowId < _lastReadRange.second) {
        auto t = GetValueFromBuffer(rowId);
        return std::make_pair(Status::OK(), t);
    }
    auto readOption = indexlib::file_system::ReadOption::LowLatency();
    auto [st, _] = PrepareSlotItems(rowId, rowId, readOption).get();
    RETURN2_IF_STATUS_ERROR(st, T(), "prepare slot item failed.");

    if (!AppendToCurrentReadBatch(rowId)) {
        AUTIL_LOG(ERROR, "Bad read rowId [%zu] for file with itemCount[%u]", rowId, _fileContent.itemCount);
        return std::make_pair(Status::Corruption("append to current batch failed"), T());
    }

    size_t readSize = FetchCurrentReadBatch(readOption).get();
    if (readSize < 0) {
        AUTIL_LOG(ERROR, "Bad read rowId [%zu] for file with itemCount[%u]", rowId, _fileContent.itemCount);
        return std::make_pair(Status::Corruption("fetch current batch failed"), T());
    }
    auto t = GetValueFromBuffer(rowId);
    return std::make_pair(Status::OK(), t);
}

template <typename T>
inline future_lite::Future<size_t>
EquivalentCompressSessionReader<T>::ReadAsync(const std::vector<int32_t>& indiceVec, size_t beginIndice,
                                              std::vector<T>& resultBuffer,
                                              indexlib::file_system::ReadOption readOption)
{
    Status status;
    if (_compressReader->IsReadFromMemory()) {
        for (size_t i = beginIndice; i < indiceVec.size(); i++) {
            if (indiceVec[i] != -1) {
                std::tie(status, resultBuffer[i]) = _compressReader->Get(indiceVec[i]);
                indexlib::util::ThrowIfStatusError(status);
            }
        }
        return future_lite::makeReadyFuture(indiceVec.size());
    }

    size_t beginIdx = beginIndice;
    while (beginIdx < indiceVec.size() && (indiceVec[beginIdx] == -1)) {
        beginIdx++;
    }

    if (beginIdx >= indiceVec.size()) {
        return future_lite::makeReadyFuture(indiceVec.size());
    }

    size_t endIdx = indiceVec.size() - 1;
    while (endIdx > beginIdx && (indiceVec[endIdx] == -1)) {
        endIdx--;
    }

    int32_t rowId = -1;
    size_t idx = beginIdx;
    for (; idx <= endIdx; idx++) {
        rowId = indiceVec[idx];
        if (rowId == -1) {
            continue;
        }
        if (rowId >= _lastReadRange.first && rowId < _lastReadRange.second) {
            resultBuffer[idx] = GetValueFromBuffer(rowId);
            continue;
        }
        break;
    }
    beginIdx = idx;
    if (beginIdx > endIdx) {
        return future_lite::makeReadyFuture(size_t(indiceVec.size()));
    }
    return BatchReadDeltaArray(indiceVec, beginIdx, endIdx, 0, resultBuffer, readOption);
}

template <typename T>
inline bool EquivalentCompressSessionReader<T>::AppendToCurrentReadBatch(int32_t pos)
{
    auto slotId = GetSlotId(pos);

    if (slotId >= _endSlotId) {
        return false;
    }

    if (slotId == _currentBatch.lastSlotId) {
        return true;
    }

    if (isEqualValueSlot(slotId)) {
        if (_currentBatch.lastSlotId == -1) {
            _currentBatch.beginSlotId = slotId;
        }
        _currentBatch.lastSlotId = slotId;
        return true;
    }

    // for not-equal slot
    std::pair<size_t, size_t> slotRange;
    GetDeltaArrayRange(slotId, &slotRange);

    if (_currentBatch.lastSlotId == -1) {
        _currentBatch.begin = slotRange.first;
        _currentBatch.end = slotRange.second;
        _currentBatch.beginSlotId = slotId;
        _currentBatch.lastSlotId = slotId;
        return true;
    }

    if (slotRange.first - _currentBatch.end < _sessionOption.ioGapSize &&
        (slotRange.second - _currentBatch.begin) <= _sessionOption.valueBufferSize) {
        _currentBatch.end = slotRange.second;
        _currentBatch.lastSlotId = slotId;
        return true;
    }
    return false;
}

template <typename T>
inline future_lite::Future<size_t>
EquivalentCompressSessionReader<T>::BatchReadDeltaArray(const std::vector<int32_t>& indiceVec, size_t beginIndice,
                                                        size_t endIndice, size_t depth, std::vector<T>& resultBuffer,
                                                        indexlib::file_system::ReadOption readOption)
{
    if (beginIndice >= indiceVec.size()) {
        return future_lite::makeReadyFuture(size_t(indiceVec.size()));
    }
    if (depth > _sessionOption.maxRecursionDepth) {
        return future_lite::makeReadyFuture(size_t(beginIndice));
    }
    return PrepareSlotItems(indiceVec[beginIndice], indiceVec[endIndice], readOption)
        .thenValue([this, &indiceVec, beginIndice, endIndice, depth, &resultBuffer,
                    readOption](std::pair<Status, int32_t> endSlotId) mutable {
            indexlib::util::ThrowIfStatusError(endSlotId.first);
            size_t nextBeginIndice = indiceVec.size();
            for (auto i = beginIndice; i < indiceVec.size(); i++) {
                if (indiceVec[i] == -1) {
                    continue;
                }

                if (!AppendToCurrentReadBatch(indiceVec[i])) {
                    nextBeginIndice = i;
                    break;
                }
            }
            return FetchCurrentReadBatch(readOption)
                .thenValue([this, &indiceVec, beginIndice, nextBeginIndice, endIndice, depth, &resultBuffer,
                            readOption](size_t readSize) mutable {
                    for (auto i = beginIndice; i < nextBeginIndice; i++) {
                        if (indiceVec[i] == -1) {
                            continue;
                        }
                        resultBuffer[i] = GetValueFromBuffer(indiceVec[i]);
                    }
                    return BatchReadDeltaArray(indiceVec, nextBeginIndice, endIndice, depth + 1, resultBuffer,
                                               readOption);
                });
        });
}

template <typename T>
inline indexlib::file_system::BatchIO
EquivalentCompressSessionReader<T>::GetSlotBatchIO(const std::vector<int32_t>& indiceVec, size_t beginIndice)
{
    size_t readNum = indiceVec.size() - beginIndice;
    indexlib::file_system::BatchIO slotBatchIO;
    size_t lastSlotId = GetSlotId(_fileContent.itemCount) + 1; // use invalid slotId to initiate
    slotBatchIO.reserve(readNum);
    for (size_t i = beginIndice; i < indiceVec.size(); ++i) {
        size_t slotId = GetSlotId(indiceVec[i]);
        if (slotId != lastSlotId) {
            lastSlotId = slotId;
            slotBatchIO.emplace_back(nullptr, sizeof(SlotType),
                                     _fileContent.slotBaseOffset + slotId * sizeof(SlotType));
        }
    }
    FillSlotBatchIOBuffer(slotBatchIO);
    return slotBatchIO;
}
template <typename T>
inline indexlib::file_system::BatchIO EquivalentCompressSessionReader<T>::GetArrayBatchIO(
    const std::vector<int32_t>& indiceVec, size_t beginIndice, const indexlib::file_system::BatchIO& slotBatchIO,
    const std::vector<indexlib::file_system::FSResult<size_t>>& slotResult)
{
    int32_t index = -1;
    indexlib::file_system::BatchIO arrayBatchIO;
    size_t lastSlotId = GetSlotId(_fileContent.itemCount) + 1; // use invalid slotId to initiate
    for (size_t i = beginIndice; i < indiceVec.size(); ++i) {
        size_t slotId = GetSlotId(indiceVec[i]);
        if (slotId == lastSlotId) {
            continue;
        }
        ++index;
        lastSlotId = slotId;
        if (!slotResult[index].OK()) {
            continue;
        }

        size_t slotSize = GetSlotSize(slotId);
        SlotType* item = (SlotType*)(slotBatchIO[index].buffer);
        if (item->IsEqual()) {
            continue;
        }
        size_t len = GetDeltaArraySize(item, slotSize);
        size_t offset = _fileContent.valueBaseOffset + item->value;
        len = std::min(len, _fileContent.fileLength - offset);
        arrayBatchIO.emplace_back(nullptr, len, offset);
    }
    FillArrayBatchIOBuffer(arrayBatchIO);
    return arrayBatchIO;
}

template <typename T>
inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
EquivalentCompressSessionReader<T>::BatchReadDeltaArray(const std::vector<int32_t>& indiceVec, size_t beginIndice,
                                                        indexlib::file_system::ReadOption readOption,
                                                        std::vector<T>* values) noexcept
{
    assert(beginIndice < indiceVec.size());
    size_t endIndice = indiceVec.size();
    indexlib::index::ErrorCodeVec result(indiceVec.size() - beginIndice, indexlib::index::ErrorCode::OK);
    auto slotBatchIO = GetSlotBatchIO(indiceVec, beginIndice);

    // read slot info
    auto slotResult = co_await _slotFileStream->BatchRead(slotBatchIO, readOption);
    auto arrayBatchIO = GetArrayBatchIO(indiceVec, beginIndice, slotBatchIO, slotResult);
    auto arrayResult = co_await _fileStream->BatchRead(arrayBatchIO, readOption);

    size_t lastSlotId = GetSlotId(_fileContent.itemCount) + 1; // use invalid slotId to initiate
    int32_t slotIdx = -1, arrayIdx = -1;
    int32_t firstBufferSlotId = -1;
    for (size_t i = beginIndice; i < endIndice; ++i) {
        size_t slotId = GetSlotId(indiceVec[i]);
        if (slotId == lastSlotId) {
            result[i - beginIndice] = result[i - 1 - beginIndice];
        } else {
            lastSlotId = slotId;
            slotIdx++;
            SlotType* item = (SlotType*)(slotBatchIO[slotIdx].buffer);
            if (slotResult[slotIdx].OK()) {
                if (!item->IsEqual()) {
                    arrayIdx++;
                    if (!arrayResult[arrayIdx].OK()) {
                        result[i - beginIndice] = indexlib::index::ConvertFSErrorCode(arrayResult[arrayIdx].ec);
                    }
                }
            } else {
                result[i - beginIndice] = indexlib::index::ConvertFSErrorCode(slotResult[slotIdx].ec);
            }
        }
        if (result[i - beginIndice] == indexlib::index::ErrorCode::OK) {
            uint8_t* arrayBuffer = nullptr;
            if (arrayIdx < arrayBatchIO.size()) {
                arrayBuffer = (uint8_t*)arrayBatchIO[arrayIdx].buffer;
            }
            assert(slotIdx < slotBatchIO.size());
            assert(slotBatchIO[slotIdx].buffer);
            (*values)[i] = GetValueFromDeltaArray((uint8_t*)slotBatchIO[slotIdx].buffer, arrayBuffer,
                                                  indiceVec[i] & _fileContent.slotMask);
            firstBufferSlotId = slotId;
        }
    }
    assert(!result.empty());
    if (result[result.size() - 1] == indexlib::index::ErrorCode::OK) {
        if (slotIdx != -1) {
            std::swap(_slotItemAllocatedBuffer[0], _slotItemAllocatedBuffer[slotIdx]);
        }
        if (arrayIdx != -1) {
            std::swap(_deltaArrayAllocatedBuffer[0], _deltaArrayAllocatedBuffer[arrayIdx]);
        }
    }
    _firstBufferSlotId = firstBufferSlotId;
    co_return result;
}

template <typename T>
inline size_t EquivalentCompressSessionReader<T>::EstimateLongDeltaArraySize(size_t slotSize)
{
    return sizeof(LongValueArrayHeader) +
           EquivalentCompressFileFormat::GetDeltaArraySizeBySlotItemType(SIT_DELTA_UINT64, slotSize);
}

template <typename T>
inline size_t EquivalentCompressSessionReader<T>::GetDeltaArraySize(SlotType* item, size_t slotSize)
{
    if constexpr (std::is_same<T, uint64_t>::value || std::is_same<T, int64_t>::value ||
                  std::is_same<T, double>::value) {
        return EstimateLongDeltaArraySize(slotSize);
    } else {
        return EquivalentCompressFileFormat::GetDeltaArraySizeBySlotItemType(item->slotType, slotSize) +
               sizeof(DeltaValueArrayTyped<UT>);
    }
}

template <typename T>
inline T EquivalentCompressSessionReader<T>::GetValueFromDeltaArray(uint8_t* slotItem, uint8_t* deltaArray,
                                                                    size_t inArrayIdx)
{
    if constexpr (std::is_same<T, uint64_t>::value || std::is_same<T, int64_t>::value ||
                  std::is_same<T, double>::value) {
        return GetLongValueFromDeltaArray(slotItem, deltaArray, inArrayIdx);
    } else {
        return GetNormalValueFromDeltaArray(slotItem, deltaArray, inArrayIdx);
    }
}

template <typename T>
inline T EquivalentCompressSessionReader<T>::GetLongValueFromDeltaArray(uint8_t* slotItem, uint8_t* deltaArray,
                                                                        size_t inArrayIdx)
{
    assert(slotItem);
    LongSlotItem* item = (LongSlotItem*)slotItem;
    if (item->IsEqual()) {
        return ZigZagEncoder::Decode((UT)(item->value));
    }
    assert(deltaArray);
    LongValueArrayHeader* arrayHeader = (LongValueArrayHeader*)deltaArray;
    UT deltaValue = EquivalentCompressFileFormat::GetDeltaValueByDeltaType<UT>(
        arrayHeader->deltaType, deltaArray + sizeof(LongValueArrayHeader), inArrayIdx);
    return ZigZagEncoder::Decode(arrayHeader->baseValue + deltaValue);
}

template <typename T>
inline T EquivalentCompressSessionReader<T>::GetNormalValueFromBuffer(size_t rowId)
{
    typedef DeltaValueArrayTyped<UT> DeltaValueArray;
    int32_t slotId = GetSlotId(rowId);
    size_t slotIdx = slotId - _beginSlotId;
    if (isEqualValueSlot(slotId)) {
        return ZigZagEncoder::Decode((UT)(_slotItemBuffer[slotIdx].value));
    }

    size_t valueIdx = rowId & _fileContent.slotMask;
    if (slotId == _decoded.slotId) {
        UT deltaValue = EquivalentCompressFileFormat::GetDeltaValueBySlotItemType<UT>(
            ((SlotItem*)(_slotItemBuffer) + slotIdx)->slotType, _decoded.deltaArray, valueIdx);
        return ZigZagEncoder::Decode(_decoded.baseValue + deltaValue);
    }

    _decoded.slotId = slotId;
    size_t deltaValueOffset = _slotItemBuffer[slotIdx].value - _ioBufferOffset;
    _decoded.deltaArray = _deltaArrayBuffer + deltaValueOffset + sizeof(DeltaValueArray);
    DeltaValueArray* arrayHeader = (DeltaValueArray*)(_deltaArrayBuffer + deltaValueOffset);
    UT deltaValue = EquivalentCompressFileFormat::GetDeltaValueBySlotItemType<UT>(
        ((SlotItem*)(_slotItemBuffer) + slotIdx)->slotType, _decoded.deltaArray, valueIdx);
    _decoded.baseValue = arrayHeader->baseValue;
    return ZigZagEncoder::Decode(_decoded.baseValue + deltaValue);
}

template <typename T>
inline T EquivalentCompressSessionReader<T>::GetLongValueFromBuffer(size_t rowId)
{
    int32_t slotId = GetSlotId(rowId);
    size_t slotIdx = slotId - _beginSlotId;
    if (isEqualValueSlot(slotId)) {
        return ZigZagEncoder::Decode(_slotItemBuffer[slotIdx].value);
    }

    size_t valueIdx = rowId & _fileContent.slotMask;
    if (slotId == _decoded.slotId) {
        UT deltaValue = EquivalentCompressFileFormat::GetDeltaValueByDeltaType<UT>(_decoded.longValueDeltaType,
                                                                                   _decoded.deltaArray, valueIdx);
        return ZigZagEncoder::Decode(_decoded.baseValue + deltaValue);
    }
    _decoded.slotId = slotId;
    size_t deltaValueOffset = _slotItemBuffer[slotIdx].value - _ioBufferOffset;
    _decoded.deltaArray = _deltaArrayBuffer + deltaValueOffset + sizeof(LongValueArrayHeader);
    LongValueArrayHeader* longValueArrayHeader = (LongValueArrayHeader*)(_deltaArrayBuffer + deltaValueOffset);
    _decoded.baseValue = longValueArrayHeader->baseValue;
    _decoded.longValueDeltaType = longValueArrayHeader->deltaType;
    UT deltaValue = EquivalentCompressFileFormat::GetDeltaValueByDeltaType<UT>(_decoded.longValueDeltaType,
                                                                               _decoded.deltaArray, valueIdx);
    return ZigZagEncoder::Decode(_decoded.baseValue + deltaValue);
}

template <typename T>
inline void EquivalentCompressSessionReader<T>::FillSlotBatchIOBuffer(indexlib::file_system::BatchIO& batchIO)
{
    size_t size = batchIO.size();
    _slotItemAllocatedBuffer.reserve(size);
    while (_slotItemAllocatedBuffer.size() < size) {
        _slotItemAllocatedBuffer.push_back(IE_POOL_COMPATIBLE_NEW_CLASS(_pool, SlotType));
    }
    for (size_t i = 0; i < batchIO.size(); ++i) {
        batchIO[i].buffer = _slotItemAllocatedBuffer[i];
    }
}

template <typename T>
inline void EquivalentCompressSessionReader<T>::FillArrayBatchIOBuffer(indexlib::file_system::BatchIO& batchIO)
{
    size_t size = batchIO.size();
    _deltaArrayAllocatedBuffer.reserve(size);
    size_t bufferLen = sizeof(LongValueArrayHeader) + GetSlotItemCount() * sizeof(T);
    while (_deltaArrayAllocatedBuffer.size() < size) {
        _deltaArrayAllocatedBuffer.push_back(IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, uint8_t, bufferLen));
    }
    for (size_t i = 0; i < batchIO.size(); ++i) {
        batchIO[i].buffer = _deltaArrayAllocatedBuffer[i];
    }
}
template <typename T>
inline bool EquivalentCompressSessionReader<T>::TryFillFromFirstBuffer(int32_t rowId, T& value)
{
    int32_t slotId = GetSlotId(rowId);
    if (slotId != _firstBufferSlotId) {
        return false;
    }
    uint8_t* buffer = nullptr;
    if (!_deltaArrayAllocatedBuffer.empty()) {
        buffer = _deltaArrayAllocatedBuffer[0];
    }
    assert(!_slotItemAllocatedBuffer.empty());
    value = GetValueFromDeltaArray((uint8_t*)(_slotItemAllocatedBuffer[0]), buffer, rowId & _fileContent.slotMask);
    return true;
}

template <typename T>
inline int64_t EquivalentCompressSessionReader<T>::GetBeginRowIdInSlot(int32_t slotId) const
{
    if (slotId < 0) {
        return -1;
    }
    return int64_t(slotId) << _fileContent.slotBitNum;
}

template <typename T>
inline int32_t EquivalentCompressSessionReader<T>::GetSlotSize(int32_t slotId) const
{
    return (slotId == (_fileContent.slotNum - 1) ? (((_fileContent.itemCount - 1) & _fileContent.slotMask) + 1)
                                                 : GetSlotItemCount());
}

template <typename T>
inline uint32_t EquivalentCompressSessionReader<T>::GetSlotItemCount() const
{
    return (uint32_t)1 << _fileContent.slotBitNum;
}

template <typename T>
inline bool EquivalentCompressSessionReader<T>::isEqualValueSlot(int32_t slotId) const
{
    return _slotItemBuffer[slotId - _beginSlotId].IsEqual();
}

template <typename T>
inline void EquivalentCompressSessionReader<T>::GetDeltaArrayRange(int32_t slotId, std::pair<size_t, size_t>* slotRange)
{
    if constexpr (std::is_same<T, uint64_t>::value || std::is_same<T, int64_t>::value ||
                  std::is_same<T, double>::value) {
        GetLongDeltaArrayRange(slotId, slotRange);
    } else {
        GetNormalDeltaArrayRange(slotId, slotRange);
    }
}
template <typename T>
inline int32_t EquivalentCompressSessionReader<T>::GetSlotId(size_t rowId) const
{
    return rowId >> _fileContent.slotBitNum;
}
template <typename T>
inline T EquivalentCompressSessionReader<T>::GetValueFromBuffer(size_t rowId)
{
    if constexpr (std::is_same<T, uint64_t>::value || std::is_same<T, int64_t>::value ||
                  std::is_same<T, double>::value) {
        return GetLongValueFromBuffer(rowId);
    } else {
        return GetNormalValueFromBuffer(rowId);
    }
}
template <typename T>
inline T EquivalentCompressSessionReader<T>::GetValueFromBuffer(SlotType* slotItemBuffer, uint8_t* deltaArrayBuffer,
                                                                size_t beginSlotId, size_t bufferOffset, size_t rowId)
{
    return GetNormalValueFromBuffer(slotItemBuffer, deltaArrayBuffer, beginSlotId, bufferOffset, rowId);
}

template <typename T>
inline void EquivalentCompressSessionReader<T>::GetNormalDeltaArrayRange(int32_t slotId,
                                                                         std::pair<size_t, size_t>* slotRange)
{
    size_t slotIdx = slotId - _beginSlotId;
    slotRange->first = _slotItemBuffer[slotIdx].value;

    size_t slotSize = GetSlotSize(slotId);

    size_t deltaArraySize = EquivalentCompressFileFormat::GetDeltaArraySizeBySlotItemType(
        ((SlotItem*)(_slotItemBuffer) + slotIdx)->slotType, slotSize);

    // TODO, define delta array headerType ?
    slotRange->second = slotRange->first + deltaArraySize + sizeof(DeltaValueArrayTyped<UT>);
}

// precondition: _slotItemBuffer[slotId - _beginSlotId] is not equal value slot
template <typename T>
inline void EquivalentCompressSessionReader<T>::GetLongDeltaArrayRange(int32_t slotId,
                                                                       std::pair<size_t, size_t>* slotRange)
{
    size_t slotIdx = slotId - _beginSlotId;
    slotRange->first = _slotItemBuffer[slotIdx].value;
    int32_t curSlotId = slotId + 1;

    while (curSlotId < _endSlotId) {
        if (!isEqualValueSlot(curSlotId)) {
            slotRange->second = _slotItemBuffer[curSlotId - _beginSlotId].value;
            return;
        }
        curSlotId++;
    }
    // if theres is no not-equal LongSlotItem in slotItemBuffer, cannot calculate accurate deltaArraySize
    // so estimate slotRange by 64-bit deltas
    size_t estimateDeltaArraySize =
        sizeof(LongValueArrayHeader) +
        EquivalentCompressFileFormat::GetDeltaArraySizeBySlotItemType(SIT_DELTA_UINT64, GetSlotSize(slotId));
    slotRange->second =
        std::min(slotRange->first + estimateDeltaArraySize, _fileContent.fileLength - _fileContent.valueBaseOffset);
}

template <typename T>
inline future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> EquivalentCompressSessionReader<T>::BatchGet(
    const std::vector<int32_t>& batchPos, indexlib::file_system::ReadOption readOption, std::vector<T>* values) noexcept

{
    if (!std::is_sorted(batchPos.begin(), batchPos.end())) {
        // batch pos should be non-decreasing
        co_return indexlib::index::ErrorCodeVec(batchPos.size(), indexlib::index::ErrorCode::BadParameter);
    }
    if (!batchPos.empty() && *batchPos.rbegin() >= _compressReader->Size()) {
        co_return indexlib::index::ErrorCodeVec(batchPos.size(), indexlib::index::ErrorCode::BadParameter);
    }
    assert(values);
    indexlib::index::ErrorCodeVec result(batchPos.size(), indexlib::index::ErrorCode::Runtime);
    values->resize(batchPos.size());
    if (_compressReader->IsReadFromMemory()) {
        for (size_t i = 0; i < batchPos.size(); ++i) {
            (*values)[i] = _compressReader->Get(batchPos[i]).second;
        }
        co_return indexlib::index::ErrorCodeVec(batchPos.size(), indexlib::index::ErrorCode::OK);
    }
    size_t beginIdx = 0;
    for (; beginIdx < batchPos.size(); ++beginIdx) {
        if (TryFillFromFirstBuffer(batchPos[beginIdx], (*values)[beginIdx])) {
            result[beginIdx] = indexlib::index::ErrorCode::OK;
        } else {
            break;
        }
    }
    if (beginIdx == batchPos.size()) {
        co_return result;
    }
    auto batchResult = co_await BatchReadDeltaArray(batchPos, beginIdx, readOption, values);
    for (size_t i = 0; i < batchResult.size(); ++i) {
        result[i + beginIdx] = batchResult[i];
    }
    co_return result;
}

template <typename T>
inline future_lite::Future<size_t>
EquivalentCompressSessionReader<T>::FetchCurrentReadBatch(indexlib::file_system::ReadOption readOption)
{
    // precondition, deltaArrayBuffer has already been allocated
    if (_currentBatch.end <= _currentBatch.begin) {
        // currentBatch contain all equal slots
        // or currentBatch is empty
        if (_currentBatch.beginSlotId != -1) {
            _lastReadRange.first = GetBeginRowIdInSlot(_currentBatch.beginSlotId);
            _lastReadRange.second =
                std::min((int64_t)_fileContent.itemCount, GetBeginRowIdInSlot(_currentBatch.lastSlotId + 1));
        }
        _currentBatch.Reset();
        return future_lite::makeReadyFuture(size_t(0));
    }

    return _fileStream
        ->ReadAsync(GetDeltaArrayBuffer(), _currentBatch.end - _currentBatch.begin,
                    _fileContent.valueBaseOffset + _currentBatch.begin, readOption)
        .thenValue([this](size_t readSize) {
            _ioBufferOffset = _currentBatch.begin;
            _lastReadRange.first = GetBeginRowIdInSlot(_currentBatch.beginSlotId);
            _lastReadRange.second =
                std::min((int64_t)(_fileContent.itemCount), GetBeginRowIdInSlot(_currentBatch.lastSlotId + 1));
            _currentBatch.Reset();
            return future_lite::makeReadyFuture(size_t(readSize));
        });
}

template <typename T>
T EquivalentCompressSessionReader<T>::GetNormalValueFromDeltaArray(uint8_t* slotItem, uint8_t* deltaArray,
                                                                   size_t inArrayIdx)
{
    assert(slotItem);
    SlotItem* item = (SlotItem*)slotItem;
    if (item->IsEqual()) {
        return ZigZagEncoder::Decode((UT)(item->value));
    }
    assert(deltaArray);
    DeltaValueArrayTyped<UT>* arrayHeader = (DeltaValueArrayTyped<UT>*)deltaArray;
    UT deltaValue =
        EquivalentCompressFileFormat::GetDeltaValueBySlotItemType<UT>(item->slotType, arrayHeader->delta, inArrayIdx);
    return ZigZagEncoder::Decode(arrayHeader->baseValue + deltaValue);
}

#define DECLARE_UNSUPPORT_TYPE_IMPL(type)                                                                              \
    template <>                                                                                                        \
    inline type EquivalentCompressSessionReader<type>::GetNormalValueFromBuffer(size_t rowId)                          \
    {                                                                                                                  \
        assert(false);                                                                                                 \
        return type();                                                                                                 \
    }                                                                                                                  \
    template <>                                                                                                        \
    inline size_t EquivalentCompressSessionReader<type>::GetDeltaArraySize(SlotType* item, size_t slotSize)            \
    {                                                                                                                  \
        assert(false);                                                                                                 \
        return 0;                                                                                                      \
    }                                                                                                                  \
    template <>                                                                                                        \
    inline type EquivalentCompressSessionReader<type>::GetValueFromDeltaArray(uint8_t* slotItem, uint8_t* deltaArray,  \
                                                                              size_t inArrayIdx)                       \
    {                                                                                                                  \
        assert(false);                                                                                                 \
        return type();                                                                                                 \
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

///////////////////////////////////////////////////////////////

} // namespace indexlib::index
