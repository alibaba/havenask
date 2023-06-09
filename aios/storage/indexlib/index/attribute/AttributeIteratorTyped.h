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

#include "autil/TimeoutTerminator.h"
#include "indexlib/base/Define.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/AttributeIteratorBase.h"
#include "indexlib/index/attribute/AttributeReaderTraits.h"
#include "indexlib/index/attribute/DefaultValueAttributeMemReader.h"
#include "indexlib/index/common/field_format/attribute/AttributeFieldPrinter.h"

namespace indexlibv2::index {

template <typename T, typename ReaderTraits = AttributeReaderTraits<T>>
class AttributeIteratorTyped : public AttributeIteratorBase
{
public:
    using SegmentReader = typename ReaderTraits::SegmentReader;
    using ReadContext = typename ReaderTraits::SegmentReadContext;
    using InMemSegmentReader = typename ReaderTraits::InMemSegmentReader;
    typedef T AttrType;

public:
    AttributeIteratorTyped(const std::vector<std::shared_ptr<SegmentReader>>& segReaders,
                           const std::vector<uint64_t>& segmentDocCount, const AttributeFieldPrinter* printer,
                           autil::mem_pool::Pool* pool);

    AttributeIteratorTyped(
        const std::vector<std::shared_ptr<SegmentReader>>& segReaders,
        const std::vector<std::pair<docid_t, std::shared_ptr<InMemSegmentReader>>>& buildingAttributeReader,
        const std::shared_ptr<DefaultValueAttributeMemReader>& defaultValueMemReader,
        const std::vector<uint64_t>& segmentDocCount, const AttributeFieldPrinter* printer,
        autil::mem_pool::Pool* pool);

    virtual ~AttributeIteratorTyped() {}

public:
    void Reset() override;
    void PrepareReadContext(autil::mem_pool::Pool* pool);
    bool Seek(docid_t docId, std::string& attrValue) noexcept override;

    inline bool Seek(docid_t docId, T& value) __ALWAYS_INLINE;

    inline bool Seek(docid_t docId, T& value, bool& isNull) __ALWAYS_INLINE;

    inline const char* GetBaseAddress(docid_t docId) __ALWAYS_INLINE;

    inline bool UpdateValue(docid_t docId, const T& value, bool isNull = false);

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> BatchSeek(const std::vector<docid_t>& docIds,
                                                                     indexlib::file_system::ReadOption readOption,
                                                                     std::vector<T>* values,
                                                                     std::vector<bool>* isNullVec) noexcept;

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    BatchSeek(const std::vector<docid_t>& docIds, indexlib::file_system::ReadOption readOption,
              std::vector<std::string>* values) noexcept override;

private:
    bool SeekInRandomMode(docid_t docId, T& value, bool& isNull);

    inline bool ReadFromMemSegment(docid_t docId, T& value, size_t& idx, bool& isNull,
                                   autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;
    inline bool UpdateFieldInMemSegment(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull) __ALWAYS_INLINE;

protected:
    std::vector<ReadContext> _segmentReadContext;
    const std::vector<std::shared_ptr<SegmentReader>>& _segmentReaders;
    const std::vector<uint64_t>& _segmentDocCount;
    std::vector<std::pair<docid_t, std::shared_ptr<InMemSegmentReader>>> _memSegmentReaders;
    std::shared_ptr<DefaultValueAttributeMemReader> _defaultValueMemReader;
    docid_t _currentSegmentBaseDocId;
    docid_t _currentSegmentEndDocId;
    uint32_t _segmentCursor;
    size_t _buildingSegIdx;
    const AttributeFieldPrinter* _fieldPrinter;
};

template <typename T, typename ReaderTraits>
AttributeIteratorTyped<T, ReaderTraits>::AttributeIteratorTyped(
    const std::vector<std::shared_ptr<SegmentReader>>& segReaders, const std::vector<uint64_t>& segmentDocCount,
    const AttributeFieldPrinter* printer, autil::mem_pool::Pool* pool)
    : AttributeIteratorBase(pool)
    , _segmentReaders(segReaders)
    , _segmentDocCount(segmentDocCount)
    , _fieldPrinter(printer)
{
    PrepareReadContext(pool);
    Reset();
}

template <typename T, typename ReaderTraits>
AttributeIteratorTyped<T, ReaderTraits>::AttributeIteratorTyped(
    const std::vector<std::shared_ptr<SegmentReader>>& segReaders,
    const std::vector<std::pair<docid_t, std::shared_ptr<InMemSegmentReader>>>& memSegmentReaders,
    const std::shared_ptr<DefaultValueAttributeMemReader>& defaultValueMemReader,
    const std::vector<uint64_t>& segmentDocCount, const AttributeFieldPrinter* printer, autil::mem_pool::Pool* pool)
    : AttributeIteratorBase(pool)
    , _segmentReaders(segReaders)
    , _segmentDocCount(segmentDocCount)
    , _memSegmentReaders(memSegmentReaders)
    , _defaultValueMemReader(defaultValueMemReader)
    , _fieldPrinter(printer)
{
    PrepareReadContext(pool);
    Reset();
}

template <typename T, typename ReaderTraits>
void AttributeIteratorTyped<T, ReaderTraits>::PrepareReadContext(autil::mem_pool::Pool* pool)
{
    _segmentReadContext.reserve(_segmentReaders.size());
    for (size_t i = 0; i < _segmentReaders.size(); ++i) {
        _segmentReadContext.emplace_back(_segmentReaders[i]->CreateReadContext(pool));
    }
}

template <typename T, typename ReaderTraits>
void AttributeIteratorTyped<T, ReaderTraits>::Reset()
{
    _segmentCursor = 0;
    _currentSegmentBaseDocId = 0;
    _buildingSegIdx = 0;

    if (_segmentDocCount.size() > 0) {
        _currentSegmentEndDocId = _segmentDocCount[0];
    } else {
        _currentSegmentEndDocId = 0;
    }
}

template <typename T, typename ReaderTraits>
bool AttributeIteratorTyped<T, ReaderTraits>::Seek(docid_t docId, std::string& attrValue) noexcept
{
    T value {};
    bool isNull = false;
    if (!Seek(docId, value, isNull)) {
        return false;
    }
    return _fieldPrinter->Print(isNull, value, &attrValue);
}

template <typename T, typename ReaderTraits>
inline bool AttributeIteratorTyped<T, ReaderTraits>::Seek(docid_t docId, T& value, bool& isNull)
{
    if (docId >= _currentSegmentBaseDocId && docId < _currentSegmentEndDocId) {
        return _segmentReaders[_segmentCursor]->Read(docId - _currentSegmentBaseDocId, value, isNull,
                                                     _segmentReadContext[_segmentCursor]);
    }

    if (docId >= _currentSegmentEndDocId) {
        _segmentCursor++;
        for (; _segmentCursor < _segmentReaders.size(); _segmentCursor++) {
            _currentSegmentBaseDocId = _currentSegmentEndDocId;
            _currentSegmentEndDocId += _segmentDocCount[_segmentCursor];
            if (docId < _currentSegmentEndDocId) {
                docid_t localId = docId - _currentSegmentBaseDocId;
                return _segmentReaders[_segmentCursor]->Read(localId, value, isNull,
                                                             _segmentReadContext[_segmentCursor]);
            }
        }
        _segmentCursor = _segmentReaders.size() - 1;
        return ReadFromMemSegment(docId, value, _buildingSegIdx, isNull, _pool);
    }

    Reset();
    return SeekInRandomMode(docId, value, isNull);
}

template <typename T, typename ReaderTraits>
inline bool AttributeIteratorTyped<T, ReaderTraits>::Seek(docid_t docId, T& value)
{
    bool isNull = false;
    return Seek(docId, value, isNull);
}

template <typename T, typename ReaderTraits>
inline const char* AttributeIteratorTyped<T, ReaderTraits>::GetBaseAddress(docid_t docId)
{
    // TODO
    return NULL;
}

template <typename T, typename ReaderTraits>
bool AttributeIteratorTyped<T, ReaderTraits>::SeekInRandomMode(docid_t docId, T& value, bool& isNull)
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < _segmentDocCount.size(); ++i) {
        if (docId < baseDocId + (docid_t)_segmentDocCount[i]) {
            _segmentCursor = i;
            _currentSegmentBaseDocId = baseDocId;
            _currentSegmentEndDocId = baseDocId + _segmentDocCount[i];
            return _segmentReaders[i]->Read(docId - baseDocId, value, isNull, _segmentReadContext[i]);
        }
        baseDocId += _segmentDocCount[i];
    }

    _currentSegmentEndDocId = baseDocId;
    if (_segmentDocCount.size() > 0) {
        _segmentCursor = _segmentDocCount.size() - 1;
        _currentSegmentBaseDocId = _currentSegmentEndDocId - _segmentDocCount[_segmentCursor];
    } else {
        _segmentCursor = 0;
        _currentSegmentBaseDocId = 0;
    }
    return ReadFromMemSegment(docId, value, _buildingSegIdx, isNull, _pool);
}

template <typename T, typename ReaderTraits>
inline bool AttributeIteratorTyped<T, ReaderTraits>::UpdateValue(docid_t docId, const T& value, bool isNull)
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < _segmentDocCount.size(); i++) {
        if (docId < baseDocId + (docid_t)_segmentDocCount[i]) {
            return _segmentReaders[i]->UpdateField(docId - baseDocId, (uint8_t*)&value, sizeof(T), isNull);
        }
        baseDocId += _segmentDocCount[i];
    }
    return UpdateFieldInMemSegment(docId, (uint8_t*)&value, sizeof(T), isNull);
}

template <typename T, typename ReaderTraits>
inline bool AttributeIteratorTyped<T, ReaderTraits>::ReadFromMemSegment(docid_t docId, T& value, size_t& idx,
                                                                        bool& isNull, autil::mem_pool::Pool* pool) const
{
    size_t index = 0;
    for (auto& [baseDocId, memReader] : _memSegmentReaders) {
        if (docId < baseDocId) {
            return false;
        }
        if (memReader->Read(docId - baseDocId, value, isNull, pool)) {
            idx = index;
            return true;
        }
        index++;
    }
    if (_defaultValueMemReader) {
#define CALL_DEFAULT_VALUE_MEM_READER_READ(type)                                                                       \
    if constexpr (std::is_same_v<T, autil::MultiValueType<type>>) {                                                    \
        return _defaultValueMemReader->ReadMultiValue(docId, value, isNull);                                           \
    }                                                                                                                  \
    if constexpr (std::is_same_v<T, type>) {                                                                           \
        return _defaultValueMemReader->ReadSingleValue(docId, value, isNull);                                          \
    }
        CALL_DEFAULT_VALUE_MEM_READER_READ(char)
        CALL_DEFAULT_VALUE_MEM_READER_READ(int8_t)
        CALL_DEFAULT_VALUE_MEM_READER_READ(uint8_t)
        CALL_DEFAULT_VALUE_MEM_READER_READ(int16_t)
        CALL_DEFAULT_VALUE_MEM_READER_READ(uint16_t)
        CALL_DEFAULT_VALUE_MEM_READER_READ(int32_t)
        CALL_DEFAULT_VALUE_MEM_READER_READ(uint32_t)
        CALL_DEFAULT_VALUE_MEM_READER_READ(int64_t)
        CALL_DEFAULT_VALUE_MEM_READER_READ(uint64_t)
        CALL_DEFAULT_VALUE_MEM_READER_READ(float)
        CALL_DEFAULT_VALUE_MEM_READER_READ(double)
        CALL_DEFAULT_VALUE_MEM_READER_READ(autil::MultiChar)
        CALL_DEFAULT_VALUE_MEM_READER_READ(autil::uint128_t)
#undef CALL_DEFAULT_VALUE_MEM_READER_READ
    }
    return false;
}

template <typename T, typename ReaderTraits>
inline bool AttributeIteratorTyped<T, ReaderTraits>::UpdateFieldInMemSegment(docid_t docId, uint8_t* buf,
                                                                             uint32_t bufLen, bool isNull)
{
    for (auto& [baseDocId, memReader] : _memSegmentReaders) {
        if (docId < baseDocId) {
            return false;
        }
        if (memReader->UpdateField(docId - baseDocId, buf, bufLen, isNull)) {
            return true;
        }
    }
    return false;
}

template <typename T, typename ReaderTraits>
future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
AttributeIteratorTyped<T, ReaderTraits>::BatchSeek(const std::vector<docid_t>& docIds,
                                                   indexlib::file_system::ReadOption readOption,
                                                   std::vector<std::string>* values) noexcept
{
    assert(_fieldPrinter);
    std::vector<T> valuesTyped;
    std::vector<bool> isNullVec;
    values->resize(docIds.size());
    auto retVec = co_await BatchSeek(docIds, readOption, &valuesTyped, &isNullVec);
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (retVec[i] == indexlib::index::ErrorCode::OK) {
            _fieldPrinter->Print(isNullVec[i], valuesTyped[i], &(*values)[i]);
        }
    }
    co_return retVec;
}

template <typename T, typename ReaderTraits>
future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
AttributeIteratorTyped<T, ReaderTraits>::BatchSeek(const std::vector<docid_t>& docIds,
                                                   indexlib::file_system::ReadOption readOption, std::vector<T>* values,
                                                   std::vector<bool>* isNullVec) noexcept
{
    if (readOption.timeoutTerminator && readOption.timeoutTerminator->checkRestrictTimeout()) {
        co_return std::vector<indexlib::index::ErrorCode>(docIds.size(), indexlib::index::ErrorCode::Timeout);
    }
    if (!std::is_sorted(docIds.begin(), docIds.end())) {
        co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::Runtime);
    }
    assert(values);
    indexlib::index::ErrorCodeVec result(docIds.size(), indexlib::index::ErrorCode::OK);
    assert(values);
    assert(isNullVec);
    values->resize(docIds.size());
    isNullVec->resize(docIds.size());
    std::vector<future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>> segmentTasks;
    docid_t currentSegDocIdEnd = 0;
    std::deque<std::vector<docid_t>> taskDocIds;
    std::deque<std::vector<T>> segmentValues;
    std::deque<std::vector<bool>> segmentIsNull;
    size_t docIdx = 0;
    for (uint32_t i = 0; i < _segmentDocCount.size(); ++i) {
        docid_t baseDocId = currentSegDocIdEnd;
        std::vector<docid_t> segmentDocIds;
        currentSegDocIdEnd += _segmentDocCount[i];

        while (docIdx < docIds.size() && docIds[docIdx] < currentSegDocIdEnd) {
            segmentDocIds.push_back(docIds[docIdx++] - baseDocId);
        }
        if (!segmentDocIds.empty()) {
            size_t size = taskDocIds.size();
            taskDocIds.push_back(std::move(segmentDocIds));
            segmentValues.push_back(std::vector<T>());
            segmentIsNull.push_back(std::vector<bool>());
            assert(i < _segmentReaders.size());
            segmentTasks.push_back(_segmentReaders[i]->BatchRead(taskDocIds[size], _segmentReadContext[i], readOption,
                                                                 &segmentValues[size], &segmentIsNull[size]));
        }
    }
    auto segmentResults = co_await future_lite::coro::collectAll(move(segmentTasks));
    docIdx = 0;
    for (size_t i = 0; i < segmentResults.size(); ++i) {
        assert(!segmentResults[i].hasError());
        auto segmentResult = segmentResults[i].value();
        for (size_t j = 0; j < segmentResult.size(); ++j) {
            result[docIdx] = segmentResult[j];
            (*values)[docIdx] = segmentValues[i][j];
            (*isNullVec)[docIdx] = segmentIsNull[i][j];
            ++docIdx;
        }
    }

    while (docIdx < docIds.size()) {
        docid_t docId = docIds[docIdx];
        T value = T();
        bool isNull = false;
        if (!ReadFromMemSegment(docId, value, _buildingSegIdx, isNull, _pool)) {
            result[docIdx] = indexlib::index::ErrorCode::Runtime;
        }
        (*values)[docIdx] = value;
        (*isNullVec)[docIdx] = isNull;
        docIdx++;
    }
    co_return result;
}

} // namespace indexlibv2::index
