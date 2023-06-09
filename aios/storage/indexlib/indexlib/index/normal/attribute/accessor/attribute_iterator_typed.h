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

#ifndef __INDEXLIB_ATTRIBUTE_ITERATOR_TYPED_H
#define __INDEXLIB_ATTRIBUTE_ITERATOR_TYPED_H

#include <memory>

#include "autil/TimeoutTerminator.h"
#include "indexlib/common/field_format/attribute/attribute_field_printer.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"
#include "indexlib/index/normal/attribute/accessor/building_attribute_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T, typename ReaderTraits = AttributeReaderTraits<T>>
class AttributeIteratorTyped : public AttributeIteratorBase
{
public:
    typedef typename ReaderTraits::SegmentReader SegmentReader;
    typedef typename ReaderTraits::SegmentReadContext ReadContext;
    typedef std::shared_ptr<SegmentReader> SegmentReaderPtr;
    typedef BuildingAttributeReader<T, ReaderTraits> BuildingAttributeReaderType;
    typedef std::shared_ptr<BuildingAttributeReaderType> BuildingAttributeReaderPtr;
    typedef T AttrType;

public:
    AttributeIteratorTyped(const std::vector<SegmentReaderPtr>& segReaders,
                           const std::vector<uint64_t>& segmentDocCount, const common::AttributeFieldPrinter* printer,
                           autil::mem_pool::Pool* pool);

    AttributeIteratorTyped(const std::vector<SegmentReaderPtr>& segReaders,
                           const BuildingAttributeReaderPtr& buildingAttributeReader,
                           const std::vector<uint64_t>& segmentDocCount, docid_t buildingBaseDocId,
                           const common::AttributeFieldPrinter* printer, autil::mem_pool::Pool* pool);

    virtual ~AttributeIteratorTyped() {}

public:
    void Reset() override;
    void PrepareReadContext(autil::mem_pool::Pool* pool);

    inline bool Seek(docid_t docId, T& value) __ALWAYS_INLINE;

    inline bool Seek(docid_t docId, T& value, bool& isNull) __ALWAYS_INLINE;

    inline const char* GetBaseAddress(docid_t docId) __ALWAYS_INLINE;

    inline bool UpdateValue(docid_t docId, const T& value, bool isNull = false);

    future_lite::coro::Lazy<index::ErrorCodeVec> BatchSeek(const std::vector<docid_t>& docIds,
                                                           file_system::ReadOption readOption, std::vector<T>* values,
                                                           std::vector<bool>* isNullVec) noexcept;

    future_lite::coro::Lazy<index::ErrorCodeVec> BatchSeek(const std::vector<docid_t>& docIds,
                                                           file_system::ReadOption readOption,
                                                           std::vector<std::string>* values) noexcept override;

private:
    bool SeekInRandomMode(docid_t docId, T& value, bool& isNull);

protected:
    std::vector<ReadContext> mSegmentReadContext;
    const std::vector<SegmentReaderPtr>& mSegmentReaders;
    const std::vector<uint64_t>& mSegmentDocCount;
    BuildingAttributeReaderPtr mBuildingAttributeReader;
    docid_t mCurrentSegmentBaseDocId;
    docid_t mCurrentSegmentEndDocId;
    uint32_t mSegmentCursor;
    docid_t mBuildingBaseDocId;
    size_t mBuildingSegIdx;
    const common::AttributeFieldPrinter* mFieldPrinter;

private:
    friend class AttributeIteratorTypedTest;
    friend class VarNumAttributeReaderTest;
    IE_LOG_DECLARE();
};

template <typename T, typename ReaderTraits>
AttributeIteratorTyped<T, ReaderTraits>::AttributeIteratorTyped(const std::vector<SegmentReaderPtr>& segReaders,
                                                                const std::vector<uint64_t>& segmentDocCount,
                                                                const common::AttributeFieldPrinter* printer,
                                                                autil::mem_pool::Pool* pool)
    : AttributeIteratorBase(pool)
    , mSegmentReaders(segReaders)
    , mSegmentDocCount(segmentDocCount)
    , mBuildingBaseDocId(INVALID_DOCID)
    , mFieldPrinter(printer)
{
    PrepareReadContext(pool);
    Reset();
}

template <typename T, typename ReaderTraits>
AttributeIteratorTyped<T, ReaderTraits>::AttributeIteratorTyped(
    const std::vector<SegmentReaderPtr>& segReaders, const BuildingAttributeReaderPtr& buildingAttributeReader,
    const std::vector<uint64_t>& segmentDocCount, docid_t buildingBaseDocId,
    const common::AttributeFieldPrinter* printer, autil::mem_pool::Pool* pool)
    : AttributeIteratorBase(pool)
    , mSegmentReaders(segReaders)
    , mSegmentDocCount(segmentDocCount)
    , mBuildingAttributeReader(buildingAttributeReader)
    , mBuildingBaseDocId(buildingBaseDocId)
    , mFieldPrinter(printer)
{
    PrepareReadContext(pool);
    Reset();
}

template <typename T, typename ReaderTraits>
void AttributeIteratorTyped<T, ReaderTraits>::PrepareReadContext(autil::mem_pool::Pool* pool)
{
    mSegmentReadContext.reserve(mSegmentReaders.size());
    for (size_t i = 0; i < mSegmentReaders.size(); ++i) {
        mSegmentReadContext.emplace_back(mSegmentReaders[i]->CreateReadContext(pool));
    }
}

template <typename T, typename ReaderTraits>
void AttributeIteratorTyped<T, ReaderTraits>::Reset()
{
    mSegmentCursor = 0;
    mCurrentSegmentBaseDocId = 0;
    mBuildingSegIdx = 0;

    if (mSegmentDocCount.size() > 0) {
        mCurrentSegmentEndDocId = mSegmentDocCount[0];
    } else {
        mCurrentSegmentEndDocId = 0;
    }
}

template <typename T, typename ReaderTraits>
inline bool AttributeIteratorTyped<T, ReaderTraits>::Seek(docid_t docId, T& value, bool& isNull)
{
    if (docId >= mCurrentSegmentBaseDocId && docId < mCurrentSegmentEndDocId) {
        return mSegmentReaders[mSegmentCursor]->Read(docId - mCurrentSegmentBaseDocId, value, isNull,
                                                     mSegmentReadContext[mSegmentCursor]);
    }

    if (docId >= mCurrentSegmentEndDocId) {
        mSegmentCursor++;
        for (; mSegmentCursor < mSegmentReaders.size(); mSegmentCursor++) {
            mCurrentSegmentBaseDocId = mCurrentSegmentEndDocId;
            mCurrentSegmentEndDocId += mSegmentDocCount[mSegmentCursor];
            if (docId < mCurrentSegmentEndDocId) {
                docid_t localId = docId - mCurrentSegmentBaseDocId;
                return mSegmentReaders[mSegmentCursor]->Read(localId, value, isNull,
                                                             mSegmentReadContext[mSegmentCursor]);
            }
        }
        mSegmentCursor = mSegmentReaders.size() - 1;
        return mBuildingAttributeReader && mBuildingAttributeReader->Read(docId, value, mBuildingSegIdx, isNull, mPool);
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
    for (size_t i = 0; i < mSegmentDocCount.size(); ++i) {
        if (docId < baseDocId + (docid_t)mSegmentDocCount[i]) {
            mSegmentCursor = i;
            mCurrentSegmentBaseDocId = baseDocId;
            mCurrentSegmentEndDocId = baseDocId + mSegmentDocCount[i];
            return mSegmentReaders[i]->Read(docId - baseDocId, value, isNull, mSegmentReadContext[i]);
        }
        baseDocId += mSegmentDocCount[i];
    }

    mCurrentSegmentEndDocId = baseDocId;
    if (mSegmentDocCount.size() > 0) {
        mSegmentCursor = mSegmentDocCount.size() - 1;
        mCurrentSegmentBaseDocId = mCurrentSegmentEndDocId - mSegmentDocCount[mSegmentCursor];
    } else {
        mSegmentCursor = 0;
        mCurrentSegmentBaseDocId = 0;
    }
    return mBuildingAttributeReader && mBuildingAttributeReader->Read(docId, value, mBuildingSegIdx, isNull, mPool);
}

template <typename T, typename ReaderTraits>
inline bool AttributeIteratorTyped<T, ReaderTraits>::UpdateValue(docid_t docId, const T& value, bool isNull)
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < mSegmentDocCount.size(); i++) {
        if (docId < baseDocId + (docid_t)mSegmentDocCount[i]) {
            return mSegmentReaders[i]->UpdateField(docId - baseDocId, (uint8_t*)&value, sizeof(T), isNull);
        }
        baseDocId += mSegmentDocCount[i];
    }
    return mBuildingAttributeReader &&
           mBuildingAttributeReader->UpdateField(docId, (uint8_t*)&value, sizeof(T), isNull);
}

template <typename T, typename ReaderTraits>
future_lite::coro::Lazy<index::ErrorCodeVec> AttributeIteratorTyped<T, ReaderTraits>::BatchSeek(
    const std::vector<docid_t>& docIds, file_system::ReadOption readOption, std::vector<std::string>* values) noexcept
{
    assert(mFieldPrinter);
    std::vector<T> valuesTyped;
    std::vector<bool> isNullVec;
    values->resize(docIds.size());
    auto retVec = co_await BatchSeek(docIds, readOption, &valuesTyped, &isNullVec);
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (retVec[i] == index::ErrorCode::OK) {
            mFieldPrinter->Print(isNullVec[i], valuesTyped[i], &(*values)[i]);
        }
    }
    co_return retVec;
}

template <typename T, typename ReaderTraits>
future_lite::coro::Lazy<index::ErrorCodeVec>
AttributeIteratorTyped<T, ReaderTraits>::BatchSeek(const std::vector<docid_t>& docIds,
                                                   file_system::ReadOption readOption, std::vector<T>* values,
                                                   std::vector<bool>* isNullVec) noexcept
{
    if (readOption.timeoutTerminator && readOption.timeoutTerminator->checkRestrictTimeout()) {
        co_return std::vector<index::ErrorCode>(docIds.size(), index::ErrorCode::Timeout);
    }
    if (!std::is_sorted(docIds.begin(), docIds.end())) {
        co_return index::ErrorCodeVec(docIds.size(), index::ErrorCode::Runtime);
    }
    assert(values);
    index::ErrorCodeVec result(docIds.size(), index::ErrorCode::OK);
    assert(values);
    assert(isNullVec);
    values->resize(docIds.size());
    isNullVec->resize(docIds.size());
    std::vector<future_lite::coro::Lazy<index::ErrorCodeVec>> segmentTasks;
    docid_t currentSegDocIdEnd = 0;
    std::deque<std::vector<docid_t>> taskDocIds;
    std::deque<std::vector<T>> segmentValues;
    std::deque<std::vector<bool>> segmentIsNull;
    size_t docIdx = 0;
    for (uint32_t i = 0; i < mSegmentDocCount.size(); ++i) {
        docid_t baseDocId = currentSegDocIdEnd;
        std::vector<docid_t> segmentDocIds;
        currentSegDocIdEnd += mSegmentDocCount[i];

        while (docIdx < docIds.size() && docIds[docIdx] < currentSegDocIdEnd) {
            segmentDocIds.push_back(docIds[docIdx++] - baseDocId);
        }
        if (!segmentDocIds.empty()) {
            size_t size = taskDocIds.size();
            taskDocIds.push_back(std::move(segmentDocIds));
            segmentValues.push_back(std::vector<T>());
            segmentIsNull.push_back(std::vector<bool>());
            assert(i < mSegmentReaders.size());
            segmentTasks.push_back(mSegmentReaders[i]->BatchRead(taskDocIds[size], mSegmentReadContext[i], readOption,
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
        assert(docId >= mBuildingBaseDocId);
        T value = T();
        bool isNull = false;
        if (!mBuildingAttributeReader ||
            !mBuildingAttributeReader->Read(docId, value, mBuildingSegIdx, isNull, mPool)) {
            result[docIdx] = index::ErrorCode::Runtime;
        }
        (*values)[docIdx] = value;
        (*isNullVec)[docIdx] = isNull;
        docIdx++;
    }
    co_return result;
}

}} // namespace indexlib::index

#endif //__INDEXLIB_ATTRIBUTE_ITERATOR_TYPED_H
