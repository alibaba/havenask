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
#include "indexlib/index/inverted_index/builtin_index/range/RangeIndexReader.h"

#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index/common/AttrTypeTraits.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/index/inverted_index/NumberDocValueFilterTyped.h"
#include "indexlib/index/inverted_index/RangePostingIterator.h"
#include "indexlib/index/inverted_index/SeekAndFilterIterator.h"
#include "indexlib/index/inverted_index/builtin_index/range/BuildingRangeIndexReader.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeLeafReader.h"
#include "indexlib/index/inverted_index/config/RangeIndexConfig.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlib::index {
namespace {
using index::Result;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, RangeIndexReader);

RangeIndexReader::RangeIndexReader(const std::shared_ptr<InvertedIndexMetrics>& metrics)
    : InvertedIndexReaderImpl(metrics)
{
}

RangeIndexReader::~RangeIndexReader() {}

Status RangeIndexReader::DoOpen(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                const std::vector<InvertedIndexReaderImpl::Indexer>& indexers)

{
    auto status = InvertedIndexReaderImpl::DoOpen(indexConfig, indexers);
    RETURN_IF_STATUS_ERROR(status, "range index reader open failed");
    auto rangeConfig = std::dynamic_pointer_cast<indexlibv2::config::RangeIndexConfig>(indexConfig);
    assert(rangeConfig);
    FieldType fieldType = rangeConfig->GetFieldConfig()->GetFieldType();
    _rangeFieldType = RangeFieldEncoder::GetRangeFieldType(fieldType);
    return Status::OK();
}

Result<PostingIterator*> RangeIndexReader::Lookup(const index::Term& term, uint32_t statePoolSize, PostingType type,
                                                  autil::mem_pool::Pool* sessionPool)
{
    return future_lite::coro::syncAwait(LookupAsync(&term, statePoolSize, type, sessionPool, nullptr));
}

future_lite::coro::Lazy<Result<PostingIterator*>>
RangeIndexReader::LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type,
                              autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept
{
    uint64_t leftTerm = 0;
    uint64_t rightTerm = 0;
    const index::Int64Term* rangeTerm = dynamic_cast<const index::Int64Term*>(term);
    if (!rangeTerm) {
        co_return nullptr;
    }
    RangeFieldEncoder::ConvertTerm(rangeTerm, _rangeFieldType, leftTerm, rightTerm);

    DocIdRangeVector ranges;
    if (rangeTerm->HasHintValues()) {
        ranges = MergeDocIdRanges(rangeTerm->GetHintValues(), ranges);
    }

    ValidatePartitonRange(ranges);
    auto tracer = indexlib::util::MakePooledUniquePtr<indexlib::index::InvertedIndexSearchTracer>(sessionPool);
    auto resultWithEc = co_await GetSegmentPostings(leftTerm, rightTerm, sessionPool, ranges, option, tracer.get());
    if (!resultWithEc.Ok()) {
        AUTIL_LOG(ERROR, "get segment posting fail, errorCode[%d]", (int)resultWithEc.GetErrorCode());
        co_return resultWithEc.GetErrorCode();
    }
    SegmentPostingsVec rangeSegmentPostings(std::move(resultWithEc.Value()));

    RangePostingIterator* iter =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, RangePostingIterator, _indexFormatOption->GetPostingFormatOption(),
                                     sessionPool, std::move(tracer));
    if (!iter->Init(rangeSegmentPostings)) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter);
        co_return nullptr;
    }

    DocValueFilter* testIter = CreateDocValueFilter(*term, sessionPool);
    auto* compositeIter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, SeekAndFilterIterator, iter, testIter, sessionPool);
    co_return compositeIter;
}
future_lite::coro::Lazy<Result<SegmentPostingsVec>>
RangeIndexReader::GetSegmentPostings(uint64_t leftTerm, uint64_t rightTerm, autil::mem_pool::Pool* sessionPool,
                                     const DocIdRangeVector& ranges, file_system::ReadOption option,
                                     InvertedIndexSearchTracer* tracer) const noexcept
{
    SegmentPostingsVec rangeSegmentPostings;
    std::vector<future_lite::coro::Lazy<Result<SegmentPostingsVec>>> tasks;
    bool needBuilding = false;
    size_t searchedSegmentCount = 0;
    if (ranges.empty()) {
        tasks.reserve(_segmentReaders.size());
        searchedSegmentCount = _segmentReaders.size();
        for (size_t i = 0; i < _segmentReaders.size(); i++) {
            tasks.push_back(((RangeLeafReader*)(_segmentReaders[i].get()))
                                ->Lookup(leftTerm, rightTerm, _baseDocIds[i], nullptr, option, tracer));
            // TODO: use session pool when use thread bind  pool object
        }
        if (_buildingIndexReader) {
            needBuilding = true;
        }

    } else {
        size_t currentRangeIdx = 0;
        size_t currentSegmentIdx = 0;
        bool currentSegmentFilled = false;
        while (currentSegmentIdx < _segmentReaders.size() && currentRangeIdx < ranges.size()) {
            const auto& range = ranges[currentRangeIdx];
            docid_t segBegin = _baseDocIds[currentRangeIdx];
            docid_t segEnd = _baseDocIds[currentSegmentIdx] + _segmentReaders[currentSegmentIdx]->GetDocCount();
            if (segEnd <= range.first) {
                ++currentSegmentIdx;
                currentSegmentFilled = false;
                continue;
            }
            if (segBegin >= range.second) {
                ++currentRangeIdx;
                continue;
            }
            if (!currentSegmentFilled) {
                ++searchedSegmentCount;
                tasks.push_back(
                    ((RangeLeafReader*)(_segmentReaders[currentSegmentIdx].get()))
                        ->Lookup(leftTerm, rightTerm, _baseDocIds[currentSegmentIdx], nullptr, option, tracer));
                currentSegmentFilled = true;
            }

            auto minEnd = std::min(segEnd, range.second);
            if (segEnd == minEnd) {
                ++currentSegmentIdx;
                currentSegmentFilled = false;
            }
            if (range.second == minEnd) {
                ++currentRangeIdx;
            }
        }
        if (currentRangeIdx < ranges.size() && _buildingIndexReader) {
            needBuilding = true;
        }
    }
    auto results = co_await future_lite::coro::collectAll(std::move(tasks));
    for (size_t i = 0; i < results.size(); ++i) {
        assert(!results[i].hasError());
        if (results[i].value().Ok()) {
            auto segmentPostings = std::move(results[i].value().Value());
            rangeSegmentPostings.insert(rangeSegmentPostings.end(), segmentPostings.begin(), segmentPostings.end());
        } else {
            co_return results[i].value().GetErrorCode();
        }
    }
    size_t buildingSegmentCount = 0;
    if (needBuilding) {
        buildingSegmentCount = _buildingIndexReader->GetSegmentCount();
        ((BuildingRangeIndexReader*)_buildingIndexReader.get())
            ->Lookup(leftTerm, rightTerm, rangeSegmentPostings, sessionPool, tracer);
    }
    if (tracer) {
        tracer->SetSearchedSegmentCount(searchedSegmentCount + buildingSegmentCount);
        tracer->SetSearchedInMemSegmentCount(buildingSegmentCount);
    }
    co_return rangeSegmentPostings;
}

DocValueFilter* RangeIndexReader::CreateDocValueFilter(const index::Term& term, autil::mem_pool::Pool* sessionPool)
{
    if (!_attrReader) {
        return NULL;
    }

    index::Int64Term intTerm = *(index::Int64Term*)(&term);
    DocValueFilter* iter = NULL;
    AttrType type = _attrReader->GetType();
    bool isMultiValue = _attrReader->IsMultiValue();

    switch (type) {
#define CREATE_ITERATOR_TYPED(attrType)                                                                                \
    case attrType: {                                                                                                   \
        if (isMultiValue) {                                                                                            \
            typedef indexlib::index::AttrTypeTraits<attrType>::AttrItemType Type;                                      \
            typedef autil::MultiValueType<Type> MultiType;                                                             \
            iter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, NumberDocValueFilterTyped<MultiType>, _attrReader,        \
                                                intTerm.GetLeftNumber(), intTerm.GetRightNumber(), sessionPool);       \
        } else {                                                                                                       \
            typedef indexlib::index::AttrTypeTraits<attrType>::AttrItemType Type;                                      \
            iter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, NumberDocValueFilterTyped<Type>, _attrReader,             \
                                                intTerm.GetLeftNumber(), intTerm.GetRightNumber(), sessionPool);       \
        }                                                                                                              \
        break;                                                                                                         \
    }
        NUMBER_ATTRIBUTE_MACRO_HELPER(CREATE_ITERATOR_TYPED);
#undef CREATE_ITERATOR_TYPED
    default:
        assert(false);
    }
    return iter;
}

std::shared_ptr<BuildingIndexReader> RangeIndexReader::CreateBuildingIndexReader() const
{
    return std::make_shared<BuildingRangeIndexReader>(_indexFormatOption->GetPostingFormatOption());
}

} // namespace indexlib::index
