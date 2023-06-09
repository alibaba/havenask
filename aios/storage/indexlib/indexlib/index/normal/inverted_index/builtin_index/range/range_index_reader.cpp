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
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_reader.h"

#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/index/inverted_index/RangePostingIterator.h"
#include "indexlib/index/inverted_index/SeekAndFilterIterator.h"
#include "indexlib/index/normal/inverted_index/accessor/number_doc_value_filter_typed.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/building_range_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_segment_reader.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::common;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, RangeIndexReader);

RangeIndexReader::RangeIndexReader() : mRangeFieldType(RangeFieldEncoder::UNKOWN) {}

RangeIndexReader::~RangeIndexReader() {}

DocValueFilter* RangeIndexReader::CreateDocValueFilter(const index::Term& term, const AttributeReaderPtr& attrReader,
                                                       autil::mem_pool::Pool* sessionPool)
{
    if (!mAttrReader) {
        return NULL;
    }

    Int64Term intTerm = *(Int64Term*)(&term);
    DocValueFilter* iter = NULL;
    AttrType type = attrReader->GetType();
    bool isMultiValue = attrReader->IsMultiValue();

    switch (type) {
#define CREATE_ITERATOR_TYPED(attrType)                                                                                \
    case attrType: {                                                                                                   \
        if (isMultiValue) {                                                                                            \
            typedef AttrTypeTraits<attrType>::AttrItemType Type;                                                       \
            typedef autil::MultiValueType<Type> MultiType;                                                             \
            iter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, legacy::NumberDocValueFilterTyped<MultiType>, attrReader, \
                                                intTerm.GetLeftNumber(), intTerm.GetRightNumber(), sessionPool);       \
        } else {                                                                                                       \
            typedef AttrTypeTraits<attrType>::AttrItemType Type;                                                       \
            iter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, legacy::NumberDocValueFilterTyped<Type>, attrReader,      \
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

index::Result<index::PostingIterator*> RangeIndexReader::Lookup(const Term& term, uint32_t statePoolSize,
                                                                PostingType type, autil::mem_pool::Pool* sessionPool)
{
    return future_lite::coro::syncAwait(LookupAsync(&term, statePoolSize, type, sessionPool, nullptr));
}

future_lite::coro::Lazy<index::Result<index::PostingIterator*>>
RangeIndexReader::LookupAsync(const Term* term, uint32_t statePoolSize, PostingType type,
                              autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept
{
    uint64_t leftTerm = 0;
    uint64_t rightTerm = 0;
    const index::Int64Term* rangeTerm = dynamic_cast<const index::Int64Term*>(term);
    if (!rangeTerm) {
        co_return nullptr;
    }
    RangeFieldEncoder::ConvertTerm(rangeTerm, mRangeFieldType, leftTerm, rightTerm);

    DocIdRangeVector ranges;
    if (rangeTerm->HasHintValues()) {
        ranges = MergeDocIdRanges(rangeTerm->GetHintValues(), ranges);
    }

    ValidatePartitonRange(ranges);
    auto resultWithEc = co_await GetSegmentPostings(leftTerm, rightTerm, sessionPool, ranges, option);
    if (!resultWithEc.Ok()) {
        co_return resultWithEc.GetErrorCode();
    }
    SegmentPostingsVec rangeSegmentPostings(move(resultWithEc.Value()));

    RangePostingIterator* iter =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, RangePostingIterator, _indexFormatOption->GetPostingFormatOption(),
                                     sessionPool, /*tracer*/ nullptr);
    if (!iter->Init(rangeSegmentPostings)) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter);
        co_return nullptr;
    }

    DocValueFilter* testIter = CreateDocValueFilter(*term, mAttrReader, sessionPool);
    auto* compositeIter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, SeekAndFilterIterator, iter, testIter, sessionPool);
    co_return compositeIter;
}

void RangeIndexReader::Open(const IndexConfigPtr& indexConfig, const PartitionDataPtr& partitionData,
                            const InvertedIndexReader* hintReader)
{
    NormalIndexReader::Open(indexConfig, partitionData, hintReader);
    RangeIndexConfigPtr rangeConfig = DYNAMIC_POINTER_CAST(RangeIndexConfig, indexConfig);
    FieldType fieldType = rangeConfig->GetFieldConfig()->GetFieldType();
    mRangeFieldType = RangeFieldEncoder::GetRangeFieldType(fieldType);
}

NormalIndexSegmentReaderPtr RangeIndexReader::CreateSegmentReader()
{
    return NormalIndexSegmentReaderPtr(new RangeIndexSegmentReader);
}

std::shared_ptr<BuildingIndexReader> RangeIndexReader::CreateBuildingIndexReader()
{
    return std::shared_ptr<BuildingIndexReader>(
        new BuildingRangeIndexReader(_indexFormatOption->GetPostingFormatOption()));
}

future_lite::coro::Lazy<index::Result<SegmentPostingsVec>>
RangeIndexReader::GetSegmentPostings(uint64_t leftTerm, uint64_t rightTerm, autil::mem_pool::Pool* sessionPool,
                                     const DocIdRangeVector& ranges, file_system::ReadOption option) const noexcept
{
    SegmentPostingsVec rangeSegmentPostings;
    std::vector<future_lite::coro::Lazy<index::Result<SegmentPostingsVec>>> tasks;
    bool needBuilding = false;
    if (ranges.empty()) {
        tasks.reserve(mSegmentReaders.size());
        for (size_t i = 0; i < mSegmentReaders.size(); i++) {
            tasks.push_back(
                ((RangeIndexSegmentReader*)(mSegmentReaders[i].get()))->Lookup(leftTerm, rightTerm, nullptr, option));
            // TODO: use session pool when use thread bind  pool object
        }
        if (mBuildingIndexReader) {
            needBuilding = true;
        }

    } else {
        size_t currentRangeIdx = 0;
        size_t currentSegmentIdx = 0;
        bool currentSegmentFilled = false;
        while (currentSegmentIdx < mSegmentReaders.size() && currentRangeIdx < ranges.size()) {
            const auto& range = ranges[currentRangeIdx];
            const auto& segData = mSegmentReaders[currentSegmentIdx]->GetSegmentData();
            docid_t segBegin = segData.GetBaseDocId();
            docid_t segEnd = segData.GetBaseDocId() + segData.GetSegmentInfo()->docCount;
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
                tasks.push_back(((RangeIndexSegmentReader*)(mSegmentReaders[currentSegmentIdx].get()))
                                    ->Lookup(leftTerm, rightTerm, nullptr, option));
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
        if (currentRangeIdx < ranges.size() && mBuildingIndexReader) {
            needBuilding = true;
        }
    }
    auto results = co_await future_lite::coro::collectAll(std::move(tasks));
    for (size_t i = 0; i < results.size(); ++i) {
        assert(!results[i].hasError());
        if (results[i].value().Ok()) {
            auto segmentPostings = move(results[i].value().Value());
            rangeSegmentPostings.insert(rangeSegmentPostings.end(), segmentPostings.begin(), segmentPostings.end());
        } else {
            co_return results[i].value().GetErrorCode();
        }
    }
    if (needBuilding) {
        ((BuildingRangeIndexReader*)mBuildingIndexReader.get())
            ->Lookup(leftTerm, rightTerm, rangeSegmentPostings, sessionPool);
    }
    co_return rangeSegmentPostings;
}

}}} // namespace indexlib::index::legacy
