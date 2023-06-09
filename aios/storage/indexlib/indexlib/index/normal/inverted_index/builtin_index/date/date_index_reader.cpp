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
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_reader.h"

#include "indexlib/config/date_index_config.h"
#include "indexlib/index/common/NumberTerm.h"
#include "indexlib/index/common/field_format/date/DateQueryParser.h"
#include "indexlib/index/common/field_format/date/DateTerm.h"
#include "indexlib/index/inverted_index/RangePostingIterator.h"
#include "indexlib/index/inverted_index/SeekAndFilterIterator.h"
#include "indexlib/index/normal/inverted_index/accessor/number_doc_value_filter_typed.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/building_date_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_segment_reader.h"
#include "indexlib/util/TimestampUtil.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, DateIndexReader);

DateIndexReader::DateIndexReader() {}

DateIndexReader::~DateIndexReader() {}

void DateIndexReader::Open(const IndexConfigPtr& indexConfig, const PartitionDataPtr& partitionData,
                           const InvertedIndexReader* hintReader)
{
    NormalIndexReader::Open(indexConfig, partitionData, hintReader);
    mDateIndexConfig = DYNAMIC_POINTER_CAST(DateIndexConfig, indexConfig);
    mDateQueryParser.reset(new DateQueryParser);
    mDateQueryParser->Init(mDateIndexConfig->GetBuildGranularity(), mDateIndexConfig->GetDateLevelFormat());
    mFieldType = mDateIndexConfig->GetFieldConfig()->GetFieldType();
}

index::Result<PostingIterator*> DateIndexReader::Lookup(const Term& term, uint32_t statePoolSize, PostingType type,
                                                        Pool* sessionPool)
{
    return future_lite::coro::syncAwait(LookupAsync(&term, statePoolSize, type, sessionPool, /*option*/ nullptr));
}

future_lite::coro::Lazy<index::Result<PostingIterator*>>
DateIndexReader::LookupAsync(const Term* term, uint32_t statePoolSize, PostingType type, Pool* sessionPool,
                             file_system::ReadOption option) noexcept
{
    if (term->IsNull()) {
        co_return co_await NormalIndexReader::LookupAsync(term, statePoolSize, type, sessionPool, option);
    }

    uint64_t leftTerm = 0;
    uint64_t rightTerm = 0;
    if (!mDateQueryParser || !mDateQueryParser->ParseQuery(term, leftTerm, rightTerm)) {
        co_return nullptr;
    }

    DocIdRangeVector ranges;
    if (term->HasHintValues()) {
        ranges = MergeDocIdRanges(term->GetHintValues(), ranges);
    }

    ValidatePartitonRange(ranges);
    auto resultWithEc = co_await GetSegmentPostings(leftTerm, rightTerm, sessionPool, ranges, option);
    if (!resultWithEc.Ok()) {
        co_return resultWithEc.GetErrorCode();
    }
    SegmentPostingsVec dateSegmentPostings(move(resultWithEc.Value()));

    RangePostingIterator* iter =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, RangePostingIterator, _indexFormatOption->GetPostingFormatOption(),
                                     sessionPool, /*tracer*/ nullptr);
    if (!iter->Init(dateSegmentPostings)) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter);
        co_return nullptr;
    }
    DocValueFilter* testIter = CreateDocValueFilter(*term, mAttrReader, sessionPool);
    auto* compositeIter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, SeekAndFilterIterator, iter, testIter, sessionPool);
    co_return compositeIter;
}

DocValueFilter* DateIndexReader::CreateDocValueFilter(const Term& term, const AttributeReaderPtr& attrReader,
                                                      Pool* sessionPool)
{
    if (!mAttrReader) {
        return NULL;
    }
    index::Int64Term intTerm = *(index::Int64Term*)(&term);
    AttrType type = attrReader->GetType();
    assert(!attrReader->IsMultiValue());
    if (type == AT_UINT64) {
        return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, legacy::NumberDocValueFilterTyped<uint64_t>, attrReader,
                                            intTerm.GetLeftNumber(), intTerm.GetRightNumber(), sessionPool);
    }

    if (type == AT_UINT32) {
        if (mFieldType == ft_date) {
            return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, legacy::NumberDocValueFilterTyped<uint32_t>, attrReader,
                                                intTerm.GetLeftNumber() / TimestampUtil::DAY_MILLION_SEC,
                                                intTerm.GetRightNumber() / TimestampUtil::DAY_MILLION_SEC, sessionPool);
        } else {
            assert(mFieldType == ft_time);
            return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, legacy::NumberDocValueFilterTyped<uint32_t>, attrReader,
                                                intTerm.GetLeftNumber(), intTerm.GetRightNumber(), sessionPool);
        }
    }
    IE_LOG(ERROR, "date index should use uint64 or uint32 typed attribute");
    return NULL;
}

NormalIndexSegmentReaderPtr DateIndexReader::CreateSegmentReader()
{
    return NormalIndexSegmentReaderPtr(new DateIndexSegmentReader);
}

std::shared_ptr<BuildingIndexReader> DateIndexReader::CreateBuildingIndexReader()
{
    return std::shared_ptr<BuildingIndexReader>(
        new BuildingDateIndexReader(_indexFormatOption->GetPostingFormatOption()));
}

future_lite::coro::Lazy<index::Result<SegmentPostingsVec>>
DateIndexReader::GetSegmentPostings(uint64_t leftTerm, uint64_t rightTerm, Pool* sessionPool,
                                    const DocIdRangeVector& ranges, file_system::ReadOption option) const noexcept
{
    SegmentPostingsVec rangeSegmentPostings;
    std::vector<future_lite::coro::Lazy<index::Result<SegmentPostingsVec>>> tasks;
    bool needBuilding = false;
    if (ranges.empty()) {
        for (size_t i = 0; i < mSegmentReaders.size(); i++) {
            tasks.push_back(
                ((DateIndexSegmentReader*)(mSegmentReaders[i].get()))->Lookup(leftTerm, rightTerm, nullptr, option));
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
                tasks.push_back(((DateIndexSegmentReader*)(mSegmentReaders[currentSegmentIdx].get()))
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
        ((BuildingDateIndexReader*)mBuildingIndexReader.get())
            ->Lookup(leftTerm, rightTerm, rangeSegmentPostings, sessionPool);
    }
    co_return rangeSegmentPostings;
}

}}} // namespace indexlib::index::legacy
