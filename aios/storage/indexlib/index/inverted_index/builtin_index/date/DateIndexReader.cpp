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
#include "indexlib/index/inverted_index/builtin_index/date/DateIndexReader.h"

#include "indexlib/index/common/NumberTerm.h"
#include "indexlib/index/common/field_format/date/DateQueryParser.h"
#include "indexlib/index/common/field_format/date/DateTerm.h"
#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/index/inverted_index/NumberDocValueFilterTyped.h"
#include "indexlib/index/inverted_index/RangePostingIterator.h"
#include "indexlib/index/inverted_index/SeekAndFilterIterator.h"
#include "indexlib/index/inverted_index/builtin_index/date/BuildingDateIndexReader.h"
#include "indexlib/index/inverted_index/builtin_index/date/DateLeafReader.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"
#include "indexlib/util/TimestampUtil.h"

namespace indexlib::index {
namespace {
using index::Result;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, DateIndexReader);

DateIndexReader::DateIndexReader(const std::shared_ptr<InvertedIndexMetrics>& metrics)
    : InvertedIndexReaderImpl(metrics)
{
}

DateIndexReader::~DateIndexReader() {}

Result<PostingIterator*> DateIndexReader::Lookup(const index::Term& term, uint32_t statePoolSize, PostingType type,
                                                 autil::mem_pool::Pool* sessionPool)
{
    return future_lite::coro::syncAwait(LookupAsync(&term, statePoolSize, type, sessionPool, nullptr));
}

future_lite::coro::Lazy<Result<PostingIterator*>> DateIndexReader::LookupAsync(const index::Term* term,
                                                                               uint32_t statePoolSize, PostingType type,
                                                                               autil::mem_pool::Pool* sessionPool,
                                                                               file_system::ReadOption option) noexcept
{
    if (term->IsNull()) {
        co_return co_await InvertedIndexReaderImpl::LookupAsync(term, statePoolSize, type, sessionPool, option);
    }

    uint64_t leftTerm = 0;
    uint64_t rightTerm = 0;
    if (!_dateQueryParser || !_dateQueryParser->ParseQuery(term, leftTerm, rightTerm)) {
        co_return nullptr;
    }

    DocIdRangeVector ranges;
    if (term->HasHintValues()) {
        ranges = MergeDocIdRanges(term->GetHintValues(), ranges);
    }

    ValidatePartitonRange(ranges);
    auto tracer = util::MakePooledUniquePtr<InvertedIndexSearchTracer>(sessionPool);
    auto resultWithEc = co_await GetSegmentPostings(leftTerm, rightTerm, sessionPool, ranges, option, tracer.get());
    if (!resultWithEc.Ok()) {
        co_return resultWithEc.GetErrorCode();
    }
    SegmentPostingsVec dateSegmentPostings(move(resultWithEc.Value()));

    RangePostingIterator* iter =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, RangePostingIterator, _indexFormatOption->GetPostingFormatOption(),
                                     sessionPool, std::move(tracer));
    if (!iter->Init(dateSegmentPostings)) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter);
        co_return nullptr;
    }
    DocValueFilter* testIter = CreateDocValueFilter(*term, sessionPool);
    auto* compositeIter = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, SeekAndFilterIterator, iter, testIter, sessionPool);
    co_return compositeIter;
}

std::shared_ptr<BuildingIndexReader> DateIndexReader::CreateBuildingIndexReader() const
{
    return std::shared_ptr<BuildingIndexReader>(
        new BuildingDateIndexReader(_indexFormatOption->GetPostingFormatOption()));
}

DocValueFilter* DateIndexReader::CreateDocValueFilter(const index::Term& term, autil::mem_pool::Pool* sessionPool)
{
    if (!_attrReader) {
        return NULL;
    }
    index::Int64Term intTerm = *(index::Int64Term*)(&term);
    AttrType type = _attrReader->GetType();
    assert(!_attrReader->IsMultiValue());
    if (type == AT_UINT64) {
        return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, NumberDocValueFilterTyped<uint64_t>, _attrReader,
                                            intTerm.GetLeftNumber(), intTerm.GetRightNumber(), sessionPool);
    }

    if (type == AT_UINT32) {
        if (_fieldType == ft_date) {
            return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, NumberDocValueFilterTyped<uint32_t>, _attrReader,
                                                intTerm.GetLeftNumber() / util::TimestampUtil::DAY_MILLION_SEC,
                                                intTerm.GetRightNumber() / util::TimestampUtil::DAY_MILLION_SEC,
                                                sessionPool);
        } else {
            assert(_fieldType == ft_time);
            return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, NumberDocValueFilterTyped<uint32_t>, _attrReader,
                                                intTerm.GetLeftNumber(), intTerm.GetRightNumber(), sessionPool);
        }
    }
    AUTIL_LOG(ERROR, "date index should use uint64 or uint32 typed attribute");
    return NULL;
}

Status DateIndexReader::DoOpen(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                               const std::vector<InvertedIndexReaderImpl::Indexer>& indexers)
{
    auto status = InvertedIndexReaderImpl::DoOpen(indexConfig, indexers);
    RETURN_IF_STATUS_ERROR(status, "inverted index reader open fail, indexName[%s]",
                           indexConfig->GetIndexName().c_str());
    _dateIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::DateIndexConfig>(indexConfig);
    assert(_dateIndexConfig);
    _dateQueryParser.reset(new DateQueryParser);
    _dateQueryParser->Init(_dateIndexConfig->GetBuildGranularity(), _dateIndexConfig->GetDateLevelFormat());
    _fieldType = _dateIndexConfig->GetFieldConfig()->GetFieldType();
    return Status::OK();
}

future_lite::coro::Lazy<Result<SegmentPostingsVec>>
DateIndexReader::GetSegmentPostings(uint64_t leftTerm, uint64_t rightTerm, autil::mem_pool::Pool* sessionPool,
                                    const DocIdRangeVector& ranges, file_system::ReadOption option,
                                    InvertedIndexSearchTracer* tracer) const noexcept
{
    SegmentPostingsVec rangeSegmentPostings;
    std::vector<future_lite::coro::Lazy<Result<SegmentPostingsVec>>> tasks;
    bool needBuilding = false;
    size_t searchedSegmentCount = 0;
    if (ranges.empty()) {
        searchedSegmentCount = _segmentReaders.size();
        for (size_t i = 0; i < _segmentReaders.size(); i++) {
            tasks.push_back(((DateLeafReader*)(_segmentReaders[i].get()))
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
                    ((DateLeafReader*)(_segmentReaders[currentSegmentIdx].get()))
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
            auto segmentPostings = move(results[i].value().Value());
            rangeSegmentPostings.insert(rangeSegmentPostings.end(), segmentPostings.begin(), segmentPostings.end());
        } else {
            co_return results[i].value().GetErrorCode();
        }
    }
    size_t buildingSegmentCount = 0;
    if (needBuilding) {
        buildingSegmentCount = _buildingIndexReader->GetSegmentCount();
        ((BuildingDateIndexReader*)_buildingIndexReader.get())
            ->Lookup(leftTerm, rightTerm, rangeSegmentPostings, sessionPool, tracer);
    }
    if (tracer) {
        tracer->SetSearchedSegmentCount(searchedSegmentCount + buildingSegmentCount);
        tracer->SetSearchedInMemSegmentCount(buildingSegmentCount);
    }
    co_return rangeSegmentPostings;
}

} // namespace indexlib::index
