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
#include "indexlib/index/inverted_index/builtin_index/range/RangeLevelLeafReader.h"

#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"

namespace indexlib::index {
namespace {
using index::ErrorCode;
using index::Result;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, RangeLevelLeafReader);

RangeLevelLeafReader::RangeLevelLeafReader(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                           const IndexFormatOption& formatOption, uint64_t docCount,
                                           const std::shared_ptr<DictionaryReader>& dictReader,
                                           const std::shared_ptr<file_system::FileReader>& postingReader)
    : InvertedLeafReader(indexConfig, formatOption, docCount, dictReader, postingReader)
    , _isHashTypeDict(indexConfig->IsHashTypedDictionary())
{
}

RangeLevelLeafReader::~RangeLevelLeafReader() {}

future_lite::coro::Lazy<Result<SegmentPosting>>
RangeLevelLeafReader::FillOneSegment(dictvalue_t value, docid_t baseDocId, autil::mem_pool::Pool* sessionPool,
                                     file_system::ReadOption option, InvertedIndexSearchTracer* tracer) noexcept
{
    SegmentPosting segPosting;
    ErrorCode result = co_await GetSegmentPostingAsync(value, baseDocId, segPosting, sessionPool, option, tracer);
    if (result == ErrorCode::OK) {
        co_return segPosting;
    }
    co_return result;
}

future_lite::coro::Lazy<ErrorCode> RangeLevelLeafReader::FillSegmentPostings(
    const RangeFieldEncoder::Ranges& ranges, docid_t baseDocId, const std::shared_ptr<SegmentPostings>& segmentPostings,
    autil::mem_pool::Pool* sessionPool, file_system::ReadOption option, InvertedIndexSearchTracer* tracer) noexcept
{
    if (_isHashTypeDict) {
        AUTIL_LOG(ERROR, "range index should use tiered dictionary.");
        co_return ErrorCode::UnSupported;
    }
    auto dictReader = std::dynamic_pointer_cast<TieredDictionaryReader>(_dictReader);
    if (!dictReader) {
        assert(false);
        AUTIL_LOG(ERROR, "cast failed, range index should use tiered dictionary reader.");
        co_return ErrorCode::UnSupported;
    }
    std::shared_ptr<DictionaryIterator> iter = _dictReader->CreateIterator();
    assert(iter);
    std::vector<future_lite::coro::Lazy<Result<SegmentPosting>>> tasks;
    if (tracer) {
        option.blockCounter = tracer->GetDictionaryBlockCacheCounter();
    }
    size_t seekDictCount = 0;
    for (size_t i = 0; i < ranges.size(); i++) {
        ++seekDictCount;
        auto seekEc = co_await iter->SeekAsync((dictkey_t)ranges[i].first, option);
        if (seekEc != ErrorCode::OK) {
            co_return seekEc;
        }
        while (iter->HasNext()) {
            index::DictKeyInfo key;
            dictvalue_t value = 0;
            ++seekDictCount;
            auto nextEc = co_await iter->NextAsync(key, option, value);
            if (nextEc != ErrorCode::OK) {
                co_return nextEc;
            }
            if (key.IsNull()) {
                continue;
            }
            if (key.GetKey() > ranges[i].second) {
                break;
            }
            tasks.push_back(FillOneSegment(value, baseDocId, sessionPool, option, tracer));
        }
    }
    if (tracer) {
        tracer->IncDictionaryLookupCount(seekDictCount);
        if (seekDictCount > 1) {
            tracer->IncDictionaryHitCount(seekDictCount - 1);
        }
    }
    auto taskResult = co_await future_lite::coro::collectAll(std::move(tasks));
    for (size_t i = 0; i < taskResult.size(); ++i) {
        assert(!taskResult[i].hasError());
        if (taskResult[i].value().Ok()) {
            segmentPostings->AddSegmentPosting(taskResult[i].value().Value());
        } else {
            auto ec = taskResult[i].value().GetErrorCode();
            co_return ec;
        }
    }
    co_return ErrorCode::OK;
}

} // namespace indexlib::index
