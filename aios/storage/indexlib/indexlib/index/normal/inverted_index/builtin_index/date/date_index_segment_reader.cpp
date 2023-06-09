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
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_segment_reader.h"

#include "indexlib/config/date_index_config.h"
#include "indexlib/index/inverted_index/builtin_index/date/TimeRangeInfo.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryIterator.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DateIndexSegmentReader);

DateIndexSegmentReader::DateIndexSegmentReader() : mMinTime(1), mMaxTime(0), mIsHashTypeDict(false) {}

DateIndexSegmentReader::~DateIndexSegmentReader() {}

void DateIndexSegmentReader::Open(const config::IndexConfigPtr& indexConfig, const index_base::SegmentData& segmentData,
                                  const NormalIndexSegmentReader* hintReader)
{
    mIsHashTypeDict = indexConfig->IsHashTypedDictionary();
    NormalIndexSegmentReader::Open(indexConfig, segmentData, hintReader);
    file_system::DirectoryPtr indexDirectory = segmentData.GetIndexDirectory(indexConfig->GetIndexName(), true);
    TimeRangeInfo rangeInfo;
    auto status = rangeInfo.Load(indexDirectory->GetIDirectory());
    if (!status.IsOK()) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "RangeInfo file load failed. [%s]",
                             indexDirectory->GetPhysicalPath("").c_str());
    }

    mMinTime = rangeInfo.GetMinTime();
    mMaxTime = rangeInfo.GetMaxTime();
    DateIndexConfigPtr dateIndexConfig = DYNAMIC_POINTER_CAST(DateIndexConfig, indexConfig);
    mFormat = dateIndexConfig->GetDateLevelFormat();
}

void DateIndexSegmentReader::NormalizeTerms(uint64_t minTime, uint64_t maxTime, uint64_t& leftTerm, uint64_t& rightTerm)
{
    if (maxTime <= rightTerm) {
        rightTerm = DateTerm::NormalizeToNextYear(maxTime);
    }
    if (minTime >= leftTerm) {
        leftTerm = DateTerm::NormalizeToYear(minTime);
    }
}

future_lite::coro::Lazy<index::Result<SegmentPostingsVec>>
DateIndexSegmentReader::Lookup(uint64_t leftTerm, uint64_t rightTerm, autil::mem_pool::Pool* sessionPool,
                               file_system::ReadOption option) noexcept
{
    SegmentPostingsVec result;
    if (mMinTime > mMaxTime || mMaxTime < leftTerm || mMinTime > rightTerm) {
        co_return result;
    }

    NormalizeTerms(mMinTime, mMaxTime, leftTerm, rightTerm);
    DateTerm::Ranges ranges;
    DateTerm::CalculateRanges(leftTerm, rightTerm, mFormat, ranges);

    shared_ptr<SegmentPostings> dateSegmentPostings(new SegmentPostings);
    auto ec = co_await FillSegmentPostings(ranges, dateSegmentPostings, sessionPool, option);
    if (ec != index::ErrorCode::OK) {
        co_return ec;
    }
    const SegmentPostingVector& postingVec = dateSegmentPostings->GetSegmentPostings();
    if (!postingVec.empty()) {
        result.push_back(dateSegmentPostings);
    }
    co_return result;
}

future_lite::coro::Lazy<index::Result<SegmentPosting>>
DateIndexSegmentReader::FillOneSegment(dictvalue_t value, autil::mem_pool::Pool* sessionPool,
                                       file_system::ReadOption option) noexcept
{
    SegmentPosting segPosting;
    index::ErrorCode result = co_await GetSegmentPostingAsync(value, segPosting, sessionPool, option);
    if (result == index::ErrorCode::OK) {
        co_return segPosting;
    }
    co_return result;
}

future_lite::coro::Lazy<index::ErrorCode>
DateIndexSegmentReader::FillSegmentPostings(const DateTerm::Ranges& ranges,
                                            const shared_ptr<SegmentPostings>& dateSegmentPostings,
                                            autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept
{
    if (mIsHashTypeDict) {
        IE_LOG(ERROR, "range index should use tiered dictionary.");
        co_return index::ErrorCode::UnSupported;
    }

    auto dictReader = std::dynamic_pointer_cast<TieredDictionaryReader>(mDictReader);
    if (!dictReader) {
        assert(false);
        IE_LOG(ERROR, "cast failed, range index should use tiered dictionary reader.");
        co_return index::ErrorCode::UnSupported;
    }
    std::shared_ptr<DictionaryIterator> iter = mDictReader->CreateIterator();
    assert(iter);
    std::vector<future_lite::coro::Lazy<index::Result<SegmentPosting>>> tasks;
    for (size_t i = 0; i < ranges.size(); i++) {
        auto seekEc = co_await iter->SeekAsync((dictkey_t)ranges[i].first, option);
        if (seekEc != index::ErrorCode::OK) {
            co_return seekEc;
        }
        while (iter->HasNext()) {
            index::DictKeyInfo key;
            dictvalue_t value = 0;
            auto nextEc = co_await iter->NextAsync(key, option, value);
            if (nextEc != index::ErrorCode::OK) {
                co_return nextEc;
            }
            if (key.IsNull()) {
                continue;
            }
            if (key.GetKey() > ranges[i].second) {
                break;
            }
            tasks.push_back(FillOneSegment(value, sessionPool, option));
        }
    }
    auto taskResult = co_await future_lite::coro::collectAll(std::move(tasks));
    for (size_t i = 0; i < taskResult.size(); ++i) {
        assert(!taskResult[i].hasError());
        if (taskResult[i].value().Ok()) {
            dateSegmentPostings->AddSegmentPosting(taskResult[i].value().Value());
        } else {
            auto ec = taskResult[i].value().GetErrorCode();
            co_return ec;
        }
    }
    co_return index::ErrorCode::OK;
}

}} // namespace indexlib::index
