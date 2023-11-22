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
#include "indexlib/index/common/field_format/date/DateTerm.h"
#include "indexlib/index/inverted_index/InvertedLeafReader.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"

namespace indexlibv2::config {
class DateIndexConfig;
}

namespace indexlib::index {
class TimeRangeInfo;

class DateLeafReader : public InvertedLeafReader
{
public:
    DateLeafReader(const std::shared_ptr<indexlibv2::config::DateIndexConfig>& dateConfig,
                   const IndexFormatOption& formatOption, uint64_t docCount,
                   const std::shared_ptr<DictionaryReader>& dictReader,
                   const std::shared_ptr<file_system::FileReader>& postingReader, const TimeRangeInfo& rangeInfo);
    ~DateLeafReader();
    future_lite::coro::Lazy<Result<SegmentPostingsVec>> Lookup(uint64_t leftTerm, uint64_t rightTerm,
                                                               docid64_t baseDocId, autil::mem_pool::Pool* sessionPool,
                                                               file_system::ReadOption option,
                                                               InvertedIndexSearchTracer* tracer) noexcept;

private:
    future_lite::coro::Lazy<ErrorCode> FillSegmentPostings(const DateTerm::Ranges& ranges, docid64_t baseDocId,
                                                           const std::shared_ptr<SegmentPostings>& dateSegmentPostings,
                                                           autil::mem_pool::Pool* sessionPool,
                                                           file_system::ReadOption option,
                                                           InvertedIndexSearchTracer* tracer) noexcept;
    future_lite::coro::Lazy<Result<SegmentPosting>> FillOneSegment(dictvalue_t value, docid64_t baseDocId,
                                                                   autil::mem_pool::Pool* sessionPool,
                                                                   file_system::ReadOption option,
                                                                   InvertedIndexSearchTracer* tracer) noexcept;

    void NormalizeTerms(uint64_t minTime, uint64_t maxTime, uint64_t& leftTerm, uint64_t& rightTerm);

private:
    uint64_t _minTime;
    uint64_t _maxTime;
    config::DateLevelFormat _format;
    bool _isHashTypeDict;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
