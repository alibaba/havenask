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
#ifndef __INDEXLIB_DATE_INDEX_SEGMENT_READER_H
#define __INDEXLIB_DATE_INDEX_SEGMENT_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/common/field_format/date/DateTerm.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"
#include "indexlib/index/inverted_index/format/dictionary/CommonDiskTieredDictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_segment_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class TimeRangeInfo;
DEFINE_SHARED_PTR(TimeRangeInfo);

class DateIndexSegmentReader : public index::NormalIndexSegmentReader
{
public:
    DateIndexSegmentReader();
    ~DateIndexSegmentReader();

public:
    void Open(const config::IndexConfigPtr& indexConfig, const index_base::SegmentData& segmentData,
              const NormalIndexSegmentReader* hintReader) override;
    future_lite::coro::Lazy<index::Result<SegmentPostingsVec>> Lookup(uint64_t leftTerm, uint64_t rightTerm,
                                                                      autil::mem_pool::Pool* sessionPool,
                                                                      file_system::ReadOption option) noexcept;

private:
    future_lite::coro::Lazy<index::ErrorCode>
    FillSegmentPostings(const DateTerm::Ranges& ranges, const std::shared_ptr<SegmentPostings>& dateSegmentPostings,
                        autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept;

    future_lite::coro::Lazy<index::Result<SegmentPosting>>
    FillOneSegment(dictvalue_t value, autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept;
    void NormalizeTerms(uint64_t minTime, uint64_t maxTime, uint64_t& leftTerm, uint64_t& rightTerm);

private:
    uint64_t mMinTime;
    uint64_t mMaxTime;
    config::DateLevelFormat mFormat;
    bool mIsHashTypeDict;

private:
    friend class DateIndexSegmentReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateIndexSegmentReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_DATE_INDEX_SEGMENT_READER_H
