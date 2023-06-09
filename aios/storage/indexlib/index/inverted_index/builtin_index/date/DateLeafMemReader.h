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
#include "indexlib/index/inverted_index/InvertedLeafMemReader.h"
#include "indexlib/index/inverted_index/config/DateLevelFormat.h"

namespace indexlib::index {
class TimeRangeInfo;
class SegmentPostings;

class DateLeafMemReader : public IndexSegmentReader
{
public:
    DateLeafMemReader(const std::shared_ptr<InvertedLeafMemReader>& invertedReader, config::DateLevelFormat format,
                      const std::shared_ptr<TimeRangeInfo>& timeRangeInfo);
    ~DateLeafMemReader();
    std::shared_ptr<indexlibv2::index::AttributeMemReader> GetSectionAttributeSegmentReaderV2() const override
    {
        return _invertedLeafMemReader->GetSectionAttributeSegmentReaderV2();
    }

    std::shared_ptr<InMemBitmapIndexSegmentReader> GetBitmapSegmentReader() const override
    {
        return _invertedLeafMemReader->GetBitmapSegmentReader();
    }

    bool Lookup(uint64_t leftTerm, uint64_t rightTerm, docid_t baseDocId,
                const std::shared_ptr<SegmentPostings>& segPostings, autil::mem_pool::Pool* sessionPool,
                InvertedIndexSearchTracer* tracer) const;

    bool GetSegmentPosting(const index::DictKeyInfo& key, docid_t baseDocId, SegmentPosting& segPosting,
                           autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption option,
                           InvertedIndexSearchTracer* tracer) const override;

private:
    config::DateLevelFormat _format;
    std::shared_ptr<InvertedLeafMemReader> _invertedLeafMemReader;
    std::shared_ptr<TimeRangeInfo> _timeRangeInfo;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
