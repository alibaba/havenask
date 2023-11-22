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

#include <memory>

#include "indexlib/index/inverted_index/SegmentPostings.h"
#include "indexlib/index/inverted_index/config/DateLevelFormat.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"

namespace indexlib { namespace index {
class TimeRangeInfo;
DEFINE_SHARED_PTR(TimeRangeInfo);

class DateInMemSegmentReader : public index::IndexSegmentReader
{
public:
    DateInMemSegmentReader(std::shared_ptr<index::InMemNormalIndexSegmentReader> indexSegmentReader,
                           config::DateLevelFormat format, TimeRangeInfoPtr timeRangeInfo);
    ~DateInMemSegmentReader();

    std::shared_ptr<index::AttributeSegmentReader> GetSectionAttributeSegmentReader() const override
    {
        return mIndexSegmentReader->GetSectionAttributeSegmentReader();
    }

    std::shared_ptr<InMemBitmapIndexSegmentReader> GetBitmapSegmentReader() const override
    {
        return mIndexSegmentReader->GetBitmapSegmentReader();
    }

    bool Lookup(uint64_t leftTerm, uint64_t rightTerm, docid_t baseDocId,
                const std::shared_ptr<SegmentPostings>& segPosting, autil::mem_pool::Pool* sessionPool) const;

    bool GetSegmentPosting(const index::DictKeyInfo& key, docid64_t baseDocId, SegmentPosting& segPosting,
                           autil::mem_pool::Pool* sessionPool,
                           InvertedIndexSearchTracer* tracer = nullptr) const override;

private:
    config::DateLevelFormat mFormat;
    std::shared_ptr<index::InMemNormalIndexSegmentReader> mIndexSegmentReader;
    TimeRangeInfoPtr mTimeRangeInfo;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateInMemSegmentReader);
}} // namespace indexlib::index
