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
#include "indexlib/index/common/field_format/range/RangeFieldEncoder.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"

namespace indexlib::index {
class InvertedLeafMemReader;
class RangeInfo;
class SegmentPosting;
class SegmentPostings;

class RangeLeafMemReader : public IndexSegmentReader
{
public:
    RangeLeafMemReader(const std::shared_ptr<InvertedLeafMemReader>& bottomReader,
                       const std::shared_ptr<InvertedLeafMemReader>& highReader, std::shared_ptr<RangeInfo>& rangeInfo);
    ~RangeLeafMemReader();

    std::shared_ptr<AttributeSegmentReader> GetSectionAttributeSegmentReader() const override
    {
        assert(false);
        return nullptr;
    }

    std::shared_ptr<InMemBitmapIndexSegmentReader> GetBitmapSegmentReader() const override
    {
        assert(false);
        return nullptr;
    }

    bool Lookup(uint64_t leftTerm, uint64_t rightTerm, docid64_t baseDocId,
                const std::shared_ptr<SegmentPostings>& segPosting, autil::mem_pool::Pool* sessionPool,
                indexlib::index::InvertedIndexSearchTracer* tracer) const;

private:
    void FillSegmentPostings(const std::shared_ptr<InvertedLeafMemReader>& reader,
                             RangeFieldEncoder::Ranges& levelRanges, docid64_t baseDocId,
                             std::shared_ptr<SegmentPostings> segPostings, autil::mem_pool::Pool* sessionPool,
                             indexlib::index::InvertedIndexSearchTracer* tracer) const;

private:
    std::shared_ptr<InvertedLeafMemReader> _bottomLevelReader;
    std::shared_ptr<InvertedLeafMemReader> _highLevelReader;
    std::shared_ptr<RangeInfo> _rangeInfo;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
