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

#include "indexlib/common/field_format/range/range_field_encoder.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"

DECLARE_REFERENCE_CLASS(index, InMemNormalIndexSegmentReader);
namespace indexlibv2::index {
class SegmentPostings;
}

namespace indexlib { namespace index {

class RangeInfo;
DEFINE_SHARED_PTR(RangeInfo);

class RangeInMemSegmentReader : public IndexSegmentReader
{
public:
    RangeInMemSegmentReader(std::shared_ptr<InMemNormalIndexSegmentReader> bottomReader,
                            std::shared_ptr<InMemNormalIndexSegmentReader> highReader, RangeInfoPtr timeRangeInfo);
    ~RangeInMemSegmentReader();

public:
    std::shared_ptr<AttributeSegmentReader> GetSectionAttributeSegmentReader() const override
    {
        assert(false);
        return std::shared_ptr<AttributeSegmentReader>();
    }

    std::shared_ptr<InMemBitmapIndexSegmentReader> GetBitmapSegmentReader() const override
    {
        assert(false);
        return std::shared_ptr<InMemBitmapIndexSegmentReader>();
        // return mIndexSegmentReader->GetBitmapSegmentReader();
    }

    bool Lookup(uint64_t leftTerm, uint64_t rightTerm, docid_t baseDocId,
                const std::shared_ptr<SegmentPostings>& segPosting, autil::mem_pool::Pool* sessionPool) const;

private:
    void FillSegmentPostings(std::shared_ptr<InMemNormalIndexSegmentReader> reader,
                             indexlib::common::RangeFieldEncoder::Ranges& levelRanges, docid_t baseDocId,
                             std::shared_ptr<SegmentPostings> segPostings, autil::mem_pool::Pool* sessionPool) const;

    std::shared_ptr<InMemNormalIndexSegmentReader> mBottomLevelReader;
    std::shared_ptr<InMemNormalIndexSegmentReader> mHighLevelReader;
    RangeInfoPtr mRangeInfo;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeInMemSegmentReader);
}} // namespace indexlib::index
