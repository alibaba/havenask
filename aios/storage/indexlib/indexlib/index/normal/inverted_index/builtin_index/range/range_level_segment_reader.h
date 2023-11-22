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
#include "indexlib/common_define.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"
#include "indexlib/index/inverted_index/format/dictionary/CommonDiskTieredDictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryIterator.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_segment_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class RangeLevelSegmentReader : public index::NormalIndexSegmentReader
{
public:
    RangeLevelSegmentReader(const std::string& parentIndexName)
        : mParentIndexName(parentIndexName)
        , mIsHashTypeDict(false)
    {
    }
    ~RangeLevelSegmentReader() {}

public:
    void Open(const config::IndexConfigPtr& indexConfig, const index_base::SegmentData& segmentData,
              const NormalIndexSegmentReader* hintReader) override;

    future_lite::coro::Lazy<index::ErrorCode>
    FillSegmentPostings(const indexlib::common::RangeFieldEncoder::Ranges& ranges,
                        const std::shared_ptr<SegmentPostings>& segmentPostings, autil::mem_pool::Pool* sessionPool,
                        file_system::ReadOption) noexcept;

private:
    future_lite::coro::Lazy<index::Result<SegmentPosting>>
    FillOneSegment(dictvalue_t value, autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept;

    std::string mParentIndexName;
    bool mIsHashTypeDict;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeLevelSegmentReader);
//////////////////////////////////////////
}} // namespace indexlib::index
