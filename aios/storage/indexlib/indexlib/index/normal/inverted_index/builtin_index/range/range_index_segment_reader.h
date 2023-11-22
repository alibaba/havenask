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

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_level_segment_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class RangeIndexSegmentReader : public index::NormalIndexSegmentReader
{
public:
    RangeIndexSegmentReader() {}
    ~RangeIndexSegmentReader() {}

public:
    void Open(const config::IndexConfigPtr& indexConfig, const index_base::SegmentData& segmentData,
              const NormalIndexSegmentReader* hintReader) override;
    future_lite::coro::Lazy<index::Result<SegmentPostingsVec>> Lookup(uint64_t leftTerm, uint64_t rightTerm,
                                                                      autil::mem_pool::Pool* sessionPool,
                                                                      file_system::ReadOption option) noexcept;

private:
    uint64_t mMinNumber;
    uint64_t mMaxNumber;
    RangeLevelSegmentReaderPtr mBottomLevelReader;
    RangeLevelSegmentReaderPtr mHighLevelReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeIndexSegmentReader);
}} // namespace indexlib::index
