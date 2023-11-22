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
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/inverted_index/InvertedLeafReader.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeInfo.h"

namespace indexlib::index {
class RangeLevelLeafReader;

class RangeLeafReader : public InvertedLeafReader
{
public:
    RangeLeafReader(const std::shared_ptr<RangeLevelLeafReader>& bottomReader,
                    const std::shared_ptr<RangeLevelLeafReader>& highReader, const RangeInfo& rangeInfo);
    ~RangeLeafReader();

    future_lite::coro::Lazy<indexlib::index::Result<SegmentPostingsVec>>
    Lookup(uint64_t leftTerm, uint64_t rightTerm, docid64_t baseDocId, autil::mem_pool::Pool* sessionPool,
           file_system::ReadOption option, InvertedIndexSearchTracer* tracer) noexcept;

private:
    RangeInfo _rangeInfo;
    std::shared_ptr<RangeLevelLeafReader> _bottomLevelLeafReader;
    std::shared_ptr<RangeLevelLeafReader> _highLevelLeafReader;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
