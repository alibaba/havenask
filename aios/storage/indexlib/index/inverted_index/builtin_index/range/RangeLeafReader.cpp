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
#include "indexlib/index/inverted_index/builtin_index/range/RangeLeafReader.h"

#include "indexlib/index/common/field_format/range/RangeFieldEncoder.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeLevelLeafReader.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, RangeLeafReader);

RangeLeafReader::RangeLeafReader(const std::shared_ptr<RangeLevelLeafReader>& bottomReader,
                                 const std::shared_ptr<RangeLevelLeafReader>& highReader, const RangeInfo& rangeInfo)
    : InvertedLeafReader(nullptr, IndexFormatOption(), 0, nullptr, nullptr)
    , _rangeInfo(rangeInfo)
    , _bottomLevelLeafReader(bottomReader)
    , _highLevelLeafReader(highReader)
{
}

RangeLeafReader::~RangeLeafReader() {}

future_lite::coro::Lazy<index::Result<SegmentPostingsVec>>
RangeLeafReader::Lookup(uint64_t leftTerm, uint64_t rightTerm, docid64_t baseDocId, autil::mem_pool::Pool* sessionPool,
                        file_system::ReadOption option, InvertedIndexSearchTracer* tracer) noexcept
{
    SegmentPostingsVec segmentPostingsVec;
    uint64_t minNumber = _rangeInfo.GetMinNumber();
    uint64_t maxNumber = _rangeInfo.GetMaxNumber();
    if (minNumber > maxNumber || minNumber > rightTerm || maxNumber < leftTerm) {
        co_return segmentPostingsVec;
    }

    bool alignRight = false;
    bool alignLeft = false;
    if (maxNumber < rightTerm) {
        rightTerm = maxNumber;
        alignRight = true;
    }

    if (minNumber > leftTerm) {
        leftTerm = minNumber;
        alignLeft = true;
    }

    RangeFieldEncoder::AlignedTerm(alignLeft, alignRight, leftTerm, rightTerm);
    RangeFieldEncoder::Ranges bottomLevelRange;
    RangeFieldEncoder::Ranges higherLevelRange;
    RangeFieldEncoder::CalculateSearchRange(leftTerm, rightTerm, bottomLevelRange, higherLevelRange);
    std::shared_ptr<SegmentPostings> segmentPostings(new SegmentPostings);
    auto ec = co_await _bottomLevelLeafReader->FillSegmentPostings(bottomLevelRange, baseDocId, segmentPostings,
                                                                   sessionPool, option, tracer);
    if (ec != index::ErrorCode::OK) {
        co_return ec;
    }
    ec = co_await _highLevelLeafReader->FillSegmentPostings(higherLevelRange, baseDocId, segmentPostings, sessionPool,
                                                            option, tracer);
    if (ec != index::ErrorCode::OK) {
        co_return ec;
    }
    if (!segmentPostings->GetSegmentPostings().empty()) {
        segmentPostingsVec.push_back(segmentPostings);
    }
    co_return segmentPostingsVec;
}

} // namespace indexlib::index
