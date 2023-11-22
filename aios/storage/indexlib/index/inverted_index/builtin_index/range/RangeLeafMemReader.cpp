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
#include "indexlib/index/inverted_index/builtin_index/range/RangeLeafMemReader.h"

#include "indexlib/index/inverted_index/InvertedLeafMemReader.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeInfo.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, RangeLeafMemReader);

RangeLeafMemReader::RangeLeafMemReader(const std::shared_ptr<InvertedLeafMemReader>& bottomReader,
                                       const std::shared_ptr<InvertedLeafMemReader>& highReader,
                                       std::shared_ptr<RangeInfo>& rangeInfo)
    : _bottomLevelReader(bottomReader)
    , _highLevelReader(highReader)
    , _rangeInfo(rangeInfo)
{
}

RangeLeafMemReader::~RangeLeafMemReader() {}

void RangeLeafMemReader::FillSegmentPostings(const std::shared_ptr<InvertedLeafMemReader>& reader,
                                             RangeFieldEncoder::Ranges& levelRanges, docid64_t baseDocId,
                                             std::shared_ptr<SegmentPostings> segPostings,
                                             autil::mem_pool::Pool* sessionPool,
                                             indexlib::index::InvertedIndexSearchTracer* tracer) const
{
    file_system::ReadOption option;
    for (size_t i = 0; i < levelRanges.size(); i++) {
        uint64_t left = levelRanges[i].first;
        uint64_t right = levelRanges[i].second;
        for (uint64_t j = left; j <= right; j++) {
            SegmentPosting segPosting;
            if (reader->GetSegmentPosting(index::DictKeyInfo(j), baseDocId, segPosting, sessionPool, option, tracer)) {
                segPostings->AddSegmentPosting(segPosting);
            }
            if (j == right) // when right equal numeric_limits<uin64_t>::max(), plus will cause overflow
            {
                break;
            }
        }
    }
}

bool RangeLeafMemReader::Lookup(uint64_t leftTerm, uint64_t rightTerm, docid64_t baseDocId,
                                const std::shared_ptr<SegmentPostings>& segPostings, autil::mem_pool::Pool* sessionPool,
                                indexlib::index::InvertedIndexSearchTracer* tracer) const
{
    uint64_t minNumber = _rangeInfo->GetMinNumber();
    uint64_t maxNumber = _rangeInfo->GetMaxNumber();
    if (minNumber > maxNumber || minNumber > rightTerm || maxNumber < leftTerm) {
        return false;
    }

    if (maxNumber < rightTerm) {
        rightTerm = maxNumber;
    }

    if (minNumber > leftTerm) {
        leftTerm = minNumber;
    }
    RangeFieldEncoder::Ranges bottomLevelRange;
    RangeFieldEncoder::Ranges higherLevelRange;
    RangeFieldEncoder::CalculateSearchRange(leftTerm, rightTerm, bottomLevelRange, higherLevelRange);

    FillSegmentPostings(_bottomLevelReader, bottomLevelRange, baseDocId, segPostings, sessionPool, tracer);
    FillSegmentPostings(_highLevelReader, higherLevelRange, baseDocId, segPostings, sessionPool, tracer);
    return true;
}

} // namespace indexlib::index
