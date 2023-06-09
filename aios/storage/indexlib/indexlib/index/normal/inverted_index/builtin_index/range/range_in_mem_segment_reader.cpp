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
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_in_mem_segment_reader.h"

#include "indexlib/index/inverted_index/builtin_index/range/RangeInfo.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"

using namespace std;
using namespace indexlib::common;

using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, RangeInMemSegmentReader);

RangeInMemSegmentReader::RangeInMemSegmentReader(InMemNormalIndexSegmentReaderPtr bottomReader,
                                                 InMemNormalIndexSegmentReaderPtr highReader, RangeInfoPtr rangeInfo)
    : mBottomLevelReader(bottomReader)
    , mHighLevelReader(highReader)
    , mRangeInfo(rangeInfo)
{
}

RangeInMemSegmentReader::~RangeInMemSegmentReader() {}

void RangeInMemSegmentReader::FillSegmentPostings(InMemNormalIndexSegmentReaderPtr reader,
                                                  RangeFieldEncoder::Ranges& levelRanges, docid_t baseDocId,
                                                  shared_ptr<SegmentPostings> segPostings,
                                                  autil::mem_pool::Pool* sessionPool) const
{
    for (size_t i = 0; i < levelRanges.size(); i++) {
        uint64_t left = levelRanges[i].first;
        uint64_t right = levelRanges[i].second;
        for (uint64_t j = left; j <= right; j++) {
            SegmentPosting segPosting;
            if (reader->GetSegmentPosting(index::DictKeyInfo(j), baseDocId, segPosting, sessionPool,
                                          /*tracer*/ nullptr)) {
                segPostings->AddSegmentPosting(segPosting);
            }
            if (j == right) // when right equal numeric_limits<uin64_t>::max(), plus will cause overflow
            {
                break;
            }
        }
    }
}

bool RangeInMemSegmentReader::Lookup(uint64_t leftTerm, uint64_t rightTerm, docid_t baseDocId,
                                     const shared_ptr<SegmentPostings>& segPostings,
                                     autil::mem_pool::Pool* sessionPool) const
{
    uint64_t minNumber = mRangeInfo->GetMinNumber();
    uint64_t maxNumber = mRangeInfo->GetMaxNumber();
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

    FillSegmentPostings(mBottomLevelReader, bottomLevelRange, baseDocId, segPostings, sessionPool);
    FillSegmentPostings(mHighLevelReader, higherLevelRange, baseDocId, segPostings, sessionPool);
    return true;
}
}} // namespace indexlib::index
