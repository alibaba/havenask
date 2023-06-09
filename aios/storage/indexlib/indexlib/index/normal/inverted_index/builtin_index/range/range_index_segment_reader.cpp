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
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_segment_reader.h"

#include "indexlib/common/field_format/range/range_field_encoder.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeInfo.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::common;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, RangeIndexSegmentReader);

void RangeIndexSegmentReader::Open(const IndexConfigPtr& indexConfig, const SegmentData& segmentData,
                                   const NormalIndexSegmentReader* hintReader)
{
    mSegmentData = segmentData;
    mBottomLevelReader.reset(new RangeLevelSegmentReader(indexConfig->GetIndexName()));
    RangeIndexConfigPtr rangeConfig = DYNAMIC_POINTER_CAST(RangeIndexConfig, indexConfig);
    mBottomLevelReader->Open(rangeConfig->GetBottomLevelIndexConfig(), segmentData, nullptr);
    mHighLevelReader.reset(new RangeLevelSegmentReader(indexConfig->GetIndexName()));
    mHighLevelReader->Open(rangeConfig->GetHighLevelIndexConfig(), segmentData, nullptr);
    file_system::DirectoryPtr indexDirectory = segmentData.GetIndexDirectory(indexConfig->GetIndexName(), true);
    RangeInfo rangeInfo;
    auto status = rangeInfo.Load(indexDirectory->GetIDirectory());
    if (!status.IsOK()) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "RangeInfo file load failed[%s]",
                             indexDirectory->GetPhysicalPath("").c_str());
    }
    mMinNumber = rangeInfo.GetMinNumber();
    mMaxNumber = rangeInfo.GetMaxNumber();
}

future_lite::coro::Lazy<index::Result<SegmentPostingsVec>>
RangeIndexSegmentReader::Lookup(uint64_t leftTerm, uint64_t rightTerm, autil::mem_pool::Pool* sessionPool,
                                file_system::ReadOption option) noexcept
{
    SegmentPostingsVec segmentPostingsVec;
    if (mMinNumber > mMaxNumber || mMinNumber > rightTerm || mMaxNumber < leftTerm) {
        co_return segmentPostingsVec;
    }

    bool alignRight = false;
    bool alignLeft = false;
    if (mMaxNumber < rightTerm) {
        rightTerm = mMaxNumber;
        alignRight = true;
    }

    if (mMinNumber > leftTerm) {
        leftTerm = mMinNumber;
        alignLeft = true;
    }

    RangeFieldEncoder::AlignedTerm(alignLeft, alignRight, leftTerm, rightTerm);
    RangeFieldEncoder::Ranges bottomLevelRange;
    RangeFieldEncoder::Ranges higherLevelRange;
    RangeFieldEncoder::CalculateSearchRange(leftTerm, rightTerm, bottomLevelRange, higherLevelRange);
    shared_ptr<SegmentPostings> segmentPostings(new SegmentPostings);
    auto ec = co_await mBottomLevelReader->FillSegmentPostings(bottomLevelRange, segmentPostings, sessionPool, option);
    if (ec != index::ErrorCode::OK) {
        co_return ec;
    }
    ec = co_await mHighLevelReader->FillSegmentPostings(higherLevelRange, segmentPostings, sessionPool, option);
    if (ec != index::ErrorCode::OK) {
        co_return ec;
    }
    if (!segmentPostings->GetSegmentPostings().empty()) {
        segmentPostingsVec.push_back(segmentPostings);
    }
    co_return segmentPostingsVec;
}
}} // namespace indexlib::index
