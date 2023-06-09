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
#include "indexlib/index/inverted_index/builtin_index/date/DateLeafMemReader.h"

#include "indexlib/index/common/field_format/date/DateTerm.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"
#include "indexlib/index/inverted_index/builtin_index/date/TimeRangeInfo.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, DateLeafMemReader);

DateLeafMemReader::DateLeafMemReader(const std::shared_ptr<InvertedLeafMemReader>& invertedReader,
                                     config::DateLevelFormat format,
                                     const std::shared_ptr<TimeRangeInfo>& timeRangeInfo)
    : _format(format)
    , _invertedLeafMemReader(invertedReader)
    , _timeRangeInfo(timeRangeInfo)
{
}

DateLeafMemReader::~DateLeafMemReader() {}

bool DateLeafMemReader::Lookup(uint64_t leftTerm, uint64_t rightTerm, docid_t baseDocId,
                               const std::shared_ptr<SegmentPostings>& segPostings, autil::mem_pool::Pool* sessionPool,
                               InvertedIndexSearchTracer* tracer) const
{
    uint64_t minTime = _timeRangeInfo->GetMinTime();
    uint64_t maxTime = _timeRangeInfo->GetMaxTime();
    if (minTime > maxTime || maxTime < leftTerm || minTime > rightTerm) {
        return false;
    }
    if (maxTime <= rightTerm) {
        rightTerm = maxTime;
    }
    if (minTime >= leftTerm) {
        leftTerm = minTime;
    }
    indexlib::file_system::ReadOption option;
    std::vector<uint64_t> terms;
    DateTerm::CalculateTerms(leftTerm, rightTerm, _format, terms);
    for (size_t i = 0; i < terms.size(); i++) {
        SegmentPosting segPosting;
        if (_invertedLeafMemReader->GetSegmentPosting(index::DictKeyInfo(terms[i]), baseDocId, segPosting, sessionPool,
                                                      option, tracer)) {
            segPostings->AddSegmentPosting(segPosting);
        }
    }
    return true;
}

bool DateLeafMemReader::GetSegmentPosting(const index::DictKeyInfo& key, docid_t baseDocId, SegmentPosting& segPosting,
                                          autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption option,
                                          InvertedIndexSearchTracer* tracer) const
{
    if (!_invertedLeafMemReader) {
        return false;
    }
    return _invertedLeafMemReader->GetSegmentPosting(key, baseDocId, segPosting, sessionPool, option, tracer);
}

} // namespace indexlib::index
