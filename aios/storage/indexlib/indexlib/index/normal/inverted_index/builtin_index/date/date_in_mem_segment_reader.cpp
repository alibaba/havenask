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
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_in_mem_segment_reader.h"

#include "indexlib/index/common/field_format/date/DateTerm.h"
#include "indexlib/index/inverted_index/builtin_index/date/TimeRangeInfo.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryIterator.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DateInMemSegmentReader);

DateInMemSegmentReader::DateInMemSegmentReader(index::InMemNormalIndexSegmentReaderPtr indexSegmentReader,
                                               config::DateLevelFormat format, TimeRangeInfoPtr timeRangeInfo)
    : mFormat(format)
    , mIndexSegmentReader(indexSegmentReader)
    , mTimeRangeInfo(timeRangeInfo)
{
}

DateInMemSegmentReader::~DateInMemSegmentReader() {}

bool DateInMemSegmentReader::Lookup(uint64_t leftTerm, uint64_t rightTerm, docid_t baseDocId,
                                    const shared_ptr<SegmentPostings>& segPostings,
                                    autil::mem_pool::Pool* sessionPool) const
{
    uint64_t minTime = mTimeRangeInfo->GetMinTime();
    uint64_t maxTime = mTimeRangeInfo->GetMaxTime();
    if (minTime > maxTime || maxTime < leftTerm || minTime > rightTerm) {
        return false;
    }
    if (maxTime <= rightTerm) {
        rightTerm = maxTime;
    }
    if (minTime >= leftTerm) {
        leftTerm = minTime;
    }
    std::vector<uint64_t> terms;
    DateTerm::CalculateTerms(leftTerm, rightTerm, mFormat, terms);
    for (size_t i = 0; i < terms.size(); i++) {
        SegmentPosting segPosting;
        if (mIndexSegmentReader->GetSegmentPosting(index::DictKeyInfo(terms[i]), baseDocId, segPosting, sessionPool)) {
            segPostings->AddSegmentPosting(segPosting);
        }
    }
    return true;
}

bool DateInMemSegmentReader::GetSegmentPosting(const index::DictKeyInfo& key, docid_t baseDocId,
                                               SegmentPosting& segPosting, autil::mem_pool::Pool* sessionPool,
                                               indexlib::index::InvertedIndexSearchTracer* tracer) const
{
    if (!mIndexSegmentReader) {
        return false;
    }
    return mIndexSegmentReader->GetSegmentPosting(key, baseDocId, segPosting, sessionPool);
}
}} // namespace indexlib::index
