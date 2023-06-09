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
#include "indexlib/index/inverted_index/builtin_index/date/BuildingDateIndexReader.h"

#include "indexlib/index/inverted_index/builtin_index/date/DateLeafMemReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, BuildingDateIndexReader);

BuildingDateIndexReader::BuildingDateIndexReader(const PostingFormatOption& postingFormatOption)
    : BuildingIndexReader(postingFormatOption)
{
}

BuildingDateIndexReader::~BuildingDateIndexReader() {}

void BuildingDateIndexReader::Lookup(uint64_t leftTerm, uint64_t rightTerm, SegmentPostingsVec& segmentPostings,
                                     autil::mem_pool::Pool* sessionPool,
                                     indexlib::index::InvertedIndexSearchTracer* tracer)
{
    for (size_t i = 0; i < _innerSegReaders.size(); ++i) {
        SegmentPosting segPosting(_postingFormatOption);
        std::shared_ptr<SegmentPostings> dateSegmentPosting(new SegmentPostings);
        DateLeafMemReader* innerReader = (DateLeafMemReader*)_innerSegReaders[i].second.get();
        if (innerReader->Lookup(leftTerm, rightTerm, _innerSegReaders[i].first, dateSegmentPosting, sessionPool,
                                tracer) &&
            !dateSegmentPosting->GetSegmentPostings().empty()) {
            segmentPostings.push_back(dateSegmentPosting);
        }
    }
}

} // namespace indexlib::index
