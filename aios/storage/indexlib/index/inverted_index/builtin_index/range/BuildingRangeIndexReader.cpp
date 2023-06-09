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
#include "indexlib/index/inverted_index/builtin_index/range/BuildingRangeIndexReader.h"

#include "indexlib/index/inverted_index/builtin_index/range/RangeLeafMemReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, BuildingRangeIndexReader);

BuildingRangeIndexReader::BuildingRangeIndexReader(const PostingFormatOption& postingFormatOption)
    : BuildingIndexReader(postingFormatOption)
{
}

BuildingRangeIndexReader::~BuildingRangeIndexReader() {}

void BuildingRangeIndexReader::Lookup(uint64_t leftTerm, uint64_t rightTerm, SegmentPostingsVec& segmentPostings,
                                      autil::mem_pool::Pool* sessionPool,
                                      indexlib::index::InvertedIndexSearchTracer* tracer)
{
    for (size_t i = 0; i < _innerSegReaders.size(); ++i) {
        std::shared_ptr<SegmentPostings> segPosting(new SegmentPostings);
        RangeLeafMemReader* innerReader = (RangeLeafMemReader*)_innerSegReaders[i].second.get();
        if (innerReader->Lookup(leftTerm, rightTerm, _innerSegReaders[i].first, segPosting, sessionPool, tracer) &&
            !segPosting->GetSegmentPostings().empty()) {
            segmentPostings.push_back(segPosting);
        }
    }
}

} // namespace indexlib::index
