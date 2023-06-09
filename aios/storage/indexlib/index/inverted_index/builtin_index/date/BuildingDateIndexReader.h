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
#include "indexlib/index/inverted_index/BuildingIndexReader.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"

namespace indexlib::index {

class BuildingDateIndexReader : public BuildingIndexReader
{
public:
    BuildingDateIndexReader(const PostingFormatOption& postingFormatOption);
    ~BuildingDateIndexReader();
    void Lookup(uint64_t leftTerm, uint64_t rightTerm, SegmentPostingsVec& segmentPostings,
                autil::mem_pool::Pool* sessionPool, indexlib::index::InvertedIndexSearchTracer* tracer);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
