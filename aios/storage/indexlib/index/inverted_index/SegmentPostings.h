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
#include <memory>

#include "indexlib/index/inverted_index/SegmentPosting.h"

namespace indexlib::index {

class SegmentPostings
{
public:
    SegmentPostings() : _baseDocId(0), _docCount(0) {}
    ~SegmentPostings() {}
    using SegmentPostingVec = std::vector<SegmentPosting>;

public:
    void AddSegmentPosting(const SegmentPosting& segmentPosting)
    {
        _segmentPostings.push_back(segmentPosting);
        _baseDocId = segmentPosting.GetBaseDocId();
        _docCount = segmentPosting.GetDocCount();
    }
    SegmentPostingVector& GetSegmentPostings() { return _segmentPostings; }

    docid_t GetBaseDocId() const { return _baseDocId; }
    size_t GetDocCount() const { return _docCount; }

public:
    SegmentPostingVec _segmentPostings;
    docid_t _baseDocId;
    uint32_t _docCount;

private:
    AUTIL_LOG_DECLARE();
};

// TODO: optimize this
using SegmentPostingsVec = std::vector<std::shared_ptr<SegmentPostings>>;
} // namespace indexlib::index
