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
#include <algorithm>
#include <memory>

#include "indexlib/base/Types.h"

namespace indexlibv2::table {

class SegmentMapper
{
public:
    SegmentMapper() {}

public:
    void Collect(size_t id, segmentindex_t index)
    {
        if (_needId2SegIdx) {
            assert(id < _data.size());
            _data[id] = index;
        }
        ++_segDocCounts[index];
        _maxSegIndex = std::max(_maxSegIndex, index);
    }
    segmentindex_t GetMaxSegmentIndex() const { return _maxSegIndex; }
    segmentindex_t GetSegmentIndex(size_t id) const
    {
        assert(_needId2SegIdx);
        if (Empty()) {
            return 0;
        }
        assert(id < _data.size());
        return _data[id];
    }
    size_t GetSegmentDocCount(segmentindex_t segIdx) const
    {
        assert(segIdx < _segDocCounts.size());
        return _segDocCounts[segIdx];
    }
    void Init(size_t size, bool needId2SegIdx = true)
    {
        _needId2SegIdx = needId2SegIdx;
        if (needId2SegIdx) {
            _data.resize(size, 0);
        }
        _segDocCounts.resize(MAX_SPLIT_SEGMENT_COUNT, 0);
    }

    void Init(size_t size, bool needId2SegIdx, size_t targetSegmentCount)
    {
        _maxSegIndex = targetSegmentCount;
        Init(size, needId2SegIdx);
    }
    void Clear()
    {
        _data.clear();
        _segDocCounts.clear();

        _data.shrink_to_fit();
        _segDocCounts.shrink_to_fit();
    }
    bool Empty() const { return _data.empty(); }

private:
    bool _needId2SegIdx = true;
    segmentindex_t _maxSegIndex = 0;
    std::vector<segmentindex_t> _data;
    std::vector<uint32_t> _segDocCounts;
};

} // namespace indexlibv2::table
