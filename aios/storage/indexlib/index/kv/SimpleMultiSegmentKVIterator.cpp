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
#include "indexlib/index/kv/SimpleMultiSegmentKVIterator.h"

namespace indexlibv2::index {

SimpleMultiSegmentKVIterator::SimpleMultiSegmentKVIterator(
    schemaid_t targetSchemaId, std::shared_ptr<config::KVIndexConfig> indexConfig,
    std::shared_ptr<AdapterIgnoreFieldCalculator> ignoreFieldCalculator,
    std::vector<std::shared_ptr<framework::Segment>> segments)
    : MultiSegmentKVIterator(targetSchemaId, std::move(indexConfig), std::move(ignoreFieldCalculator),
                             std::move(segments))
    , _currentIndex(-1)
{
}

SimpleMultiSegmentKVIterator::~SimpleMultiSegmentKVIterator() {}

Status SimpleMultiSegmentKVIterator::Init() { return Status::OK(); }

bool SimpleMultiSegmentKVIterator::HasNext() const
{
    const int32_t segmentCount = static_cast<int32_t>(_segments.size());
    if (segmentCount == 0) {
        return false;
    }
    return _currentIndex < segmentCount;
}

Status SimpleMultiSegmentKVIterator::Next(autil::mem_pool::Pool* pool, Record& record)
{
    while (true) {
        if (_curIter && _curIter->HasNext()) {
            return _curIter->Next(pool, record);
        } else {
            auto s = MoveToSegment(_currentIndex + 1);
            if (!s.IsOK()) {
                return s;
            }
        }
    }
}

Status SimpleMultiSegmentKVIterator::Seek(offset_t segmentIndex, offset_t offset)
{
    auto s = MoveToSegment(segmentIndex);
    if (!s.IsOK()) {
        return s;
    }
    return _curIter->Seek(offset);
}

void SimpleMultiSegmentKVIterator::Reset()
{
    _curIter.reset();
    _currentIndex = -1;
}

std::pair<offset_t, offset_t> SimpleMultiSegmentKVIterator::GetOffset() const
{
    if (!_curIter) {
        return {-1, -1};
    }
    return {_currentIndex, _curIter->GetOffset()};
}

Status SimpleMultiSegmentKVIterator::MoveToSegment(int32_t idx)
{
    std::unique_ptr<IKVIterator> iterator;
    auto s = CreateIterator(idx, iterator);
    if (s.IsOK()) {
        _curIter = std::move(iterator);
        _currentIndex = idx;
    } else if (s.IsEof()) {
        _currentIndex = idx;
    }
    return s;
}

} // namespace indexlibv2::index
