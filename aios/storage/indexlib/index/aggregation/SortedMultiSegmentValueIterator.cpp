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
#include "indexlib/index/aggregation/SortedMultiSegmentValueIterator.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SortedMultiSegmentValueIterator);

SortedMultiSegmentValueIterator::SortedMultiSegmentValueIterator(std::shared_ptr<PackValueComparator> cmp)
    : _cmp(std::move(cmp))
    , _heap(Comparator {_cmp.get()})
{
}

SortedMultiSegmentValueIterator::~SortedMultiSegmentValueIterator()
{
    while (!_heap.empty()) {
        _heap.pop();
    }
}

Status SortedMultiSegmentValueIterator::Init(std::vector<std::unique_ptr<IValueIterator>> iters,
                                             autil::mem_pool::PoolBase* pool)
{
    for (auto& iter : iters) {
        if (iter->HasNext()) {
            _iters.emplace_back(std::move(iter));
        }
    }

    for (size_t i = 0; i < _iters.size(); ++i) {
        if (!_iters[i]->HasNext()) {
            continue;
        }
        autil::StringView value;
        auto s = _iters[i]->Next(pool, value);
        if (!s.IsOK() && !s.IsEof()) {
            return s;
        }
        if (s.IsOK()) {
            _heap.emplace(value, i);
        }
    }
    return Status::OK();
}

bool SortedMultiSegmentValueIterator::HasNext() const { return !_heap.empty(); }

Status SortedMultiSegmentValueIterator::Next(autil::mem_pool::PoolBase* pool, autil::StringView& value)
{
    auto [v, index] = _heap.top();
    _heap.pop();
    value = std::move(v);
    if (_iters[index]->HasNext()) {
        autil::StringView nextValue;
        auto s = _iters[index]->Next(pool, nextValue);
        if (!s.IsOK() && !s.IsEof()) {
            return s;
        }
        if (s.IsOK()) {
            _heap.emplace(std::move(nextValue), index);
        }
    }
    return Status::OK();
}

std::unique_ptr<IValueIterator>
SortedMultiSegmentValueIterator::Create(std::vector<std::unique_ptr<IValueIterator>> iters,
                                        autil::mem_pool::PoolBase* pool, std::shared_ptr<PackValueComparator> cmp)
{
    auto iter = std::make_unique<SortedMultiSegmentValueIterator>(std::move(cmp));
    auto s = iter->Init(std::move(iters), pool);
    if (!s.IsOK()) {
        return nullptr;
    }
    return iter;
}

} // namespace indexlibv2::index
