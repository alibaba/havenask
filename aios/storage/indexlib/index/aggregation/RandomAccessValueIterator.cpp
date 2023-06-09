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
#include "indexlib/index/aggregation/RandomAccessValueIterator.h"

namespace indexlibv2::index {

namespace {
class Slice final : public RandomAccessValueIterator
{
public:
    Slice(std::unique_ptr<RandomAccessValueIterator> parent, size_t startIdx, size_t count)
        : _parent(std::move(parent))
        , _startIdx(startIdx)
        , _count(count)
        , _nextId(0)
    {
    }

public:
    bool HasNext() const override { return _nextId < _count; }
    Status Next(autil::mem_pool::PoolBase* pool, autil::StringView& value) override
    {
        value = Get(_nextId++);
        return Status::OK();
    }
    size_t Size() const override { return _count; }
    autil::StringView Get(size_t idx) const override { return _parent->Get(_startIdx + idx); }

private:
    std::unique_ptr<RandomAccessValueIterator> _parent;
    const size_t _startIdx;
    const size_t _count;
    size_t _nextId;
};
} // namespace

std::unique_ptr<RandomAccessValueIterator>
RandomAccessValueIterator::MakeSlice(std::unique_ptr<RandomAccessValueIterator> iter,
                                     const RandomAccessValueIterator::StdIteratorWrapper& begin,
                                     const RandomAccessValueIterator::StdIteratorWrapper& end)
{
    if (!begin.reverse) {
        size_t count = begin.idx >= end.idx ? 0 : end.idx - begin.idx;
        return std::make_unique<Slice>(std::move(iter), begin.idx, count);
    } else {
        size_t idx = end.idx + 1;
        int count = std::max(0, begin.idx - end.idx);
        return std::make_unique<Slice>(std::move(iter), idx, count);
    }
}

} // namespace indexlibv2::index
