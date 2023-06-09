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
#include "indexlib/index/aggregation/SequentialMultiSegmentValueIterator.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SequentialMultiSegmentValueIterator);

void SequentialMultiSegmentValueIterator::Init(std::vector<std::unique_ptr<IValueIterator>> iters)
{
    for (auto& it : iters) {
        if (it->HasNext()) {
            _iters.emplace_back(std::move(it));
        }
    }
}

bool SequentialMultiSegmentValueIterator::HasNext() const
{
    if (_cursor + 1 < _iters.size()) {
        return true;
    }
    if (_cursor + 1 == _iters.size()) {
        return _iters[_cursor]->HasNext();
    }
    return false;
}

Status SequentialMultiSegmentValueIterator::Next(autil::mem_pool::PoolBase* pool, autil::StringView& value)
{
    while (_cursor < _iters.size()) {
        if (_iters[_cursor]->HasNext()) {
            auto s = _iters[_cursor]->Next(pool, value);
            if (!s.IsEof()) {
                return s;
            }
            ++_cursor;
        } else {
            ++_cursor;
        }
    }
    return Status::Eof();
}

std::vector<std::unique_ptr<IValueIterator>> SequentialMultiSegmentValueIterator::Steal()
{
    std::vector<std::unique_ptr<IValueIterator>> ret;
    ret.swap(_iters);
    return ret;
}

std::unique_ptr<IValueIterator>
SequentialMultiSegmentValueIterator::Create(std::vector<std::unique_ptr<IValueIterator>> segIters)
{
    auto iter = std::make_unique<SequentialMultiSegmentValueIterator>();
    iter->Init(std::move(segIters));
    return iter;
}

} // namespace indexlibv2::index
