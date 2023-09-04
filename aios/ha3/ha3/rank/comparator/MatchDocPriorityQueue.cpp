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
#include "ha3/rank/MatchDocPriorityQueue.h"

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/rank/Comparator.h"
#include "matchdoc/MatchDoc.h"

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, MatchDocPriorityQueue);

MatchDocPriorityQueue::MatchDocPriorityQueue(uint32_t size,
                                             autil::mem_pool::Pool *pool,
                                             const Comparator *cmp) {
    _items = (matchdoc::MatchDoc *)pool->allocate(sizeof(matchdoc::MatchDoc) * (size + 1));
    _count = 0;
    _size = size;
    setComparator(cmp);
    assert(_items);
}

MatchDocPriorityQueue::~MatchDocPriorityQueue() {}

matchdoc::MatchDoc MatchDocPriorityQueue::pop() {
    if (_count < 1) {
        return matchdoc::INVALID_MATCHDOC;
    }
    auto retItem = _items[1];
    _items[1] = _items[_count];
    _count--;
    adjustDown(1);
    return retItem;
}

void MatchDocPriorityQueue::adjustUp(uint32_t idx) {
    assert(idx <= _count);
    while (idx > 1) {
        uint32_t parent = idx >> 1;
        if (!_queueCmp->compare(_items[idx], _items[parent])) {
            break;
        }
        swap(idx, parent);
        idx = parent;
    }
}

void MatchDocPriorityQueue::adjustDown(uint32_t idx) {
    uint32_t min = idx;
    do {
        idx = min;
        uint32_t left = idx << 1;
        uint32_t right = left + 1;
        if (left <= _count && _queueCmp->compare(_items[left], _items[min])) {
            min = left;
        }
        if (right <= _count && _queueCmp->compare(_items[right], _items[min])) {
            min = right;
        }
        if (min != idx) {
            swap(idx, min);
        }
    } while (min != idx);
}

void MatchDocPriorityQueue::swap(uint32_t a, uint32_t b) {
    if (a != b) {
        auto tmp = _items[a];
        _items[a] = _items[b];
        _items[b] = tmp;
    }
}

void MatchDocPriorityQueue::reset() {
    _count = 0;
}

} // namespace rank
} // namespace isearch
