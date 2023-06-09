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

#include <stdint.h>
#include <utility>

#include "indexlib/indexlib.h"

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace search {

struct QueryExecutorEntry {
    docid_t docId;
    uint32_t entryId;
    QueryExecutorEntry() {
        docId = INVALID_DOCID;
        entryId = 0;
    }
};

inline void adjustDown(uint32_t idx, uint32_t end,
                       QueryExecutorEntry* heap)
{
    uint32_t min = idx;
    do {
        idx = min;
        uint32_t left = idx << 1;
        uint32_t right = left + 1;
        if (left <= end &&
            heap[left].docId < heap[min].docId)
        {
            min = left;
        }
        if (right <= end &&
            heap[right].docId < heap[min].docId)
        {
            min = right;
        }
        if (min != idx) {
            std::swap(heap[idx], heap[min]);
        }
    } while (min != idx);
}

template <class CollectFn>
void collectMinEntries(
        uint32_t idx,
        uint32_t end,
        const QueryExecutorEntry* heap,
        CollectFn& fn)
{
    collectMinEntries(idx, end, heap[idx].docId, heap, fn);
}

template <class CollectFn>
void collectMinEntries(
        uint32_t idx,
        uint32_t end,
        docid_t min,
        const QueryExecutorEntry* heap,
        CollectFn& fn)
{
    fn(heap[idx].entryId);
    uint32_t left = idx << 1;
    if (left <= end && heap[left].docId == min) {
        collectMinEntries(left, end, min, heap, fn);
    }
    uint32_t right = left + 1;
    if (right <= end && heap[right].docId == min) {
        collectMinEntries(right, end, min, heap, fn);
    }
}

} // namespace search
} // namespace isearch

