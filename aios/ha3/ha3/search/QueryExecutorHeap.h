#ifndef ISEARCH_QUERYEXECUTORENTRY_H
#define ISEARCH_QUERYEXECUTORENTRY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <algorithm>

BEGIN_HA3_NAMESPACE(search);

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

END_HA3_NAMESPACE(search);

#endif //ISEARCH_QUERYEXECUTORENTRY_H
