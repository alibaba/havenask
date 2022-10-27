#include <ha3/rank/DistinctPriorityQueue.h>

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, DistinctPriorityQueue);

DistinctPriorityQueue::DistinctPriorityQueue(uint32_t size, autil::mem_pool::Pool *pool,
        const Comparator *cmp, matchdoc::Reference<DistinctInfo> *distinctInfoRef)
    : MatchDocPriorityQueue(size, pool, cmp)
    , _distinctInfoRef(distinctInfoRef)
{
}

DistinctPriorityQueue::~DistinctPriorityQueue() { 
}

void DistinctPriorityQueue::swap(uint32_t a, uint32_t b) {
    MatchDocPriorityQueue::swap(a, b);
    if (a != b) {
        setQueuePosition(_items[a], a);
        setQueuePosition(_items[b], b);
    } else {
        setQueuePosition(_items[a], a);
    }

}

END_HA3_NAMESPACE(rank);

