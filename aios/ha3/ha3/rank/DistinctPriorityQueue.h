#ifndef ISEARCH_DISTINCTPRIORITYQUEUE_H
#define ISEARCH_DISTINCTPRIORITYQUEUE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/Reference.h>
#include <ha3/rank/MatchDocPriorityQueue.h>
#include <ha3/rank/DistinctInfo.h>

BEGIN_HA3_NAMESPACE(rank);

class DistinctPriorityQueue : public MatchDocPriorityQueue
{
public:
    DistinctPriorityQueue(uint32_t size, autil::mem_pool::Pool *pool,
                          const Comparator *cmp,
                          matchdoc::Reference<DistinctInfo> *distinctInfoRef);
    ~DistinctPriorityQueue();
private:
    DistinctPriorityQueue(const DistinctPriorityQueue &);
    DistinctPriorityQueue& operator=(const DistinctPriorityQueue &);
public:
    void swap(uint32_t a, uint32_t b) override;
private:
    void setQueuePosition(matchdoc::MatchDoc matchDoc,
                          uint32_t idx) 
    {
        DistinctInfo *distInfo 
            = _distinctInfoRef->getPointer(matchDoc);
        assert(distInfo);
        distInfo->setQueuePosition(idx);
    }
private:
    matchdoc::Reference<DistinctInfo> *_distinctInfoRef;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(DistinctPriorityQueue);

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_DISTINCTPRIORITYQUEUE_H
