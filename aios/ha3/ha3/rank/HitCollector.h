#ifndef ISEARCH_HITCOLLECTOR_H
#define ISEARCH_HITCOLLECTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <assert.h>
#include <ha3/rank/Comparator.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/rank/MatchDocPriorityQueue.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3/rank/ComboComparator.h>
#include <ha3/rank/HitCollectorBase.h>

BEGIN_HA3_NAMESPACE(rank);

class HitCollector : public HitCollectorBase
{
public:
    HitCollector(uint32_t size,
                 autil::mem_pool::Pool *pool,
                 const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
                 suez::turing::AttributeExpression *expr,
                 ComboComparator *cmp);
    virtual ~HitCollector();
public:
    uint32_t doGetItemCount() const override {
        return _queue->count();
    }
    HitCollectorType getType() const override {
        return HCT_SINGLE;
    }
    matchdoc::MatchDoc top() const override {
        return _queue->top();
    }
    const ComboComparator *getComparator() const override {
        return _cmp;
    }
protected:
    matchdoc::MatchDoc collectOneDoc(matchdoc::MatchDoc matchDoc) override;
    void doQuickInit(matchdoc::MatchDoc *matchDocs, uint32_t count) override;
    void doStealAllMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target) override;
protected:
    MatchDocPriorityQueue *_queue;
    ComboComparator *_cmp;
private:
    friend class HitCollectorTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(HitCollector);

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_HITCOLLECTOR_H
