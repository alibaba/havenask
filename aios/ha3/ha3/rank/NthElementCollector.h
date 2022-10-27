#ifndef ISEARCH_NTHELEMENTCOLLECTOR_H
#define ISEARCH_NTHELEMENTCOLLECTOR_H

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

class NthElementCollector : public HitCollectorBase
{
public:
    NthElementCollector(uint32_t size,
                        autil::mem_pool::Pool *pool,
                        const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
                        suez::turing::AttributeExpression *expr,
                        ComboComparator *cmp);
    virtual ~NthElementCollector();
public:
    uint32_t doGetItemCount() const override {
        return _matchDocCount;
    }
    HitCollectorType getType() const override {
        return HCT_SINGLE;
    }
    matchdoc::MatchDoc top() const override {
        return _minMatchDoc;
    }
    uint32_t flushBuffer(matchdoc::MatchDoc *&retDocs) override;
    const ComboComparator *getComparator() const override {
        return _cmp;
    }
protected:
    uint32_t collectAndReplace(matchdoc::MatchDoc *matchDocs,
            uint32_t count, matchdoc::MatchDoc *&retDocs) override;
    void doQuickInit(matchdoc::MatchDoc *matchDocs, uint32_t count) override;
    void doStealAllMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target) override;
    matchdoc::MatchDoc collectOneDoc(matchdoc::MatchDoc ) override {
        assert(false);
        return matchdoc::INVALID_MATCHDOC;
    }
    matchdoc::MatchDoc findMinMatchDoc(matchdoc::MatchDoc *matchDocs, uint32_t count);
private:
    void doNthElement();
protected:
    uint32_t _size;
    uint32_t _maxBufferSize;
    uint32_t _matchDocCount;
    matchdoc::MatchDoc *_matchDocBuffer;
    matchdoc::MatchDoc _minMatchDoc;
    ComboComparator *_cmp;
private:
    friend class NthElementCollectorTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(NthElementCollector);

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_NTHELEMENTCOLLECTOR_H
