#ifndef ISEARCH_HITCOLLECTORBASE_H
#define ISEARCH_HITCOLLECTORBASE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <autil/mem_pool/PoolVector.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <suez/turing/expression/framework/AttributeExpression.h>

BEGIN_HA3_NAMESPACE(rank);

class ComboComparator;

class HitCollectorBase
{
public:
    enum HitCollectorType {
        HCT_SINGLE,
        HCT_MULTI
    };
public:
    static const uint32_t BATCH_EVALUATE_SCORE_SIZE = 128;
public:
    HitCollectorBase(suez::turing::AttributeExpression *expr,
                     autil::mem_pool::Pool *pool,
                     const common::Ha3MatchDocAllocatorPtr &allocatorPtr);
    virtual ~HitCollectorBase();
private:
    HitCollectorBase(const HitCollectorBase &);
    HitCollectorBase& operator=(const HitCollectorBase &);
public:
    /*
     * get item count.
     */
    uint32_t getItemCount() const;
    /*
     * return hit collector type
     */
    virtual HitCollectorType getType() const = 0;
    /* 
     * collect and evaluate expression.
     * implement for rank.
     */
    void collect(matchdoc::MatchDoc matchDoc, bool needFlatten=false);
    /*
     * only collect match doc.
     * implement for extra_rank and proxy qrs merge.
     */
    void doCollect(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount);
    /*
     * steal all items.
     */
    void stealAllMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target);
    /*
     * batch evaluate to flush matchDoc in matchDocBuffer to heap.
     */
    void flush();
    /*
     * get min score match doc in heap.
     * only work when HitCollectorType == HCT_SINGLE;
     */
    virtual matchdoc::MatchDoc top() const = 0;
    virtual const ComboComparator *getComparator() const = 0;
public:
    void enableLazyScore(size_t bufferSize);
    const common::Ha3MatchDocAllocatorPtr &getMatchDocAllocator() const {
        return _matchDocAllocatorPtr;
    }
    void updateExprEvaluatedStatus();

    bool isScored() const {
        return !_lazyScore;
    }

    void disableBatchScore() {
        _batchScore = false;
    }
    
    bool isEnableBatchScore() const {
        return _batchScore;
    }

    uint32_t getDeletedDocCount() const {
        return _deletedDocCount;
    }
    uint32_t stealCollectCount() {
        uint32_t collectCount = _collectCount;
        _collectCount = 0;
        return collectCount;
    }

public:
    /*
     * collect matchDocs, return replaced matchDoc count.
     */
    virtual uint32_t collectAndReplace(matchdoc::MatchDoc *matchDocs,
            uint32_t count, matchdoc::MatchDoc *&retMatchDocs);
    virtual matchdoc::MatchDoc collectOneDoc(matchdoc::MatchDoc matchDoc) = 0;
    virtual void doStealAllMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target) = 0;
    virtual uint32_t doGetItemCount() const = 0;
    virtual uint32_t flushBuffer(matchdoc::MatchDoc *&retDocs) { return 0; }
public:
    // for case
    autil::mem_pool::Pool *getPool() { return _pool; }
protected:
    virtual void doQuickInit(matchdoc::MatchDoc *matchDocBuffer, uint32_t count);
    virtual void doUpdateExprEvaluatedStatus();
protected:
    void incDeletedCount() { ++_deletedDocCount; }
    void addExtraDocIdentifierCmp(ComboComparator *cmp);
private:
    void evaluateAllMatchDoc(matchdoc::MatchDoc *matchDocs, uint32_t count);
    void evaluateMatchDoc(matchdoc::MatchDoc &matchDoc);
    void doBatchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t count);
    void deallocateMatchDocs(matchdoc::MatchDoc *matchDocs, uint32_t count);
    uint32_t filterDeletedMatchDocs(matchdoc::MatchDoc *matchDocs, uint32_t count);
    void doFlush();
    void collectOneMatchDoc(matchdoc::MatchDoc matchDoc);
    void flattenCollectMatchDoc(matchdoc::MatchDoc matchDoc);
    void collectDoc(matchdoc::MatchDoc matchDoc);
protected:
    suez::turing::AttributeExpression *_expr;
    autil::mem_pool::Pool *_pool;
    bool _batchScore;
    uint32_t _cursor;
    matchdoc::MatchDoc *_matchDocBuffer;
    uint32_t _deletedDocCount;
    common::Ha3MatchDocAllocatorPtr _matchDocAllocatorPtr;
private:
    uint32_t _collectCount;
private:
    bool _lazyScore;
    uint32_t _unscoredMatchDocCount;
    uint32_t _maxUnscoredMatchDocCount;
    matchdoc::MatchDoc *_unscoredMatchDocs;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(HitCollectorBase);

///////////////////////////////////////////////////////////////////////////////////

inline void HitCollectorBase::collect(matchdoc::MatchDoc matchDoc, bool needFlatten) {
    if (likely(!needFlatten)) {
        return collectOneMatchDoc(matchDoc);
    } else {
        return flattenCollectMatchDoc(matchDoc);
    }
}

inline void HitCollectorBase::doCollect(matchdoc::MatchDoc *matchDocs, uint32_t count) {
    matchdoc::MatchDoc *retDocs = NULL;
    uint32_t replacedCount = collectAndReplace(matchDocs, count, retDocs);
    if (replacedCount > 0) {
        deallocateMatchDocs(retDocs, replacedCount);
    }
}

inline void HitCollectorBase::doBatchEvaluate(matchdoc::MatchDoc *matchDocBuffer, uint32_t count) {
    if (_expr) {
        _expr->batchEvaluate(matchDocBuffer, count);
    }
}

inline void HitCollectorBase::evaluateMatchDoc(matchdoc::MatchDoc &matchDoc) {
    if (_expr) {
        _expr->evaluate(matchDoc);
    }
}

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_HITCOLLECTORBASE_H
