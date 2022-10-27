#ifndef ISEARCH_DISTINCTHITCOLLECTOR_H
#define ISEARCH_DISTINCTHITCOLLECTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/HitCollector.h>
#include <ha3/rank/Comparator.h>
#include <ha3/rank/GradeComparator.h>
#include <ha3/rank/DistinctInfo.h>
#include <ha3/rank/DistinctBoostScorer.h>
#include <ha3/rank/ComboComparator.h>
#include <ha3/rank/DistinctComparator.h>
#include <map>
#include <ha3/rank/DistinctMap.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3/search/SortExpression.h>
#include <ha3/rank/DistinctInfo.h>
#include <ha3/rank/GradeCalculator.h>
#include <ha3/search/Filter.h>
#include <ha3/rank/DistinctPriorityQueue.h>

BEGIN_HA3_NAMESPACE(rank);

struct DistinctParameter
{
    uint32_t distinctTimes;
    uint32_t distinctCount;
    uint32_t maxItemCount;
    bool reservedFlag;
    bool updateTotalHitFlag;
    search::SortExpression *keyExpression;
    matchdoc::Reference<DistinctInfo> *distinctInfoRef;
    search::Filter *distinctFilter;
    std::vector<double> gradeThresholds;

    // for test
    DistinctParameter(
            uint32_t distTimes = 0, uint32_t distCount = 0,
            uint32_t itemCount = 0, bool reserved = false,
            bool updateTotalHit = false,
            search::SortExpression *keyExpr = NULL,
            matchdoc::Reference<DistinctInfo> *distInfoRef = NULL,
            search::Filter *filter = NULL)
    {
        distinctCount = distCount;
        distinctTimes = distTimes;
        maxItemCount = itemCount;
        reservedFlag = reserved;
        updateTotalHitFlag = updateTotalHit;
        keyExpression = keyExpr;
        distinctInfoRef = distInfoRef;
        distinctFilter = filter;
    }
};

class DistinctHitCollector : public HitCollectorBase
{
public:
    DistinctHitCollector(uint32_t size,
                         autil::mem_pool::Pool *pool,
                         common::Ha3MatchDocAllocatorPtr allocatorPtr,
                         suez::turing::AttributeExpression *expr,
                         ComboComparator *cmp,
                         const DistinctParameter &dp,
                         bool sortFlagForGrade);
    ~DistinctHitCollector() override;
public:
    matchdoc::MatchDoc popItem();
    // just for test
    DistinctMap* getDistinctMap() {return _distinctMap;}
    uint32_t getDistinctTimes() const {return _distinctTimes;}
    uint32_t getDistinctCount() const {return _distinctCount;}
    uint32_t getQueuePosition(matchdoc::MatchDoc matchDoc) const;
public:
    HitCollectorType getType() const override {
        return HCT_SINGLE;
    }
    uint32_t doGetItemCount() const override {
        return _queue->count();
    }
    matchdoc::MatchDoc top() const override {
        return _queue->top();
    }
    const ComboComparator *getComparator() const override {
        return &_comboComparator;
    }
protected:
    matchdoc::MatchDoc collectOneDoc(matchdoc::MatchDoc matchDoc) override;
    void doUpdateExprEvaluatedStatus() override;
    void doStealAllMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target) override;
private:
    matchdoc::MatchDoc replaceElement(DistinctInfo *distinctInfo,
            matchdoc::MatchDoc matchDoc);
    matchdoc::MatchDoc pushQueue(matchdoc::MatchDoc matchDoc);
    void setupComparators(ComboComparator *cmp,
                          Comparator *distinctComparator);

    void adjustQueue(DistinctInfo *item);
    DistinctInfo* adjustDistinctInfosInList(TreeNode *tn);
    void recalculateDistinctBoost(DistinctInfo *item, uint32_t newPosition);
    inline void setQueuePosition(matchdoc::MatchDoc matchDoc, uint32_t idx);
    inline bool canDrop(DistinctInfo& distInfo, matchdoc::MatchDoc matchDoc);
    void calculateGradeBoost(DistinctInfo *distInfo);

    friend class DistinctHitCollectorTest;
private:
    DistinctPriorityQueue *_queue;
    ComboComparator _comboComparator;
    const Comparator *_rawComparator;
    GradeComparator *_gradeComparator;
    suez::turing::AttributeExpression *_keyExpr;
    Comparator *_keyComparator;

    DistinctBoostScorer *_distinctBoostScorer;
    uint32_t _distinctTimes;
    uint32_t _distinctCount;
    bool _reservedFlag;
    bool _updateTotalHitFlag;
    bool _sortFlagForGrade;
    DistinctMap *_distinctMap;
    matchdoc::Reference<DistinctInfo> *_distinctInfoRef;
    search::Filter *_filter;
    std::vector<double> _gradeThresholds;
    GradeCalculatorBase *_gradeCalculator;
private:
    HA3_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////////
///inline functions
inline void DistinctHitCollector::setQueuePosition(matchdoc::MatchDoc matchDoc,
        uint32_t idx)
{
    DistinctInfo *distInfo
        = _distinctInfoRef->getPointer(matchDoc);
    assert(distInfo);
    distInfo->setQueuePosition(idx);
}

inline bool DistinctHitCollector::canDrop(DistinctInfo& distInfo,
        matchdoc::MatchDoc matchDoc)
{
    if (_queue->count() == _queue->size()) {
        distInfo.setDistinctBoost(_distinctBoostScorer->calculateBoost(0, 0));
        return _comboComparator.compare(matchDoc, _queue->item(1));
    }
    return false;
}

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_DISTINCTHITCOLLECTOR_H
