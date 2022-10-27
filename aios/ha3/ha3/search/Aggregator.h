#ifndef ISEARCH_AGGREGATOR_H
#define ISEARCH_AGGREGATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/Filter.h>
#include <ha3/search/AggregateFunction.h>
#include <ha3/common/AggregateResult.h>
#include <ha3/common/Result.h>
#include <assert.h>
#include <ha3/search/BatchAggregateSampler.h>
#include <matchdoc/MatchDoc.h>
#include <matchdoc/MatchDocAllocator.h>
#include <ha3/common/CommonDef.h>
#include <cava/codegen/CavaModule.h>
#include <matchdoc/Reference.h>
#include <suez/turing/expression/cava/common/CavaAggModuleInfo.h>

BEGIN_HA3_NAMESPACE(search);

class Aggregator
{
public:
    typedef std::vector<AggregateFunction*> FunctionVector;
public:
    Aggregator(autil::mem_pool::Pool *pool) 
        : _pool(pool)
        , _matchDocAllocator(NULL)
        , _isStopped(false)
        , _toAggDocCount(0) {}
    virtual ~Aggregator() {}
public:
    void aggregate(matchdoc::MatchDoc matchDoc) {
        if (_isStopped) {
            return;
        }
        _toAggDocCount++;
        doAggregate(matchDoc);
    }
    void setFactor(double factor) {
        _factor = factor;
    }
    double getFactor() {
        return _factor;
    }
    virtual void batchAggregate(std::vector<matchdoc::MatchDoc> &matchDocs) {
        for (auto matchDoc : matchDocs) {
            aggregate(matchDoc);
        }
    }
    virtual void doAggregate(matchdoc::MatchDoc matchDoc) = 0;
    virtual common::AggregateResultsPtr collectAggregateResult() = 0;
    virtual void doCollectAggregateResult(common::AggregateResultPtr &aggResultPtr) {
        assert(false);
    }
    virtual void estimateResult(double factor) = 0;
    virtual void estimateResult(
            double factor, BatchAggregateSampler& sampler) {
        assert(false);
    }
    virtual void stop() {
        _isStopped = true;
    }
    virtual void setFilter(Filter *filter) {
        assert(false);
    }
    virtual void setMatchDocAllocator(matchdoc::MatchDocAllocator *alloc) {
        _matchDocAllocator = alloc;
    }
    template<typename T2>
    const matchdoc::Reference<T2> *getFunResultReference(
            const std::string &funString) {
        assert(false);
        return NULL;
    }
    virtual void addAggregateFunction(AggregateFunction *fun) {
    }
    virtual void beginSample() = 0;
    virtual void endLayer(double factor) = 0;
    virtual std::string getName() {
        assert(false);
        return "baseAgg";
    }
    virtual uint32_t getAggregateCount() {
        return 0;
    }
    uint32_t getToAggDocCount() {
        return _toAggDocCount;
    }
    virtual void updateExprEvaluatedStatus() = 0;
    virtual suez::turing::CavaAggModuleInfoPtr codegen() {
        return suez::turing::CavaAggModuleInfoPtr();
    }
    virtual suez::turing::AttributeExpression *getGroupKeyExpr() {
        assert(false);
        return NULL;
    }
    virtual suez::turing::AttributeExpression *getAggFunctionExpr(uint index) {
        assert(false);
        return NULL;
    }
    virtual matchdoc::ReferenceBase *getAggFunctionRef(uint index, uint id) {
        assert(false);
        return NULL;
    }
    virtual matchdoc::ReferenceBase *getGroupKeyRef() {
        assert(false);
        return NULL;
    }
    virtual uint32_t getMaxSortCount() {
        assert(false);
        return 0;
    }
protected:
    autil::mem_pool::Pool *_pool;
    matchdoc::MatchDocAllocator *_matchDocAllocator;
private:
    bool _isStopped;
    double _factor;
protected:
    uint32_t _toAggDocCount;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(Aggregator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_AGGREGATOR_H
