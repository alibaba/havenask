#ifndef ISEARCH_COUNTAGGREGATEFUNCTION_H
#define ISEARCH_COUNTAGGREGATEFUNCTION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/AggregateFunction.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <assert.h>

BEGIN_HA3_NAMESPACE(search);
class CountAggregateFunction : public AggregateFunction
{
public:
    CountAggregateFunction()
        : AggregateFunction("count", "", vt_int64)
    {
        _totalResultRefer = NULL;
        _curLayerResultRefer = NULL;
        _curLayerSampleRefer = NULL;
    }

    ~CountAggregateFunction() {
        _totalResultRefer = NULL;
        _curLayerResultRefer = NULL;
        _curLayerSampleRefer = NULL;
    }
public:
    void calculate(matchdoc::MatchDoc matchDoc, matchdoc::MatchDoc aggMatchDoc) override {
        int64_t &countValue = _curLayerSampleRefer->getReference(aggMatchDoc);
        countValue++;
    }
    void declareResultReference(matchdoc::MatchDocAllocator *allocator) override {
        _totalResultRefer = allocator->declareWithConstructFlagDefaultGroup<int64_t>(
                "count", false);
        _curLayerResultRefer =
            allocator->declareWithConstructFlagDefaultGroup<int64_t>(
                    toString() + ".curLayerResult", false);
        _curLayerSampleRefer =
            allocator->declareWithConstructFlagDefaultGroup<int64_t>(
                    toString() + ".curLayerSample", false);
        assert(_totalResultRefer);
        assert(_curLayerResultRefer);
        assert(_curLayerSampleRefer);
    }
    void initFunction(matchdoc::MatchDoc aggMatchDoc,
                      autil::mem_pool::Pool *pool = NULL) override {
        _totalResultRefer->set(aggMatchDoc, 0);
        _curLayerResultRefer->set(aggMatchDoc, 0);
        _curLayerSampleRefer->set(aggMatchDoc, 0);
    }
    matchdoc::ReferenceBase *getReference(uint id = 0) const override {
        return _totalResultRefer;
    }
    void estimateResult(double factor, matchdoc::MatchDoc aggMatchDoc) override {
        int64_t &countValue = _totalResultRefer->getReference(aggMatchDoc);
        countValue = int64_t(countValue * factor);
    }
    void beginSample(matchdoc::MatchDoc aggMatchDoc) override {
        int64_t &sampleValue = _curLayerSampleRefer->getReference(aggMatchDoc);
        int64_t &layerValue = _curLayerResultRefer->getReference(aggMatchDoc);
        layerValue = sampleValue;
        sampleValue = 0;
    }
    void endLayer(uint32_t sampleStep, double factor,
                  matchdoc::MatchDoc aggMatchDoc) override
    {
        int64_t &sampleValue = _curLayerSampleRefer->getReference(aggMatchDoc);
        int64_t &layerValue = _curLayerResultRefer->getReference(aggMatchDoc);
        int64_t &totalValue = _totalResultRefer->getReference(aggMatchDoc);
        totalValue += int64_t(factor * (layerValue + sampleValue * sampleStep));
        sampleValue = 0;
        layerValue = 0;
    }
    std::string getStrValue(matchdoc::MatchDoc aggMatchDoc) const override {
        int64_t result = _totalResultRefer->get(aggMatchDoc);
        return autil::StringUtil::toString(result);
    }
    suez::turing::AttributeExpression *getExpr() override {
        return NULL;
    }
    bool canCodegen() override {
        return true;
    }
private:
    matchdoc::Reference<int64_t> *_totalResultRefer;
    matchdoc::Reference<int64_t> *_curLayerResultRefer;
    matchdoc::Reference<int64_t> *_curLayerSampleRefer;
private:
    HA3_LOG_DECLARE();
};


END_HA3_NAMESPACE(search);

#endif //ISEARCH_COUNTAGGREGATEFUNCTION_H
