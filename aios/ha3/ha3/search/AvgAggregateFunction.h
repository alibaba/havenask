#ifndef ISEARCH_AVGAGGREGATEFUNCTION_H
#define ISEARCH_AVGAGGREGATEFUNCTION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/AggregateFunction.h>
#include <ha3/search/AttributeExpression.h>
#include <autil/StringUtil.h>
#include <assert.h>
#include <iostream>
#include <autil/MultiValueType.h>

BEGIN_HA3_NAMESPACE(search);

template <typename ResultType, typename ExprType = ResultType>
class AvgAggregateFunction : public AggregateFunction
{
public:
    AvgAggregateFunction(const std::string &parameter,
                         VariableType resultType,
                         AttributeExpressionTyped<ExprType> *attriExpr)
        : AggregateFunction("avg", parameter, resultType)
    {
        _expr = attriExpr;
        _numRefer = NULL;
        _sumRefer = NULL;
    }
    ~AvgAggregateFunction() {
        _expr = NULL;
    }
public:
    void calculate(matchdoc::MatchDoc matchDoc,
                   matchdoc::MatchDoc aggMatchDoc) override
    {
        ExprType value = _expr->evaluateAndReturn(matchDoc);
        calculateAvgFun(value, aggMatchDoc);
    }
    void declareResultReference(matchdoc::MatchDocAllocator *allocator) override {
        _avgRefer = allocator->declareWithConstructFlagDefaultGroup<ResultType>(
                toString()+".avg",
                matchdoc::ConstructTypeTraits<ResultType>::NeedConstruct());
        _numRefer = allocator->declareWithConstructFlagDefaultGroup<ResultType>(
                toString()+".num",
                matchdoc::ConstructTypeTraits<ResultType>::NeedConstruct());
        _sumRefer = allocator->declareWithConstructFlagDefaultGroup<ResultType>(
                toString()+".sum",
                matchdoc::ConstructTypeTraits<ResultType>::NeedConstruct());

        assert(_numRefer);
        assert(_sumRefer);
        assert(_avgRefer);
    }
    void initFunction(matchdoc::MatchDoc aggMatchDoc, autil::mem_pool::Pool* pool = NULL) override {
        _numRefer->set(aggMatchDoc, 0);
        _sumRefer->set(aggMatchDoc, 0);
        _avgRefer->set(aggMatchDoc, 0);
    }
    suez::turing::AttributeExpression *getExpr() override {
        return _expr;
    }
    matchdoc::ReferenceBase *getReference(uint id = 0) const override {
        return _avgRefer;
    }
    std::string getStrValue(matchdoc::MatchDoc aggMatchDoc) const override {
    ResultType result = _avgRefer->get(aggMatchDoc);
    return autil::StringUtil::toString(result);
    }
private:
    void calculateAvgFun(const ResultType &attrExprValue, matchdoc::MatchDoc aggMatchDoc) {
    ResultType numValue = _numRefer->get(aggMatchDoc);
    ResultType sumValue = _sumRefer->get(aggMatchDoc);
    _sumRefer->set(aggMatchDoc, sumValue + attrExprValue);
    _numRefer->set(aggMatchDoc, numValue + 1);
    _avgRefer->set(aggMatchDoc, (sumValue + attrExprValue)/(numValue + 1));
    }

    void calculateAvgFun(const autil::MultiValueType<ResultType> &attrExprValue,
    matchdoc::MatchDoc aggMatchDoc)
    {
    ResultType numValue = _numRefer->get(aggMatchDoc);
    ResultType sumValue = _sumRefer->get(aggMatchDoc);

    for (size_t i = 0; i < attrExprValue.size(); i++) {
    sumValue += attrExprValue[i];
        }
        numValue += attrExprValue.size();

        _sumRefer->set(aggMatchDoc, sumValue);
        _numRefer->set(aggMatchDoc, numValue);
        _avgRefer->set(aggMatchDoc, sumValue/numValue);
    }

private:
    AttributeExpressionTyped<ExprType> *_expr;
    matchdoc::Reference<ResultType> *_numRefer;
    matchdoc::Reference<ResultType> *_sumRefer;
    matchdoc::Reference<ResultType> *_avgRefer;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_AVGAGGREGATEFUNCTION_H
