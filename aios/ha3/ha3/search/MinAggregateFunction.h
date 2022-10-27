#ifndef ISEARCH_MINAGGREGATEFUNCTION_H
#define ISEARCH_MINAGGREGATEFUNCTION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/NumericLimits.h>
#include <ha3/search/AggregateFunction.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <autil/StringUtil.h>
#include <assert.h>
#include <iostream>
#include <autil/MultiValueType.h>

BEGIN_HA3_NAMESPACE(search);

template <typename ResultType, typename ExprType = ResultType>
class MinAggregateFunction : public AggregateFunction
{
public:
    MinAggregateFunction(const std::string &parameter,
                         VariableType resultType,
                         suez::turing::AttributeExpressionTyped<ExprType> *attriExpr)
        : AggregateFunction("min", parameter, resultType)
    {
        _expr = attriExpr;
        _resultRefer = NULL;
    }
    ~MinAggregateFunction() {
        _expr = NULL;
    }
public:
    void calculate(matchdoc::MatchDoc matchDoc,
                   matchdoc::MatchDoc aggMatchDoc) override
    {
        ExprType exprValue = _expr->evaluateAndReturn(matchDoc);
        calculateMin(exprValue, aggMatchDoc);
    }
    void declareResultReference(matchdoc::MatchDocAllocator *allocator) override {
        _resultRefer = allocator->declareWithConstructFlagDefaultGroup<ResultType>(
                toString(),
                matchdoc::ConstructTypeTraits<ResultType>::NeedConstruct());
        assert(_resultRefer);
    }
    void initFunction(matchdoc::MatchDoc aggMatchDoc,
                      autil::mem_pool::Pool *pool = NULL) override {
        _resultRefer->set(aggMatchDoc, util::NumericLimits<ResultType>::max());
    }
    matchdoc::ReferenceBase *getReference(uint id = 0) const override {
        return _resultRefer;
    }
    std::string getStrValue(matchdoc::MatchDoc aggMatchDoc) const override {
        ResultType result = _resultRefer->get(aggMatchDoc);
        return autil::StringUtil::toString(result);
    }
    suez::turing::AttributeExpression *getExpr() override {
        return _expr;
    }
    bool canCodegen() override {
        if (_expr->getType() == vt_bool) {
            return false;
        }
        return true;
    }
private:
    void calculateMin(const ResultType &attrExprValue, matchdoc::MatchDoc aggMatchDoc) {
        ResultType minValue = _resultRefer->get(aggMatchDoc);
        if (attrExprValue < minValue) {
            _resultRefer->set(aggMatchDoc, attrExprValue);
        }
    }
    void calculateMin(const autil::MultiValueType<ResultType> &attrExprValue,
                      matchdoc::MatchDoc aggMatchDoc)
    {
        ResultType minValue = _resultRefer->get(aggMatchDoc);
        for (size_t i = 0; i < attrExprValue.size(); i++) {
            if (attrExprValue[i] < minValue) {
                minValue = attrExprValue[i];
            }
        }
        _resultRefer->set(aggMatchDoc, minValue);
    }
private:
    suez::turing::AttributeExpressionTyped<ExprType> *_expr;
    matchdoc::Reference<ResultType> *_resultRefer;
private:
    HA3_LOG_DECLARE();
};
HA3_LOG_SETUP_TEMPLATE_2(search, MinAggregateFunction, ResultType, ExprType);
END_HA3_NAMESPACE(search);

#endif //ISEARCH_MINAGGREGATEFUNCTION_H
