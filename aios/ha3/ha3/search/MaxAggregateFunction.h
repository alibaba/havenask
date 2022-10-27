#ifndef ISEARCH_MAXAGGREGATEFUNCTION_H
#define ISEARCH_MAXAGGREGATEFUNCTION_H

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
class MaxAggregateFunction : public AggregateFunction
{
public:
    MaxAggregateFunction(const std::string &parameter,
                         VariableType resultType,
                         suez::turing::AttributeExpressionTyped<ExprType> *attriExpr)
        : AggregateFunction("max", parameter, resultType)
    {
        _expr = attriExpr;
        _resultRefer = NULL;
    }
    ~MaxAggregateFunction() {
        _expr = NULL;
    }
public:
    void calculate(matchdoc::MatchDoc matchDoc,
                   matchdoc::MatchDoc aggMatchDoc) override
    {
        ExprType exprValue = _expr->evaluateAndReturn(matchDoc);
        calculateMax(exprValue, aggMatchDoc);
    }
    void declareResultReference(matchdoc::MatchDocAllocator *allocator) override {
        _resultRefer = allocator->declareWithConstructFlagDefaultGroup<ResultType>(
                toString(),
                matchdoc::ConstructTypeTraits<ResultType>::NeedConstruct());
        assert(_resultRefer);
    }
    void initFunction(matchdoc::MatchDoc aggMatchDoc,
                      autil::mem_pool::Pool *pool = NULL) override {
        _resultRefer->set(aggMatchDoc, util::NumericLimits<ResultType>::min());
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
    void calculateMax(const ResultType &attrExprValue,
                      matchdoc::MatchDoc aggMatchDoc)
    {
        ResultType maxValue = _resultRefer->get(aggMatchDoc);

        if (attrExprValue > maxValue) {
            _resultRefer->set(aggMatchDoc, attrExprValue);
        }
    }

    void calculateMax(const autil::MultiValueType<ResultType> &attrExprValue,
                      matchdoc::MatchDoc aggMatchDoc)
    {
        ResultType maxValue = _resultRefer->get(aggMatchDoc);
        for (size_t i = 0; i < attrExprValue.size(); i++) {
            if (attrExprValue[i] > maxValue) {
                maxValue = attrExprValue[i];
            }
        }

        _resultRefer->set(aggMatchDoc, maxValue);
    }
private:
    suez::turing::AttributeExpressionTyped<ExprType> *_expr;
    matchdoc::Reference<ResultType> *_resultRefer;
private:
    HA3_LOG_DECLARE();
};
HA3_LOG_SETUP_TEMPLATE_2(search, MaxAggregateFunction, ResultType, ExprType);
END_HA3_NAMESPACE(search);

#endif //ISEARCH_MAXAGGREGATEFUNCTION_H
