/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <assert.h>
#include <stddef.h>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/StringUtil.h"
#include "ha3/isearch.h"
#include "ha3/search/AggregateFunction.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "sys/types.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
template <typename T> class MultiValueType;
}  // namespace autil
namespace matchdoc {
class MatchDocAllocator;
}  // namespace matchdoc

namespace isearch {
namespace search {

template <typename ResultType, typename ExprType = ResultType>
class AvgAggregateFunction : public AggregateFunction
{
public:
    AvgAggregateFunction(const std::string &parameter,
                         VariableType resultType,
                         suez::turing::AttributeExpressionTyped<ExprType> *attriExpr)
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
    suez::turing::AttributeExpressionTyped<ExprType> *_expr;
    matchdoc::Reference<ResultType> *_numRefer;
    matchdoc::Reference<ResultType> *_sumRefer;
    matchdoc::Reference<ResultType> *_avgRefer;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch

