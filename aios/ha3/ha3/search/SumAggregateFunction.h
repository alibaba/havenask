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
#include <stdint.h>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/StringUtil.h"
#include "ha3/isearch.h"
#include "ha3/search/AggregateFunction.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"  // IWYU pragma: keep
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"
#include "suez/turing/expression/common.h"
#include "sys/types.h"

namespace suez {
namespace turing {
class AttributeExpression;
template <typename T> class AttributeExpressionTyped;
}  // namespace turing
}  // namespace suez

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
template <typename T> class MultiValueType;
}  // namespace autil

namespace isearch {
namespace search {

template <typename ResultType, typename ExprType = ResultType>
class SumAggregateFunction : public AggregateFunction
{
public:
    SumAggregateFunction(const std::string &parameter,
                         VariableType resultType,
                         suez::turing::AttributeExpressionTyped<ExprType> *attriExpr)
        : AggregateFunction("sum", parameter, resultType)
    {
        _expr = attriExpr;
        _totalResultRefer = NULL;
        _curLayerResultRefer = NULL;
        _curLayerSampleRefer = NULL;
    }
    ~SumAggregateFunction() {
        _expr = NULL;
        _totalResultRefer = NULL;
        _curLayerResultRefer = NULL;
        _curLayerSampleRefer = NULL;
    }
public:
    void calculate(matchdoc::MatchDoc matchDoc,
                   matchdoc::MatchDoc aggMatchDoc) override
    {
        ExprType value = _expr->evaluateAndReturn(matchDoc);
        calculateSum(value, aggMatchDoc);
    }
    void declareResultReference(matchdoc::MatchDocAllocator *allocator) override {
        _totalResultRefer =
            allocator->declareWithConstructFlagDefaultGroup<ResultType>(
                    toString(),
                    matchdoc::ConstructTypeTraits<ResultType>::NeedConstruct());
        _curLayerResultRefer =
            allocator->declareWithConstructFlagDefaultGroup<ResultType>(
                    toString() + ".layerResult",
                    matchdoc::ConstructTypeTraits<ResultType>::NeedConstruct());
        _curLayerSampleRefer =
            allocator->declareWithConstructFlagDefaultGroup<ResultType>(
                    toString() + ".layerSample",
                    matchdoc::ConstructTypeTraits<ResultType>::NeedConstruct());
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
    void estimateResult(double factor, matchdoc::MatchDoc aggMatchDoc) override;
    void beginSample(matchdoc::MatchDoc aggMatchDoc) override;
    void endLayer(uint32_t sampleStep, double factor,
                  matchdoc::MatchDoc aggMatchDoc) override;
    std::string getStrValue(matchdoc::MatchDoc aggMatchDoc) const override {
        ResultType result = _totalResultRefer->get(aggMatchDoc);
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
    void calculateSum(const ResultType &attrExprValue, matchdoc::MatchDoc aggMatchDoc) {
        ResultType &sumValue = _curLayerSampleRefer->getReference(aggMatchDoc);
        sumValue +=  attrExprValue;
    }
    void calculateSum(const autil::MultiValueType<ResultType> &attrExprValue,
                         matchdoc::MatchDoc aggMatchDoc)
    {
        ResultType &sumValue = _curLayerSampleRefer->getReference(aggMatchDoc);
        for (size_t i = 0; i < attrExprValue.size(); i++) {
            sumValue += attrExprValue[i];
        }
    }
private:
    suez::turing::AttributeExpressionTyped<ExprType> *_expr;
    matchdoc::Reference<ResultType> *_totalResultRefer;
    matchdoc::Reference<ResultType> *_curLayerResultRefer;
    matchdoc::Reference<ResultType> *_curLayerSampleRefer;
private:
    AUTIL_LOG_DECLARE();
};

template<typename ResultType, typename ExprType>
void SumAggregateFunction<ResultType, ExprType>::estimateResult(double factor,
        matchdoc::MatchDoc aggMatchDoc)
{
    ResultType &totalValue = _totalResultRefer->getReference(aggMatchDoc);
    totalValue = ResultType(totalValue * factor);
}

template<typename ResultType, typename ExprType>
void SumAggregateFunction<ResultType, ExprType>::beginSample(
        matchdoc::MatchDoc aggMatchDoc)
{
    ResultType &sampleValue = _curLayerSampleRefer->getReference(aggMatchDoc);
    ResultType &layerValue = _curLayerResultRefer->getReference(aggMatchDoc);
    layerValue = sampleValue;
    sampleValue = 0;
}

template<typename ResultType, typename ExprType>
void SumAggregateFunction<ResultType, ExprType>::endLayer(uint32_t sampleStep,
        double factor, matchdoc::MatchDoc aggMatchDoc)
{
    ResultType &sampleValue = _curLayerSampleRefer->getReference(aggMatchDoc);
    ResultType &layerValue = _curLayerResultRefer->getReference(aggMatchDoc);
    ResultType &totalValue = _totalResultRefer->getReference(aggMatchDoc);
    if (sampleStep == 1 && factor == 1.0) {
        // TODO. delete this branch, ut
        totalValue += layerValue + sampleValue;
    } else {
        totalValue += ResultType(factor * (layerValue + sampleValue * (ResultType)sampleStep));
    }
    sampleValue = 0;
    layerValue = 0;
}

} // namespace search
} // namespace isearch
