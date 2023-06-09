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

#include "autil/StringUtil.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Trait.h"
#include "suez/turing/expression/common.h"
#include "sys/types.h"

#include "ha3/isearch.h"
#include "ha3/search/AggregateFunction.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/util/NumericLimits.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
template <typename T> class MultiValueType;
}  // namespace autil
namespace matchdoc {
class MatchDocAllocator;
class ReferenceBase;
template <typename T> class Reference;
}  // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpression;
template <typename T> class AttributeExpressionTyped;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {
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
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE_2(ha3, MaxAggregateFunction, ResultType, ExprType);
} // namespace search
} // namespace isearch
