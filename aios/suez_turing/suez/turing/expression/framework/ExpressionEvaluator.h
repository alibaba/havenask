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

#include "autil/Log.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/SubDocAccessor.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

namespace suez {
namespace turing {

class SubDocExpressionProcessor {
public:
    SubDocExpressionProcessor(AttributeExpression *expr) : _expr(expr) {}

public:
    void operator()(matchdoc::MatchDoc matchDoc) { _expr->evaluate(matchDoc); }

private:
    AttributeExpression *_expr;
};

template <typename ExpressionContainer>
class ExpressionEvaluator {
public:
    ExpressionEvaluator(const ExpressionContainer &exprs, matchdoc::SubDocAccessor *subDocAccessor)
        : _exprs(exprs), _subDocAccessor(subDocAccessor) {}

private:
    ExpressionEvaluator(const ExpressionEvaluator &);
    ExpressionEvaluator &operator=(const ExpressionEvaluator &);

public:
    void evaluateExpressions(matchdoc::MatchDoc matchDoc);
    void batchEvaluateExpressions(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount);
    void batchEvaluated(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount);

private:
    void evaluateExpressionForSubDocs(AttributeExpression *expr, matchdoc::MatchDoc matchDoc);

private:
    const ExpressionContainer &_exprs;
    matchdoc::SubDocAccessor *_subDocAccessor;

private:
    AUTIL_LOG_DECLARE();
};

template <typename ExpressionContainer>
inline void ExpressionEvaluator<ExpressionContainer>::evaluateExpressions(matchdoc::MatchDoc matchDoc) {
    typename ExpressionContainer::const_iterator it = _exprs.begin();
    for (; it != _exprs.end(); it++) {
        AttributeExpression *expr = *it;
        if (!expr->isSubExpression()) {
            expr->evaluate(matchDoc);
        } else {
            evaluateExpressionForSubDocs(expr, matchDoc);
        }
    }
}

template <typename ExpressionContainer>
inline void ExpressionEvaluator<ExpressionContainer>::batchEvaluateExpressions(matchdoc::MatchDoc *matchDocs,
                                                                               uint32_t matchDocCount) {
    typename ExpressionContainer::const_iterator it = _exprs.begin();
    for (; it != _exprs.end(); it++) {
        AttributeExpression *expr = *it;
        if (!expr->isSubExpression()) {
            expr->batchEvaluate(matchDocs, matchDocCount);
        } else {
            assert(expr->getExpressionType() != ET_COMBO);
            for (size_t i = 0; i < matchDocCount; i++) {
                evaluateExpressionForSubDocs(expr, matchDocs[i]);
            }
        }
    }
}

template <typename ExpressionContainer>
inline void ExpressionEvaluator<ExpressionContainer>::batchEvaluated(matchdoc::MatchDoc *matchDocs,
                                                                     uint32_t matchDocCount) {
    typename ExpressionContainer::const_iterator it = _exprs.begin();
    for (; it != _exprs.end(); it++) {
        AttributeExpression *expr = *it;
        if (!expr->isSubExpression()) {
            expr->batchEvaluate(matchDocs, matchDocCount);
        } else {
            assert(expr->getExpressionType() != ET_COMBO);
            for (size_t i = 0; i < matchDocCount; i++) {
                evaluateExpressionForSubDocs(expr, matchDocs[i]);
            }
        }
        expr->setEvaluated();
    }
}

template <typename ExpressionContainer>
inline void ExpressionEvaluator<ExpressionContainer>::evaluateExpressionForSubDocs(AttributeExpression *expr,
                                                                                   matchdoc::MatchDoc matchDoc) {
    assert(_subDocAccessor != NULL);
    SubDocExpressionProcessor processor(expr);
    _subDocAccessor->foreach (matchDoc, processor);
}

} // namespace turing
} // namespace suez
