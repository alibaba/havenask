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
#include "expression/framework/BottomUpEvaluator.h"
#include "expression/framework/AttributeExpression.h"

using namespace std;
using namespace matchdoc;

#define DEFAULT_EVALUATE_EXPRESSION_BUFF_SIZE 32

namespace expression {
AUTIL_LOG_SETUP(expression, BottomUpEvaluator);

bool expressionLessFunc(const AttributeExpression* left,
                        const AttributeExpression* right)
{
    return left->getAttributeExpressionId() < 
        right->getAttributeExpressionId();
}

bool expressionEqualFunc(const AttributeExpression* left,
                         const AttributeExpression* right)
{
    return left->getAttributeExpressionId() == 
        right->getAttributeExpressionId();
}

void BottomUpEvaluator::init(const vector<AttributeExpression*> &exprs,
                             SubDocAccessor *subDocAccessor) {
    
    vector<AttributeExpression*> tmpExprs;
    tmpExprs.reserve(DEFAULT_EVALUATE_EXPRESSION_BUFF_SIZE);
    
    for (size_t i = 0; i < exprs.size(); i++) {
        if (!exprs[i]) {
            continue;
        }
        exprs[i]->getDependentAttributeExpressions(tmpExprs);
    }
    
    sort(tmpExprs.begin(), tmpExprs.end(), expressionLessFunc);
    vector<AttributeExpression*>::iterator iter = 
        unique(tmpExprs.begin(), tmpExprs.end(), expressionEqualFunc);
    tmpExprs.resize(distance(tmpExprs.begin(), iter));

    filterNoNeedEvaluateExpression(tmpExprs);
    _bottomUpExpressions.assign(tmpExprs.begin(), tmpExprs.end());
    _subDocAccessor = subDocAccessor;
}

void BottomUpEvaluator::evaluate(MatchDoc matchDoc) {
    for (size_t i = 0; i < _bottomUpExpressions.size(); ++i) {
        AttributeExpression *expr = _bottomUpExpressions[i];
        if (!expr->isSubExpression()) {
            _bottomUpExpressions[i]->evaluate(matchDoc);
        } else {
            evaluateForSubDocs(expr, matchDoc);
        }
    }
}

void BottomUpEvaluator::batchEvaluate(MatchDoc *matchDocs, uint32_t docCount) {
    for (size_t i = 0; i < _bottomUpExpressions.size(); ++i) {
        AttributeExpression *expr = _bottomUpExpressions[i];
        if (!expr->isSubExpression()) {
            expr->batchEvaluate(matchDocs, docCount);
        } else {
            batchEvaluateForSubDocs(expr, matchDocs, docCount);
        }
    }
}

void BottomUpEvaluator::getToEvaluateExprIds(vector<expressionid_t>& exprIds) const {
    for (size_t i = 0; i < _bottomUpExpressions.size(); ++i) {
        exprIds.push_back(_bottomUpExpressions[i]->getAttributeExpressionId());
    }
}

void BottomUpEvaluator::filterNoNeedEvaluateExpression(
        vector<AttributeExpression*> &exprs)
{
    uint32_t idx = 0;
    for (size_t i = 0; i < exprs.size(); i++) {
        if (exprs[i]->needEvaluate()) {
            expressionid_t exprId = exprs[i]->getAttributeExpressionId();
            if (find(_noNeedEvalExprIds.begin(), _noNeedEvalExprIds.end(), exprId)
                == _noNeedEvalExprIds.end())
            {
                exprs[idx++] = exprs[i];
            }
        }
    }
    exprs.resize(idx);
}

class SubDocExpressionProcessor {
public:
    SubDocExpressionProcessor(AttributeExpression *expr)
        : _expr(expr)
    {}
public:
    void operator()(matchdoc::MatchDoc matchDoc) {
        _expr->evaluate(matchDoc);
    }
private:
    AttributeExpression *_expr;
};

inline void BottomUpEvaluator::evaluateForSubDocs(
    AttributeExpression *expr, matchdoc::MatchDoc matchDoc)
{
    assert(_subDocAccessor != NULL);
    SubDocExpressionProcessor processor(expr);
    _subDocAccessor->foreach(matchDoc, processor);
}

void BottomUpEvaluator::batchEvaluateForSubDocs(
        AttributeExpression *expr, 
        matchdoc::MatchDoc *matchDocs, uint32_t docCount)
{
    assert(_subDocAccessor != NULL);
    for (uint32_t i = 0; i < docCount; ++i) {
        SubDocExpressionProcessor processor(expr);
        _subDocAccessor->foreach(matchDocs[i], processor);
    }
}

}
