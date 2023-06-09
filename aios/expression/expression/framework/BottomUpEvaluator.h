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
#ifndef ISEARCH_EXPRESSION_BOTTOMUPEVALUATOR_H
#define ISEARCH_EXPRESSION_BOTTOMUPEVALUATOR_H

#include "matchdoc/MatchDoc.h"
#include "matchdoc/SubDocAccessor.h"
#include "expression/common.h"

namespace expression {

class AttributeExpression;

class BottomUpEvaluator
{
public:
    BottomUpEvaluator()
        : _subDocAccessor(NULL) {};
    BottomUpEvaluator(const std::vector<expressionid_t>& noNeedEvaluateExprIds)
        : _subDocAccessor(NULL)
        , _noNeedEvalExprIds(noNeedEvaluateExprIds)
    {}
    
    ~BottomUpEvaluator() {};
private:
    BottomUpEvaluator(const BottomUpEvaluator &);
    BottomUpEvaluator& operator=(const BottomUpEvaluator &);

public:
    void init(const std::vector<AttributeExpression*>& exprs,
              matchdoc::SubDocAccessor *subDocAccessor);
    void evaluate(matchdoc::MatchDoc matchDoc);
    void batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount);
    void getToEvaluateExprIds(std::vector<expressionid_t>& exprIds) const;

private:
    void filterNoNeedEvaluateExpression(
            std::vector<AttributeExpression*> &exprs);

    void evaluateForSubDocs(AttributeExpression *expr,
                            matchdoc::MatchDoc matchDoc);

    void batchEvaluateForSubDocs(AttributeExpression *expr,
                                 matchdoc::MatchDoc *matchDocs,
                                 uint32_t docCount);

private:
    std::vector<AttributeExpression*> _bottomUpExpressions;
    matchdoc::SubDocAccessor *_subDocAccessor;
    std::vector<expressionid_t> _noNeedEvalExprIds;    
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BottomUpEvaluator> BottomUpEvaluatorPtr;
}

#endif //ISEARCH_EXPRESSION_BOTTOMUPEVALUATOR_H
