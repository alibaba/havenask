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

#include <ext/alloc_traits.h>
#include <stddef.h>
#include <memory>
#include <string>
#include <vector>

#include "ha3/sql/ops/condition/Condition.h"
#include "ha3/sql/ops/condition/ConditionVisitor.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/MatchDocsExpressionCreator.h"

namespace isearch {
namespace sql {

class CalcConditionVisitor : public ConditionVisitor {
public:
    CalcConditionVisitor(suez::turing::MatchDocsExpressionCreator *attrExprCreator);
    ~CalcConditionVisitor();

public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;

public:
    suez::turing::AttributeExpression *stealAttributeExpression();
    const suez::turing::AttributeExpression *getAttributeExpresson() const;

private:
    template <class T>
    void visitAndOrCondition(Condition *condition, const std::string opType);

private:
    suez::turing::AttributeExpression *_attrExpr = nullptr;
    suez::turing::MatchDocsExpressionCreator *_attrExprCreator = nullptr;
};

typedef std::shared_ptr<CalcConditionVisitor> CalcConditionVisitorPtr;
template <class T>
void CalcConditionVisitor::visitAndOrCondition(Condition *condition, const std::string opType) {
    if (isError()) {
        return;
    }
    const std::vector<ConditionPtr> &children = condition->getChildCondition();
    std::vector<suez::turing::AttributeExpression *> childExpr;
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
        if (getAttributeExpresson() != NULL) {
            childExpr.push_back(stealAttributeExpression());
        }
        if (isError()) {
            return;
        }
    }
    if (childExpr.size() == 1) {
        _attrExpr = childExpr[0];
    } else if (childExpr.size() > 1) {
        suez::turing::AttributeExpression *leftExpr = childExpr[0];
        suez::turing::AttributeExpression *rightExpr = nullptr;
        for (size_t i = 1; i < childExpr.size(); i++) {
            rightExpr = childExpr[i];
            std::string oriString = "(" + leftExpr->getOriginalString() + opType
                                    + rightExpr->getOriginalString() + ")";
            leftExpr = POOL_NEW_CLASS(_attrExprCreator->getPool(), T, leftExpr, rightExpr);
            leftExpr->setOriginalString(oriString);
            _attrExprCreator->addNeedDeleteExpr(leftExpr);
        }
        _attrExpr = leftExpr;
    }
}

} // namespace sql
} // namespace isearch
