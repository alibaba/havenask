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
#include "ha3/sql/ops/calc/CalcConditionVisitor.h"

#include <assert.h>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "ha3/sql/ops/condition/NotExpressionWrapper.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/BinaryAttributeExpression.h"

using namespace std;
using namespace autil;
using namespace suez::turing;

namespace isearch {
namespace sql {

CalcConditionVisitor::CalcConditionVisitor(
    suez::turing::MatchDocsExpressionCreator *attrExprCreator)
    : _attrExprCreator(attrExprCreator) {}

CalcConditionVisitor::~CalcConditionVisitor() {
    _attrExpr = nullptr; // delete by attribute expression pool
}

void CalcConditionVisitor::visitAndCondition(AndCondition *condition) {
    typedef BinaryAttributeExpression<std::logical_and, bool> AndAttrExpr;
    visitAndOrCondition<AndAttrExpr>(condition, " AND ");
}

void CalcConditionVisitor::visitOrCondition(OrCondition *condition) {
    typedef BinaryAttributeExpression<std::logical_or, bool> OrAttrExpr;
    visitAndOrCondition<OrAttrExpr>(condition, " OR ");
}

void CalcConditionVisitor::visitNotCondition(NotCondition *condition) {
    CHECK_ERROR_AND_RETRUN();
    const vector<ConditionPtr> &children = condition->getChildCondition();
    assert(children.size() == 1);
    children[0]->accept(this);
    if (isError()) {
        return;
    }
    AttributeExpression *childExpr = stealAttributeExpression();
    if (childExpr != nullptr) {
        typedef AttributeExpressionTyped<bool> BoolAttrExpr;
        BoolAttrExpr *childExprTyped = dynamic_cast<BoolAttrExpr *>(childExpr);
        if (childExprTyped == nullptr) {
            string errorInfo = "dynamic cast expr[" + childExpr->getOriginalString()
                               + "] to bool expression failed.";
            setErrorInfo(errorInfo);
            return;
        }
        _attrExpr = POOL_NEW_CLASS(_attrExprCreator->getPool(), NotExpressionWrapper, childExprTyped);
        string oriString = "not (" + childExprTyped->getOriginalString() + ")";
        _attrExpr->setOriginalString(oriString);
        _attrExprCreator->addNeedDeleteExpr(_attrExpr);
    }
}

void CalcConditionVisitor::visitLeafCondition(LeafCondition *condition) {
    CHECK_ERROR_AND_RETRUN();
    const SimpleValue &leafCondition = condition->getCondition();
    if (ExprUtil::isCaseOp(leafCondition)) {
        bool hasError = false;
        string errorInfo;
        SQL_LOG(TRACE1, "create leaf exprStr [case when]");
        _attrExpr = ExprUtil::CreateCaseExpression(
            _attrExprCreator, "CONDITION_BOOLEAN", leafCondition, hasError, errorInfo, _attrExprCreator->getPool());
        if (!_attrExpr) {
            SQL_LOG(WARN, "%s", errorInfo.c_str());
            setErrorInfo(errorInfo);
        }
    } else {
        string exprStr = "";
        if (!ExprUtil::createExprString(leafCondition, exprStr) || exprStr.empty()) {
            setErrorInfo("create expr string failed");
            return;
        }
        SQL_LOG(TRACE1, "create leaf exprStr [%s]", exprStr.c_str());
        _attrExpr = _attrExprCreator->createAttributeExpression(exprStr);
        if (!_attrExpr) {
            string errorInfo = "parse expression string [" + exprStr + "] failed.";
            SQL_LOG(WARN, "%s", errorInfo.c_str());
            setErrorInfo(errorInfo);
        }
    }
}

suez::turing::AttributeExpression *CalcConditionVisitor::stealAttributeExpression() {
    suez::turing::AttributeExpression *attrExpr = _attrExpr;
    _attrExpr = nullptr;
    return attrExpr;
}

const suez::turing::AttributeExpression *CalcConditionVisitor::getAttributeExpresson() const {
    return _attrExpr;
}

} // namespace sql
} // namespace isearch
