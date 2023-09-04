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
#include "suez/turing/expression/framework/AttributeExpressionCreatorBase.h"

#include "suez/turing/expression/framework/OptimizeReCalcExpression.h"
#include "suez/turing/expression/framework/ReCalcFetcherVisitor.h"
#include "suez/turing/expression/framework/ReCalcStatsVisitor.h"
#include "suez/turing/expression/syntax/SyntaxParser.h"

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, AttributeExpressionCreatorBase);

AttributeExpressionCreatorBase::~AttributeExpressionCreatorBase() {
    for (size_t i = 0; i < _syntaxExprVec.size(); ++i) {
        DELETE_AND_SET_NULL(_syntaxExprVec[i]);
    }
    _syntaxExprVec.clear();

    DELETE_AND_SET_NULL(_exprPool);
}

SyntaxExpr *AttributeExpressionCreatorBase::parseSyntax(const std::string &exprStr) {
    return SyntaxParser::parseSyntax(exprStr);
}

AttributeExpression *AttributeExpressionCreatorBase::createAttributeExpression(const std::string &exprStr) {
    SyntaxExpr *syntaxExpr = parseSyntax(exprStr);
    if (!syntaxExpr) {
        AUTIL_LOG(WARN, "parse syntax [%s] failed", exprStr.c_str());
        return NULL;
    }
    AttributeExpression *ret = createAttributeExpression(syntaxExpr, true);
    delete syntaxExpr;
    return ret;
}

AttributeExpression *AttributeExpressionCreatorBase::createOptimizeReCalcExpression() {
    ReCalcStatsVisitor reCalcStatsVisitor;
    for (auto &syntaxExpr : _syntaxExprVec) {
        reCalcStatsVisitor.stat(syntaxExpr);
    }
    ReCalcFetcherVisitor reCalcFetcherVisitor(
        reCalcStatsVisitor.getCnt(), reCalcStatsVisitor.getNeedOptimize(), _exprPool);
    for (auto &syntaxExpr : _syntaxExprVec) {
        reCalcFetcherVisitor.fetch(syntaxExpr);
    }
    const ExpressionVector &attrExprs = reCalcFetcherVisitor.getAttrExprs();
    OptimizeReCalcExpression *expr = POOL_NEW_CLASS(_pool, OptimizeReCalcExpression, attrExprs);
    addNeedDeleteExpr(expr);
    std::vector<std::string> originStrs;
    for (const auto &attrExpr : attrExprs) {
        originStrs.emplace_back(attrExpr->getOriginalString());
    }
    expr->setOriginalString(autil::StringUtil::toString(originStrs, ","));
    return expr;
}

} // namespace turing
} // namespace suez
