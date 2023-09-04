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
#include "suez/turing/expression/framework/ReCalcFetcherVisitor.h"

#include <assert.h>
#include <map>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/cava/common/CavaFieldTypeHelper.h"
#include "suez/turing/expression/cava/common/CavaFunctionModuleInfo.h"
#include "suez/turing/expression/cava/common/CavaModuleInfo.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "suez/turing/expression/function/FunctionMap.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"
#include "suez/turing/expression/syntax/BinarySyntaxExpr.h"
#include "suez/turing/expression/syntax/FuncSyntaxExpr.h"
#include "suez/turing/expression/syntax/MultiSyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/util/AttributeInfos.h"
#include "suez/turing/expression/util/VirtualAttribute.h"

namespace suez {
namespace turing {
class RankSyntaxExpr;
} // namespace turing
} // namespace suez

using namespace std;
namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, ReCalcFetcherVisitor);

ReCalcFetcherVisitor::ReCalcFetcherVisitor(std::unordered_map<std::string, int32_t> *cnt,
                                           std::unordered_map<std::string, bool> *needOptimize,
                                           AttributeExpressionPool *exprPool)
    : _cnt(cnt), _needOptimize(needOptimize), _exprPool(exprPool) {}

ReCalcFetcherVisitor::~ReCalcFetcherVisitor() {}

void ReCalcFetcherVisitor::fetch(const SyntaxExpr *expr, const std::string &fatherExprStr) {
    const std::string &exprStr = expr->getExprString();
    if (_attrExprSet.find(exprStr) != _attrExprSet.end()) {
        return;
    }
    _attrExprSet.insert(exprStr);
    expr->accept(this);
    int fatherCnt = fatherExprStr.empty() ? 1 : (*_cnt)[fatherExprStr];
    if ((*_needOptimize)[exprStr] && fatherCnt < (*_cnt)[exprStr]) {
        auto attrExpr = _exprPool->tryGetAttributeExpression(exprStr);
        if (attrExpr) {
            _attrExprs.emplace_back(attrExpr);
        }
    }
}

void ReCalcFetcherVisitor::visitBinarySyntaxExpr(const BinarySyntaxExpr *syntaxExpr) {
    const string &exprStr = syntaxExpr->getExprString();
    const SyntaxExpr *leftExpr = syntaxExpr->getLeftExpr();
    const SyntaxExpr *rightExpr = syntaxExpr->getRightExpr();
    fetch(leftExpr, exprStr);
    fetch(rightExpr, exprStr);
}

void ReCalcFetcherVisitor::visitLeftSyntaxExpr(const BinarySyntaxExpr *syntaxExpr) {
    const string &exprStr = syntaxExpr->getExprString();
    const SyntaxExpr *leftExpr = syntaxExpr->getLeftExpr();
    fetch(leftExpr, exprStr);
}

void ReCalcFetcherVisitor::visitAtomicSyntaxExpr(const AtomicSyntaxExpr *syntaxExpr) {}

void ReCalcFetcherVisitor::visitAddSyntaxExpr(const AddSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcFetcherVisitor::visitMinusSyntaxExpr(const MinusSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcFetcherVisitor::visitMulSyntaxExpr(const MulSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcFetcherVisitor::visitDivSyntaxExpr(const DivSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcFetcherVisitor::visitPowerSyntaxExpr(const PowerSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcFetcherVisitor::visitLogSyntaxExpr(const LogSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcFetcherVisitor::visitEqualSyntaxExpr(const EqualSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcFetcherVisitor::visitNotEqualSyntaxExpr(const NotEqualSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcFetcherVisitor::visitLessSyntaxExpr(const LessSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcFetcherVisitor::visitGreaterSyntaxExpr(const GreaterSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcFetcherVisitor::visitLessEqualSyntaxExpr(const LessEqualSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcFetcherVisitor::visitGreaterEqualSyntaxExpr(const GreaterEqualSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcFetcherVisitor::visitAndSyntaxExpr(const AndSyntaxExpr *syntaxExpr) { visitLeftSyntaxExpr(syntaxExpr); }

void ReCalcFetcherVisitor::visitOrSyntaxExpr(const OrSyntaxExpr *syntaxExpr) { visitLeftSyntaxExpr(syntaxExpr); }

void ReCalcFetcherVisitor::visitMultiSyntaxExpr(const MultiSyntaxExpr *syntaxExpr) {
    const string &exprStr = syntaxExpr->getExprString();
    for (auto *expr : syntaxExpr->getSyntaxExprs()) {
        fetch(expr, exprStr);
    }
}

void ReCalcFetcherVisitor::visitBitAndSyntaxExpr(const BitAndSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcFetcherVisitor::visitBitXorSyntaxExpr(const BitXorSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcFetcherVisitor::visitBitOrSyntaxExpr(const BitOrSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcFetcherVisitor::visitRankSyntaxExpr(const RankSyntaxExpr *syntaxExpr) {}

void ReCalcFetcherVisitor::visitFuncSyntaxExpr(const FuncSyntaxExpr *syntaxExpr) {
    const string &exprStr = syntaxExpr->getExprString();
    for (auto *expr : syntaxExpr->getParam()) {
        fetch(expr, exprStr);
    }
}

} // namespace turing
} // namespace suez
