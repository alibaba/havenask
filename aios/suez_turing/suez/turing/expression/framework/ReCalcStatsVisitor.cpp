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
#include "suez/turing/expression/framework/ReCalcStatsVisitor.h"

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

AUTIL_LOG_SETUP(expression, ReCalcStatsVisitor);

ReCalcStatsVisitor::ReCalcStatsVisitor() {}

ReCalcStatsVisitor::~ReCalcStatsVisitor() {}

void ReCalcStatsVisitor::stat(const SyntaxExpr *expr) {
    const std::string &exprStr = expr->getExprString();
    if (_cnt.find(exprStr) != _cnt.end()) {
        ++_cnt[exprStr];
    } else {
        _cnt[exprStr] = 1;
        _needOptimize[exprStr] = false;
    }
    expr->accept(this);
}

void ReCalcStatsVisitor::visitBinarySyntaxExpr(const BinarySyntaxExpr *syntaxExpr) {
    const string &exprStr = syntaxExpr->getExprString();
    const SyntaxExpr *leftExpr = syntaxExpr->getLeftExpr();
    const SyntaxExpr *rightExpr = syntaxExpr->getRightExpr();
    stat(leftExpr);
    stat(rightExpr);
    _needOptimize[exprStr] |= _needOptimize[leftExpr->getExprString()];
    _needOptimize[exprStr] |= _needOptimize[rightExpr->getExprString()];
}

void ReCalcStatsVisitor::visitLeftSyntaxExpr(const BinarySyntaxExpr *syntaxExpr) {
    const string &exprStr = syntaxExpr->getExprString();
    const SyntaxExpr *leftExpr = syntaxExpr->getLeftExpr();
    stat(leftExpr);
    _needOptimize[exprStr] |= _needOptimize[leftExpr->getExprString()];
}

void ReCalcStatsVisitor::visitAtomicSyntaxExpr(const AtomicSyntaxExpr *syntaxExpr) {}

void ReCalcStatsVisitor::visitAddSyntaxExpr(const AddSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcStatsVisitor::visitMinusSyntaxExpr(const MinusSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcStatsVisitor::visitMulSyntaxExpr(const MulSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcStatsVisitor::visitDivSyntaxExpr(const DivSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcStatsVisitor::visitPowerSyntaxExpr(const PowerSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcStatsVisitor::visitLogSyntaxExpr(const LogSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcStatsVisitor::visitEqualSyntaxExpr(const EqualSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcStatsVisitor::visitNotEqualSyntaxExpr(const NotEqualSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcStatsVisitor::visitLessSyntaxExpr(const LessSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcStatsVisitor::visitGreaterSyntaxExpr(const GreaterSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcStatsVisitor::visitLessEqualSyntaxExpr(const LessEqualSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcStatsVisitor::visitGreaterEqualSyntaxExpr(const GreaterEqualSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcStatsVisitor::visitAndSyntaxExpr(const AndSyntaxExpr *syntaxExpr) { visitLeftSyntaxExpr(syntaxExpr); }

void ReCalcStatsVisitor::visitOrSyntaxExpr(const OrSyntaxExpr *syntaxExpr) { visitLeftSyntaxExpr(syntaxExpr); }

void ReCalcStatsVisitor::visitMultiSyntaxExpr(const MultiSyntaxExpr *syntaxExpr) {
    const string &exprStr = syntaxExpr->getExprString();
    for (auto *expr : syntaxExpr->getSyntaxExprs()) {
        stat(expr);
        _needOptimize[exprStr] |= _needOptimize[expr->getExprString()];
    }
}

void ReCalcStatsVisitor::visitBitAndSyntaxExpr(const BitAndSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcStatsVisitor::visitBitXorSyntaxExpr(const BitXorSyntaxExpr *syntaxExpr) {
    visitBinarySyntaxExpr(syntaxExpr);
}

void ReCalcStatsVisitor::visitBitOrSyntaxExpr(const BitOrSyntaxExpr *syntaxExpr) { visitBinarySyntaxExpr(syntaxExpr); }

void ReCalcStatsVisitor::visitRankSyntaxExpr(const RankSyntaxExpr *syntaxExpr) {}

void ReCalcStatsVisitor::visitFuncSyntaxExpr(const FuncSyntaxExpr *syntaxExpr) {
    const string &exprStr = syntaxExpr->getExprString();
    _needOptimize[exprStr] = true;
    for (auto *expr : syntaxExpr->getParam()) {
        stat(expr);
    }
}

} // namespace turing
} // namespace suez
