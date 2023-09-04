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
#include "suez/turing/expression/syntax/SyntaxExprFactory.h"

#include <assert.h>
#include <sstream>
#include <stddef.h>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"
#include "suez/turing/expression/syntax/BinarySyntaxExpr.h"
#include "suez/turing/expression/syntax/ConditionalSyntaxExpr.h"
#include "suez/turing/expression/syntax/FuncSyntaxExpr.h"
#include "suez/turing/expression/syntax/RankSyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, SyntaxExprFactory);

AndSyntaxExpr *SyntaxExprFactory::createAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    AndSyntaxExpr *ret = new AndSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createAndExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

OrSyntaxExpr *SyntaxExprFactory::createOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    OrSyntaxExpr *ret = new OrSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createOrExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

EqualSyntaxExpr *SyntaxExprFactory::createEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    EqualSyntaxExpr *ret = new EqualSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createEqualExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

LessSyntaxExpr *SyntaxExprFactory::createLessExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    LessSyntaxExpr *ret = new LessSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createLessExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

GreaterSyntaxExpr *SyntaxExprFactory::createGreaterExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    GreaterSyntaxExpr *ret = new GreaterSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createGreaterExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

LessEqualSyntaxExpr *SyntaxExprFactory::createLessEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    LessEqualSyntaxExpr *ret = new LessEqualSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createLessEqualExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

GreaterEqualSyntaxExpr *SyntaxExprFactory::createGreaterEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    GreaterEqualSyntaxExpr *ret = new GreaterEqualSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createGreaterEqualExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

NotEqualSyntaxExpr *SyntaxExprFactory::createNotEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    NotEqualSyntaxExpr *ret = new NotEqualSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createNotEqualExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

AtomicSyntaxExpr *SyntaxExprFactory::createAtomicExpr(string *expr,
                                                      AtomicSyntaxExprType atomicSyntaxExprType,
                                                      bool isPositive,
                                                      string *prefix) {
    assert(expr);
    const string &exprStr = isPositive ? *expr : ("-" + *expr);
    AtomicSyntaxExpr *ret = new AtomicSyntaxExpr(exprStr, vt_unknown, atomicSyntaxExprType, prefix ? *prefix : "");
    AUTIL_LOG(
        TRACE2, "createAtomicExpr: %s %s, ret(%p)", (prefix ? (*prefix + ".").c_str() : ""), exprStr.c_str(), ret);
    delete expr;
    if (prefix) {
        delete prefix;
    }
    return ret;
}

AddSyntaxExpr *SyntaxExprFactory::createAddExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    AddSyntaxExpr *ret = new AddSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    AUTIL_LOG(TRACE2, "createAddExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

MinusSyntaxExpr *SyntaxExprFactory::createMinusExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    MinusSyntaxExpr *ret = new MinusSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    AUTIL_LOG(TRACE2, "createMinusExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

MulSyntaxExpr *SyntaxExprFactory::createMulExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    MulSyntaxExpr *ret = new MulSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    AUTIL_LOG(TRACE2, "createMulExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

DivSyntaxExpr *SyntaxExprFactory::createDivExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    DivSyntaxExpr *ret = new DivSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    AUTIL_LOG(TRACE2, "createDivExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

PowerSyntaxExpr *SyntaxExprFactory::createPowerExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    PowerSyntaxExpr *ret = new PowerSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    AUTIL_LOG(TRACE2, "createPowerExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

LogSyntaxExpr *SyntaxExprFactory::createLogExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    LogSyntaxExpr *ret = new LogSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprB->getExprResultType());
    AUTIL_LOG(TRACE2, "createLogExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

BitOrSyntaxExpr *SyntaxExprFactory::createBitOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    BitOrSyntaxExpr *ret = new BitOrSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    AUTIL_LOG(TRACE2, "createBitOrExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

BitXorSyntaxExpr *SyntaxExprFactory::createBitXorExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    BitXorSyntaxExpr *ret = new BitXorSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    AUTIL_LOG(TRACE2, "createBitXorExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

BitAndSyntaxExpr *SyntaxExprFactory::createBitAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    BitAndSyntaxExpr *ret = new BitAndSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    AUTIL_LOG(TRACE2, "createBitAndExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

ConditionalSyntaxExpr *
SyntaxExprFactory::createConditionalExpr(SyntaxExpr *conditionalExpr, SyntaxExpr *trueExpr, SyntaxExpr *falseExpr) {
    ConditionalSyntaxExpr *ret = new ConditionalSyntaxExpr(conditionalExpr, trueExpr, falseExpr);
    ret->setExprResultType(trueExpr->getExprResultType());
    AUTIL_LOG(TRACE2, "createConditionalSyntaxExpr(%p, %p, %p), ret(%p)", conditionalExpr, trueExpr, falseExpr, ret);
    return ret;
}

FuncSyntaxExpr *SyntaxExprFactory::createFuncExpr(std::string *funcName, const std::vector<SyntaxExpr *> *param) {
    assert(funcName);
    FuncSyntaxExpr *ret = NULL;
    if (!param) {
        ret = new FuncSyntaxExpr(*funcName, vector<SyntaxExpr *>());
    } else {
        ret = new FuncSyntaxExpr(*funcName, *param);
    }
    AUTIL_LOG(TRACE2, "createFuncExpr(%s), ret(%p)", funcName->c_str(), ret);
    delete funcName;
    delete param;
    return ret;
}

RankSyntaxExpr *SyntaxExprFactory::createRankExpr() {
    auto ret = new RankSyntaxExpr();
    AUTIL_LOG(TRACE2, "createRankExpr ret(%p)", ret);
    return ret;
}

} // namespace turing
} // namespace suez
