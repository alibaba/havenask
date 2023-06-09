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

#include <stddef.h>
#include <string>
#include <vector>

#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"

#include "autil/Log.h" // IWYU pragma: keep

namespace suez {
namespace turing {
class AddSyntaxExpr;
class AndSyntaxExpr;
class BitAndSyntaxExpr;
class BitOrSyntaxExpr;
class BitXorSyntaxExpr;
class DivSyntaxExpr;
class EqualSyntaxExpr;
class FuncSyntaxExpr;
class GreaterEqualSyntaxExpr;
class GreaterSyntaxExpr;
class LessEqualSyntaxExpr;
class LessSyntaxExpr;
class MinusSyntaxExpr;
class MulSyntaxExpr;
class MultiSyntaxExpr;
class NotEqualSyntaxExpr;
class OrSyntaxExpr;
class SyntaxExpr;

class SyntaxExprParser
{
public:
    SyntaxExprParser();
    ~SyntaxExprParser();
public:
    AndSyntaxExpr* createAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    OrSyntaxExpr* createOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    EqualSyntaxExpr* createEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    NotEqualSyntaxExpr* createNotEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    LessSyntaxExpr* createLessExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    LessEqualSyntaxExpr* createLessEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    GreaterSyntaxExpr* createGreaterExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    GreaterEqualSyntaxExpr* createGreaterEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    AtomicSyntaxExpr* createAtomicExpr(std::string* expr, AtomicSyntaxExprType valueType, bool isPositive = true);

    AddSyntaxExpr* createAddExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    MinusSyntaxExpr* createMinusExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    MulSyntaxExpr* createMulExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    DivSyntaxExpr* createDivExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    BitOrSyntaxExpr* createBitOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    BitXorSyntaxExpr* createBitXorExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    BitAndSyntaxExpr* createBitAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    FuncSyntaxExpr* createFuncExpr(std::string *funcName,
                                   const std::vector<SyntaxExpr*> *param = NULL);
    MultiSyntaxExpr *createMultiSyntax();
private:
    bool isBooleanExpr(SyntaxExpr *expr) const;
private:
    AUTIL_LOG_DECLARE();
};

}
}

