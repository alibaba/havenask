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

#include "autil/Log.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"
#include "suez/turing/expression/syntax/BinarySyntaxExpr.h"
#include "suez/turing/expression/syntax/ConditionalSyntaxExpr.h"
#include "suez/turing/expression/syntax/FuncSyntaxExpr.h"
#include "suez/turing/expression/syntax/MultiSyntaxExpr.h"
#include "suez/turing/expression/syntax/RankSyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

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
class ConditionalSyntaxExpr;
class LogSyntaxExpr;
class MinusSyntaxExpr;
class MulSyntaxExpr;
class NotEqualSyntaxExpr;
class OrSyntaxExpr;
class PowerSyntaxExpr;
class RankSyntaxExpr;
class SyntaxExpr;

class SyntaxExprFactory {
public:
    static AndSyntaxExpr *createAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static OrSyntaxExpr *createOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    static EqualSyntaxExpr *createEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static NotEqualSyntaxExpr *createNotEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    static LessSyntaxExpr *createLessExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static LessEqualSyntaxExpr *createLessEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    static GreaterSyntaxExpr *createGreaterExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static GreaterEqualSyntaxExpr *createGreaterEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    static AtomicSyntaxExpr *createAtomicExpr(std::string *expr,
                                              AtomicSyntaxExprType valueType,
                                              bool isPositive = true,
                                              std::string *prefix = nullptr);

    static AddSyntaxExpr *createAddExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static MinusSyntaxExpr *createMinusExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static MulSyntaxExpr *createMulExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static DivSyntaxExpr *createDivExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static PowerSyntaxExpr *createPowerExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static LogSyntaxExpr *createLogExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    static BitOrSyntaxExpr *createBitOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static BitXorSyntaxExpr *createBitXorExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static BitAndSyntaxExpr *createBitAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    static ConditionalSyntaxExpr *
    createConditionalExpr(SyntaxExpr *conditionalExpr, SyntaxExpr *trueExpr, SyntaxExpr *falseExpr);

    static FuncSyntaxExpr *createFuncExpr(std::string *funcName, const std::vector<SyntaxExpr *> *param = NULL);
    static RankSyntaxExpr *createRankExpr();

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
