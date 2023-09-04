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
#include "suez/turing/expression/syntax/SyntaxExpr.h"

#include <assert.h>
#include <vector>

#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"
#include "suez/turing/expression/syntax/BinarySyntaxExpr.h"
#include "suez/turing/expression/syntax/ConditionalSyntaxExpr.h"
#include "suez/turing/expression/syntax/FuncSyntaxExpr.h"
#include "suez/turing/expression/syntax/MultiSyntaxExpr.h"
#include "suez/turing/expression/syntax/RankSyntaxExpr.h"

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, SyntaxExpr);

void SyntaxExpr::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_isMultiValue);
    dataBuffer.write(_isSubExpression);
    dataBuffer.write(_syntaxType);
    dataBuffer.write(_resultType);
    dataBuffer.write(_exprString);
}

void SyntaxExpr::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_isMultiValue);
    dataBuffer.read(_isSubExpression);
    dataBuffer.read(_syntaxType);
    dataBuffer.read(_resultType);
    dataBuffer.read(_exprString);
}

SyntaxExpr *SyntaxExpr::createSyntaxExpr(SyntaxExprType type) {
    SyntaxExpr *t = NULL;
    switch (type) {
    case SYNTAX_EXPR_TYPE_ATOMIC_ATTR:
    case SYNTAX_EXPR_TYPE_VIRTUAL_ATTR:
    case SYNTAX_EXPR_TYPE_FUNC_ARGUMENT:
    case SYNTAX_EXPR_TYPE_CONST_VALUE:
        t = new AtomicSyntaxExpr("");
        break;
    case SYNTAX_EXPR_TYPE_MINUS:
        t = new MinusSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_ADD:
        t = new AddSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_DIV:
        t = new DivSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_MUL:
        t = new MulSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_POWER:
        t = new PowerSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_LOG:
        t = new LogSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_EQUAL:
        t = new EqualSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_NOTEQUAL:
        t = new NotEqualSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_LESS:
        t = new LessSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_GREATERTHAN:
        t = new GreaterSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_LESSEQUAL:
        t = new LessEqualSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_GREATEREQUAL:
        t = new GreaterEqualSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_AND:
        t = new AndSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_OR:
        t = new OrSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_BITAND:
        t = new BitAndSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_BITXOR:
        t = new BitXorSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_BITOR:
        t = new BitOrSyntaxExpr(NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_CONDITIONAL:
        t = new ConditionalSyntaxExpr(NULL, NULL, NULL);
        break;
    case SYNTAX_EXPR_TYPE_RANK:
        t = new RankSyntaxExpr;
        break;
    case SYNTAX_EXPR_TYPE_FUNC: {
        std::string funcName;
        std::vector<SyntaxExpr *> param;
        t = new FuncSyntaxExpr(funcName, param);
        break;
    }
    case SYNTAX_EXPR_TYPE_MULTI: {
        t = new MultiSyntaxExpr();
        break;
    }
    case SYNTAX_EXPR_TYPE_UNKNOWN:
        assert(false);
        break;
    default:
        assert(false);
        break;
    }

    return t;
}

} // namespace turing
} // namespace suez
