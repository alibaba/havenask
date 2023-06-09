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
#include "expression/syntax/SyntaxExpr.h"
#include "expression/syntax/ConstSyntaxExpr.h"
#include "expression/syntax/AttributeSyntaxExpr.h"
#include "expression/syntax/BinarySyntaxExpr.h"
#include "expression/syntax/FuncSyntaxExpr.h"

namespace expression {
AUTIL_LOG_SETUP(syntax, SyntaxExpr);

SyntaxExpr *SyntaxExpr::createSyntax(SyntaxExprType type, autil::mem_pool::Pool* pool) {
    switch (type) {
    case SYNTAX_EXPR_TYPE_UNKNOWN:
        return NULL;
    case SYNTAX_EXPR_TYPE_CONST_VALUE:
        return POOL_COMPATIBLE_NEW_CLASS(pool, ConstSyntaxExpr,
                "", UNKNOWN, SYNTAX_EXPR_TYPE_CONST_VALUE, pool);
    case SYNTAX_EXPR_TYPE_ATOMIC_ATTR:
        return POOL_COMPATIBLE_NEW_CLASS(pool, AttributeSyntaxExpr, "", "", pool);
    case SYNTAX_EXPR_TYPE_MINUS:
        return POOL_COMPATIBLE_NEW_CLASS(pool, MinusSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_ADD:
        return POOL_COMPATIBLE_NEW_CLASS(pool, AddSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_DIV:
        return POOL_COMPATIBLE_NEW_CLASS(pool, DivSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_MUL:
        return POOL_COMPATIBLE_NEW_CLASS(pool, MulSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_EQUAL:
        return POOL_COMPATIBLE_NEW_CLASS(pool, EqualSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_NOTEQUAL:
        return POOL_COMPATIBLE_NEW_CLASS(pool, NotEqualSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_LESS :
        return POOL_COMPATIBLE_NEW_CLASS(pool, LessSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_GREATERTHAN:
        return POOL_COMPATIBLE_NEW_CLASS(pool, GreaterSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_LESSEQUAL:
        return POOL_COMPATIBLE_NEW_CLASS(pool, LessEqualSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_GREATEREQUAL:
        return POOL_COMPATIBLE_NEW_CLASS(pool, GreaterEqualSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_AND:
        return POOL_COMPATIBLE_NEW_CLASS(pool, AndSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_OR:
        return POOL_COMPATIBLE_NEW_CLASS(pool, OrSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_BITAND:
        return POOL_COMPATIBLE_NEW_CLASS(pool, BitAndSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_BITOR:
        return POOL_COMPATIBLE_NEW_CLASS(pool, BitOrSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_BITXOR:
        return POOL_COMPATIBLE_NEW_CLASS(pool, BitXorSyntaxExpr, NULL, NULL, pool);
    case SYNTAX_EXPR_TYPE_FUNC:
        return POOL_COMPATIBLE_NEW_CLASS(pool, FuncSyntaxExpr,
                std::string(), FuncSyntaxExpr::SubExprType(), pool);
    case SYNTAX_EXPR_TYPE_FUNC_ARGUMENT:
        return POOL_COMPATIBLE_NEW_CLASS(pool, ConstSyntaxExpr, 
                "", UNKNOWN, SYNTAX_EXPR_TYPE_FUNC_ARGUMENT, false, pool);
    default:
        return NULL;
    }    
    return NULL;
}

SyntaxExpr *SyntaxExpr::createSyntax(SyntaxExprType type) {
    switch (type) {
    case SYNTAX_EXPR_TYPE_UNKNOWN:
        return NULL;
    case SYNTAX_EXPR_TYPE_CONST_VALUE:
        return new ConstSyntaxExpr();
    case SYNTAX_EXPR_TYPE_ATOMIC_ATTR:
        return new AttributeSyntaxExpr();
    case SYNTAX_EXPR_TYPE_MINUS:
        return new MinusSyntaxExpr();
    case SYNTAX_EXPR_TYPE_ADD:
        return new AddSyntaxExpr();
    case SYNTAX_EXPR_TYPE_DIV:
        return new DivSyntaxExpr();
    case SYNTAX_EXPR_TYPE_MUL:
        return new MulSyntaxExpr();
    case SYNTAX_EXPR_TYPE_EQUAL:
        return new EqualSyntaxExpr();
    case SYNTAX_EXPR_TYPE_NOTEQUAL:
        return new NotEqualSyntaxExpr();
    case SYNTAX_EXPR_TYPE_LESS :
        return new LessSyntaxExpr();
    case SYNTAX_EXPR_TYPE_GREATERTHAN:
        return new GreaterSyntaxExpr();
    case SYNTAX_EXPR_TYPE_LESSEQUAL:
        return new LessEqualSyntaxExpr();
    case SYNTAX_EXPR_TYPE_GREATEREQUAL:
        return new GreaterEqualSyntaxExpr();
    case SYNTAX_EXPR_TYPE_AND:
        return new AndSyntaxExpr();
    case SYNTAX_EXPR_TYPE_OR:
        return new OrSyntaxExpr();
    case SYNTAX_EXPR_TYPE_BITAND:
        return new BitAndSyntaxExpr();
    case SYNTAX_EXPR_TYPE_BITOR:
        return new BitOrSyntaxExpr();
    case SYNTAX_EXPR_TYPE_BITXOR:
        return new BitXorSyntaxExpr();
    case SYNTAX_EXPR_TYPE_FUNC:
        return new FuncSyntaxExpr();
    case SYNTAX_EXPR_TYPE_FUNC_ARGUMENT:
        return new ConstSyntaxExpr("", UNKNOWN, SYNTAX_EXPR_TYPE_FUNC_ARGUMENT);
    default:
        return NULL;
    }    
    return NULL;
}

}
