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
#include "suez/turing/expression/syntax/BinarySyntaxExpr.h"

#include <assert.h>
#include <cstddef>
#include <string>

#include "autil/DataBuffer.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, BinarySyntaxExpr);

BinarySyntaxExpr::BinarySyntaxExpr(SyntaxExpr *a, SyntaxExpr *b, SyntaxExprType syntaxExprType)
    : SyntaxExpr(vt_unknown, syntaxExprType) {
    _exprA = a;
    _exprB = b;

    setExprString(a, b, syntaxExprType);
}

BinarySyntaxExpr::~BinarySyntaxExpr() {
    delete _exprA;
    _exprA = NULL;
    delete _exprB;
    _exprB = NULL;
}

bool BinarySyntaxExpr::operator==(const SyntaxExpr *expr) const {
    assert(expr);
    const BinarySyntaxExpr *checkExpr = dynamic_cast<const BinarySyntaxExpr *>(expr);

    if (checkExpr && this->_syntaxType == checkExpr->_syntaxType && *(this->_exprA) == checkExpr->_exprA &&
        *(this->_exprB) == checkExpr->_exprB) {
        return true;
    }

    return false;
}

void BinarySyntaxExpr::setExprString(SyntaxExpr *a, SyntaxExpr *b, SyntaxExprType syntaxExprType) {
    if (!(a && b)) {
        return;
    }

    if (syntaxExprType == SYNTAX_EXPR_TYPE_LOG) {
        SyntaxExpr::setExprString("LOG(" + a->getExprString() + "," + b->getExprString() + ")");
        return;
    }
    string opStr;
    switch (syntaxExprType) {
    case SYNTAX_EXPR_TYPE_MINUS:
        opStr = "-";
        break;
    case SYNTAX_EXPR_TYPE_ADD:
        opStr = "+";
        break;
    case SYNTAX_EXPR_TYPE_DIV:
        opStr = "/";
        break;
    case SYNTAX_EXPR_TYPE_MUL:
        opStr = "*";
        break;
    case SYNTAX_EXPR_TYPE_POWER:
        opStr = "**";
        break;
    case SYNTAX_EXPR_TYPE_EQUAL:
        opStr = "=";
        break;
    case SYNTAX_EXPR_TYPE_NOTEQUAL:
        opStr = "!=";
        break;
    case SYNTAX_EXPR_TYPE_LESS:
        opStr = "<";
        break;
    case SYNTAX_EXPR_TYPE_GREATERTHAN:
        opStr = ">";
        break;
    case SYNTAX_EXPR_TYPE_LESSEQUAL:
        opStr = "<=";
        break;
    case SYNTAX_EXPR_TYPE_GREATEREQUAL:
        opStr = ">=";
        break;
    case SYNTAX_EXPR_TYPE_AND:
        opStr = " AND ";
        break;
    case SYNTAX_EXPR_TYPE_OR:
        opStr = " OR ";
        break;
    case SYNTAX_EXPR_TYPE_BITAND:
        opStr = "&";
        break;
    case SYNTAX_EXPR_TYPE_BITOR:
        opStr = "|";
        break;
    case SYNTAX_EXPR_TYPE_BITXOR:
        opStr = "^";
        break;
    default:
        break;
    }

    SyntaxExpr::setExprString(a->getExprString() + opStr + b->getExprString());
}

void BinarySyntaxExpr::serialize(autil::DataBuffer &dataBuffer) const {
    SyntaxExpr::serialize(dataBuffer);
    dataBuffer.write(_exprA);
    dataBuffer.write(_exprB);
}

void BinarySyntaxExpr::deserialize(autil::DataBuffer &dataBuffer) {
    SyntaxExpr::deserialize(dataBuffer);
    dataBuffer.read(_exprA);
    dataBuffer.read(_exprB);
}

} // namespace turing
} // namespace suez
