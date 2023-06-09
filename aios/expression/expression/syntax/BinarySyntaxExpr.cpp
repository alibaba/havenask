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
#include "expression/syntax/BinarySyntaxExpr.h"

using namespace std;

namespace expression {
AUTIL_LOG_SETUP(syntax, BinarySyntaxExpr);

bool BinarySyntaxExpr::operator == (const SyntaxExpr *expr) const {
    assert(expr);
    const BinarySyntaxExpr *checkExpr =
        dynamic_cast<const BinarySyntaxExpr*>(expr);
    
    if (checkExpr && this->_syntaxType == checkExpr->_syntaxType
        && *(this->_exprA) == checkExpr->_exprA && *(this->_exprB) == checkExpr->_exprB)
    {
        return true;
    }

    return false;
}

void BinarySyntaxExpr::setExprString(SyntaxExpr *a, SyntaxExpr *b, 
                                     SyntaxExprType syntaxExprType)
{
    assert(a && b);
    
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

    if (_pool) {
        const string& aExprStr = a->getExprString();
        const string& bExprStr = b->getExprString();
        size_t bufferSize = aExprStr.size() + opStr.size() + bExprStr.size() + 2;
        char* buffer = (char*)_pool->allocate(bufferSize);
        char* cursor = buffer;
        memcpy(cursor++, "(", 1);
        memcpy(cursor, aExprStr.c_str(), aExprStr.size());
        cursor += aExprStr.size();
        memcpy(cursor, opStr.c_str(), opStr.size());
        cursor += opStr.size();
        memcpy(cursor, bExprStr.c_str(), bExprStr.size());
        cursor += bExprStr.size();
        memcpy(cursor++, ")", 1);
        assert((size_t)(cursor - buffer) == bufferSize);
        SyntaxExpr::setExprString(autil::StringView(buffer, bufferSize));
    } else {
        SyntaxExpr::setExprString("(" + a->getExprString() + opStr +
                b->getExprString() + ")");
    }
}

void BinarySyntaxExpr::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_exprA);
    dataBuffer.write(_exprB);
}

void BinarySyntaxExpr::deserialize(autil::DataBuffer &dataBuffer)
{
    if (_pool)
    {
        dataBuffer.read(_exprA, _pool);
        dataBuffer.read(_exprB, _pool);
    }
    else
    {
        dataBuffer.read(_exprA);
        dataBuffer.read(_exprB);
    }

    if (_exprA && _exprB) {
        setExprString(_exprA, _exprB, _syntaxType);
    }
}

}

