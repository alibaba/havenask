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
#include "expression/syntax/FuncSyntaxExpr.h"
#include "expression/syntax/SyntaxExprVisitor.h"
using namespace std;

namespace expression {
AUTIL_LOG_SETUP(syntax, FuncSyntaxExpr);

FuncSyntaxExpr::FuncSyntaxExpr(const std::string &funcName,
                               const SubExprType &params,
                               autil::mem_pool::Pool* pool)
    : SyntaxExpr(SYNTAX_EXPR_TYPE_FUNC, pool)
    , _funcName(funcName)
    , _exprs(params)
{
    for(SubExprType::iterator i = _exprs.begin();
        i != _exprs.end(); ++i)
    {
        SyntaxExpr *expr = *i;
        assert(_pool == expr->getPool());
        if (expr->getSyntaxType() == SYNTAX_EXPR_TYPE_CONST_VALUE) {
            expr->setSyntaxType(SYNTAX_EXPR_TYPE_FUNC_ARGUMENT);
        }
    }
    
    if (!_funcName.empty()) {
        initExprString();
    }
}

FuncSyntaxExpr::~FuncSyntaxExpr() {
    for(SubExprType::iterator i = _exprs.begin();
        i != _exprs.end(); ++i)
    {
        POOL_COMPATIBLE_DELETE_CLASS(_pool, *i);
    }

    _exprs.clear();
}

bool FuncSyntaxExpr::operator == (const SyntaxExpr *expr) const {
    assert(expr);
    const FuncSyntaxExpr *checkExpr =
        dynamic_cast<const FuncSyntaxExpr*>(expr);
    
    if (!checkExpr) {
        return false;
    }
    if (_exprs.size() != checkExpr->_exprs.size()) {
        return false;
    }
    if (!(_funcName == checkExpr->_funcName)) {
        return false;
    }
    for (size_t i = 0; i < _exprs.size(); ++i) {
        if (!(*_exprs[i] == checkExpr->_exprs[i])) {
            return false;
        }
    }
    return true;
}

void FuncSyntaxExpr::accept(SyntaxExprVisitor *visitor) const {
    visitor->visitFuncSyntaxExpr(this);
}

std::string FuncSyntaxExpr::genExprString() const {
    std::string ret = _funcName + "(";
    for (FuncSyntaxExpr::SubExprType::const_iterator i = _exprs.begin();
         i != _exprs.end(); ++i)
    {
        if (i != _exprs.begin()) {
            ret += ", ";
        }
        ret += (*i)->getExprString();
    }
    ret += ")";
    return ret;
}

autil::StringView FuncSyntaxExpr::genExprConstString() const {
    assert(_pool);
    string delimStr = ", ";
    size_t delimCount = (_exprs.size() <= 1) ? 0 : _exprs.size() - 1;
    size_t bufferSize = _funcName.size() + delimStr.size() * delimCount + 2;
    for (size_t i = 0; i < _exprs.size(); i++) {
        bufferSize += _exprs[i]->getExprString().size();
    }

    char* buffer = (char*)_pool->allocate(bufferSize);
    char* cursor = buffer;
    memcpy(cursor, _funcName.c_str(), _funcName.size());
    cursor += _funcName.size();
    memcpy(cursor++, "(", 1);
    for (size_t i = 0; i < _exprs.size(); i++) {
        if (i != 0) {
            memcpy(cursor, delimStr.c_str(), delimStr.size());
            cursor += delimStr.size();
        }
        const string &exprStr = _exprs[i]->getExprString();
        memcpy(cursor, exprStr.c_str(), exprStr.size());
        cursor += exprStr.size();
    }
    memcpy(cursor++, ")", 1);
    assert((size_t)(cursor - buffer) == bufferSize);
    return autil::StringView(buffer, bufferSize);
}

void FuncSyntaxExpr::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_funcName);
    dataBuffer.write(_exprs);
}

void FuncSyntaxExpr::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_funcName);
    if (_pool)
    {
        dataBuffer.read(_exprs, _pool);
    }
    else
    {
        dataBuffer.read(_exprs);
    }
    initExprString();
}

}

