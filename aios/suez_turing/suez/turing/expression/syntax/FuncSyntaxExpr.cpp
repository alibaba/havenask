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
#include "suez/turing/expression/syntax/FuncSyntaxExpr.h"

#include <assert.h>
#include <cstddef>
#include <string>

#include "autil/DataBuffer.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/syntax/SyntaxExprVisitor.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, FuncSyntaxExpr);

FuncSyntaxExpr::FuncSyntaxExpr(const std::string &funcName, const SubExprType &param)
    : SyntaxExpr(vt_unknown, SYNTAX_EXPR_TYPE_FUNC), _funcName(funcName), _exprs(param) {
    for (auto i = _exprs.begin(); i != _exprs.end(); ++i) {
        SyntaxExpr *expr = *i;
        if (expr->getSyntaxExprType() == SYNTAX_EXPR_TYPE_CONST_VALUE) {
            expr->setSyntaxExprType(SYNTAX_EXPR_TYPE_FUNC_ARGUMENT);
        }
    }
    SyntaxExpr::setExprString(getExprString());
}

FuncSyntaxExpr::~FuncSyntaxExpr() {
    for (SubExprType::iterator i = _exprs.begin(); i != _exprs.end(); ++i) {
        delete *i;
    }

    _exprs.clear();
}

void FuncSyntaxExpr::addParam(SyntaxExpr *expr) {
    // convert const value param to function arguments.
    if (expr->getSyntaxExprType() == SYNTAX_EXPR_TYPE_CONST_VALUE) {
        expr->setSyntaxExprType(SYNTAX_EXPR_TYPE_FUNC_ARGUMENT);
    }
    _exprs.push_back(expr);
    SyntaxExpr::setExprString(getExprString());
}

std::string FuncSyntaxExpr::getExprString() const {
    std::string ret = _funcName + "(";
    for (FuncSyntaxExpr::SubExprType::const_iterator i = _exprs.begin(); i != _exprs.end(); ++i) {
        if (i != _exprs.begin()) {
            ret += " , ";
        }
        ret += (*i)->getExprString();
    }
    ret += ")";
    return ret;
}

void FuncSyntaxExpr::accept(SyntaxExprVisitor *visitor) const { visitor->visitFuncSyntaxExpr(this); }

bool FuncSyntaxExpr::operator==(const SyntaxExpr *expr) const {
    assert(expr);
    const FuncSyntaxExpr *checkExpr = dynamic_cast<const FuncSyntaxExpr *>(expr);

    if (checkExpr && this->_syntaxType == checkExpr->_syntaxType) {
        if (_exprs.size() != checkExpr->_exprs.size()) {
            return false;
        }
        for (size_t i = 0; i < _exprs.size(); ++i) {
            if (!(*_exprs[i] == checkExpr->_exprs[i])) {
                return false;
            }
        }
        return true;
    }

    return false;
}

void FuncSyntaxExpr::serialize(autil::DataBuffer &dataBuffer) const {
    SyntaxExpr::serialize(dataBuffer);
    dataBuffer.write(_funcName);
    dataBuffer.write(_exprs);
}

void FuncSyntaxExpr::deserialize(autil::DataBuffer &dataBuffer) {
    SyntaxExpr::deserialize(dataBuffer);
    dataBuffer.read(_funcName);
    dataBuffer.read(_exprs);
}

} // namespace turing
} // namespace suez
