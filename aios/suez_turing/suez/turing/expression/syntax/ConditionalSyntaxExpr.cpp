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
#include "suez/turing/expression/syntax/ConditionalSyntaxExpr.h"

#include <assert.h>
#include <cstddef>
#include <string>

#include "autil/DataBuffer.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/syntax/SyntaxExprVisitor.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, ConditionalSyntaxExpr);

ConditionalSyntaxExpr::ConditionalSyntaxExpr(SyntaxExpr *conditionalExpr, SyntaxExpr *trueExpr, SyntaxExpr *falseExpr)
    : SyntaxExpr(vt_unknown, SYNTAX_EXPR_TYPE_CONDITIONAL) {
    _conditionalExpr = conditionalExpr;
    _trueExpr = trueExpr;
    _falseExpr = falseExpr;

    if (_conditionalExpr && _trueExpr && _falseExpr) {
        SyntaxExpr::setExprString(_conditionalExpr->getExprString() + "?" + _trueExpr->getExprString() + ":" +
                                  _falseExpr->getExprString());
    }
}

ConditionalSyntaxExpr::~ConditionalSyntaxExpr() {
    delete _conditionalExpr;
    _conditionalExpr = NULL;
    delete _trueExpr;
    _trueExpr = NULL;
    delete _falseExpr;
    _falseExpr = NULL;
}

bool ConditionalSyntaxExpr::operator==(const SyntaxExpr *expr) const {
    assert(expr);
    const ConditionalSyntaxExpr *checkExpr = dynamic_cast<const ConditionalSyntaxExpr *>(expr);

    if (checkExpr && this->_syntaxType == checkExpr->_syntaxType &&
        *(this->_conditionalExpr) == checkExpr->_conditionalExpr && *(this->_trueExpr) == checkExpr->_trueExpr &&
        *(this->_falseExpr) == checkExpr->_falseExpr) {
        return true;
    }

    return false;
}

void ConditionalSyntaxExpr::serialize(autil::DataBuffer &dataBuffer) const {
    SyntaxExpr::serialize(dataBuffer);
    dataBuffer.write(_conditionalExpr);
    dataBuffer.write(_trueExpr);
    dataBuffer.write(_falseExpr);
}

void ConditionalSyntaxExpr::deserialize(autil::DataBuffer &dataBuffer) {
    SyntaxExpr::deserialize(dataBuffer);
    dataBuffer.read(_conditionalExpr);
    dataBuffer.read(_trueExpr);
    dataBuffer.read(_falseExpr);
}

std::string ConditionalSyntaxExpr::getExprString() const { return std::string("(") + _exprString + std::string(")"); }

void ConditionalSyntaxExpr::accept(SyntaxExprVisitor *visitor) const { visitor->visitConditionalSyntaxExpr(this); }

} // namespace turing
} // namespace suez
