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
#include "expression/syntax/ConstSyntaxExpr.h"
#include "expression/syntax/SyntaxExprVisitor.h"
#include "expression/util/SyntaxStringConvertor.h"
using namespace std;

namespace expression {
AUTIL_LOG_SETUP(expression, ConstSyntaxExpr);

ConstSyntaxExpr::ConstSyntaxExpr(const std::string &valueStr, 
                                 ConstExprType valueType, SyntaxExprType type,
                                 bool needEncode,
                                 autil::mem_pool::Pool* pool)
    : SyntaxExpr(type, pool)
    , _valueStr((valueType == STRING_VALUE && !needEncode) ? SyntaxStringConvertor::encodeEscapeString(valueStr) : valueStr)
    , _valueType(valueType)
    , _needEncode(needEncode)
{
    if (valueType != UNKNOWN) {
        setExprString(_valueStr);
    }
}

ConstSyntaxExpr::~ConstSyntaxExpr() {
}

bool ConstSyntaxExpr::operator == (const SyntaxExpr *expr) const {
    const ConstSyntaxExpr *checkExpr = dynamic_cast<const ConstSyntaxExpr*>(expr);
    if (!checkExpr) {
        return false;
    }
    return _valueStr == checkExpr->_valueStr
        && _valueType == checkExpr->_valueType;
}

void ConstSyntaxExpr::accept(SyntaxExprVisitor *visitor) const {
    SyntaxExprType syntaxType = getSyntaxType();
    if (syntaxType == SYNTAX_EXPR_TYPE_FUNC_ARGUMENT) {
        visitor->visitFuncArgumentExpr(this);
    } else {
        assert(syntaxType == SYNTAX_EXPR_TYPE_CONST_VALUE);
        visitor->visitConstSyntaxExpr(this);
    }
}

void ConstSyntaxExpr::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_valueStr);
    dataBuffer.write(_valueType);
}

void ConstSyntaxExpr::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_valueStr);
    dataBuffer.read(_valueType);
    setExprString(_valueStr);
}

}
