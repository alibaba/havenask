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
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"

#include <assert.h>
#include <cstddef>
#include <typeinfo>

#include "alog/Logger.h"
#include "autil/DataBuffer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "suez/turing/expression/syntax/SyntaxExprVisitor.h"

using namespace std;
using namespace autil;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, AtomicSyntaxExpr);

static string getConstStringValueExprString(const string &exprStr) {
    string ret;
    ret.reserve(exprStr.size() + 8);
    ret += "\"";
    for (size_t i = 0; i < exprStr.size(); ++i) {
        char ch = exprStr[i];
        if (ch == '\\' || ch == '\"') {
            ret += '\\';
        }
        ret += ch;
    }

    ret += "\"";
    return ret;
}

AtomicSyntaxExpr::AtomicSyntaxExpr(const string &exprStr,
                                   ExprResultType resultType,
                                   AtomicSyntaxExprType atomicSyntaxExprType,
                                   const string &prefixStr)
    : SyntaxExpr(resultType, SYNTAX_EXPR_TYPE_UNKNOWN), _valueString(exprStr), _prefixString(prefixStr) {
    _atomicSyntaxExprType = atomicSyntaxExprType;
    if (atomicSyntaxExprType == ATTRIBUTE_NAME) {
        setSyntaxExprType(SYNTAX_EXPR_TYPE_ATOMIC_ATTR);
        _exprString = prefixStr.empty() ? exprStr : prefixStr + "." + exprStr;
    } else if (atomicSyntaxExprType == DOUBLE_VT || atomicSyntaxExprType == FLOAT_VT ||
               atomicSyntaxExprType == INT64_VT || atomicSyntaxExprType == UINT64_VT ||
               atomicSyntaxExprType == INT32_VT || atomicSyntaxExprType == UINT32_VT ||
               atomicSyntaxExprType == INT16_VT || atomicSyntaxExprType == UINT16_VT ||
               atomicSyntaxExprType == INT8_VT || atomicSyntaxExprType == UINT8_VT ||
               atomicSyntaxExprType == STRING_VT) {
        setSyntaxExprType(SYNTAX_EXPR_TYPE_CONST_VALUE);
        if (atomicSyntaxExprType == STRING_VT) {
            _exprString = getConstStringValueExprString(exprStr);
        } else {
            _exprString = exprStr;
        }
    } else {
        AUTIL_LOG(TRACE3, "unknown atomic syntax expression");
        _exprString = exprStr;
    }
}

AtomicSyntaxExpr::~AtomicSyntaxExpr() {}

bool AtomicSyntaxExpr::operator==(const SyntaxExpr *expr) const {
    const AtomicSyntaxExpr *checkExpr = dynamic_cast<const AtomicSyntaxExpr *>(expr);

    AUTIL_LOG(TRACE3, "checkExpr:%p", checkExpr);
    AUTIL_LOG(TRACE3, "type: %s", typeid(expr).name());

    if (checkExpr && this->_exprString == checkExpr->getExprString() &&
        this->getExprResultType() == checkExpr->getExprResultType() &&
        this->getAtomicSyntaxExprType() == checkExpr->getAtomicSyntaxExprType() &&
        this->_valueString == checkExpr->_valueString && this->_prefixString == checkExpr->_prefixString) {
        return true;
    }
    return false;
}

void AtomicSyntaxExpr::accept(SyntaxExprVisitor *visitor) const { visitor->visitAtomicSyntaxExpr(this); }

void AtomicSyntaxExpr::serialize(autil::DataBuffer &dataBuffer) const {
    SyntaxExpr::serialize(dataBuffer);
    dataBuffer.write(_atomicSyntaxExprType);
    dataBuffer.write(_valueString);
    dataBuffer.write(_prefixString);
}

void AtomicSyntaxExpr::deserialize(autil::DataBuffer &dataBuffer) {
    SyntaxExpr::deserialize(dataBuffer);
    dataBuffer.read(_atomicSyntaxExprType);
    dataBuffer.read(_valueString);
    dataBuffer.read(_prefixString);
}

} // namespace turing
} // namespace suez
