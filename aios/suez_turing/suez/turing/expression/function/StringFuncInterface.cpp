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
#include "suez/turing/expression/function/StringFuncInterface.h"

#include <cstddef>

#include "alog/Logger.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, TypedFuncCreator);
AUTIL_LOG_SETUP(expression, StringReplaceFuncInterfaceCreatorImpl);

template <>
autil::MultiChar to_string<autil::MultiChar>(const autil::MultiChar &x, autil::mem_pool::Pool *_pool) {
    return x;
}

template <typename T>
FunctionInterface *TypedFuncCreator::createTypedFunction(const FunctionSubExprVec &funcSubExprVec) {
    AttributeExpressionTyped<T> *attrExpr = dynamic_cast<AttributeExpressionTyped<T> *>(funcSubExprVec[0]);
    if (!attrExpr) {
        AUTIL_LOG(WARN, "unexpected expression for in function: %s.", funcSubExprVec[0]->getOriginalString().c_str());
        return NULL;
    }
    return new ToStringFuncInterface<T>(attrExpr);
}

template <>
FunctionInterface *TypedFuncCreator::createTypedFunction<MultiChar>(const FunctionSubExprVec &funcSubExprVec) {
    AttributeExpressionTyped<MultiChar> *attrExpr =
        dynamic_cast<AttributeExpressionTyped<MultiChar> *>(funcSubExprVec[0]);
    if (!attrExpr) {
        AUTIL_LOG(WARN, "unexpected expression for in function: %s.", funcSubExprVec[0]->getOriginalString().c_str());
        return NULL;
    }
    return new ToStringFuncInterface<MultiChar>(attrExpr);
}

FunctionInterface *TypedFuncCreator::createTypedFunction(const FunctionSubExprVec &funcSubExprVec) {
    if (funcSubExprVec.size() != 1) {
        AUTIL_LOG(WARN, "function can't accept more than one arg.");
        return NULL;
    }
    if (funcSubExprVec[0]->isMultiValue() && funcSubExprVec[0]->getType() != vt_string) {
        AUTIL_LOG(WARN, "function can't accept multivalue arg.");
        return NULL;
    }
#define CREATE_TYPED_FUNCTION(vt_type)                                                                                 \
    case vt_type: {                                                                                                    \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;                                                    \
        return createTypedFunction<T>(funcSubExprVec);                                                                 \
    }

    auto vt = funcSubExprVec[0]->getType();
    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(CREATE_TYPED_FUNCTION);
    case vt_string: {
        return createTypedFunction<MultiChar>(funcSubExprVec);
    }
    default:
        AUTIL_LOG(WARN, "function can't accept %s type arg.", vt2TypeName(vt).c_str());
        return NULL;
    }
#undef CREATE_TYPED_FUNCTION
    return NULL;
}

FunctionInterface *ToStringFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    return TypedFuncCreator::createTypedFunction(funcSubExprVec);
}

//////////////////////////////////////////////////////////////////////////////////////////////

StringReplaceFuncInterface::StringReplaceFuncInterface(AttributeExpressionTyped<autil::MultiChar> *attrExpr,
                                                       const std::string &replaceStr,
                                                       const char newChar)
    : _attrExpr(attrExpr), _replaceStr(replaceStr), _newChar(newChar) {}

StringReplaceFuncInterface::~StringReplaceFuncInterface() {}

string StringReplaceFuncInterface::evaluate(const matchdoc::MatchDoc matchDoc) {
    const autil::MultiChar &value = _attrExpr->getValue(matchDoc);
    string strValue(value.data(), value.size());
    for (size_t k = 0; k < _replaceStr.length(); k++) {
        autil::StringUtil::replace(strValue, _replaceStr.at(k), _newChar);
    }
    return strValue;
}

FunctionInterface *StringReplaceFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    AttributeExpressionTyped<autil::MultiChar> *attrExpr =
        dynamic_cast<AttributeExpressionTyped<autil::MultiChar> *>(funcSubExprVec[0]);
    if (!attrExpr) {
        AUTIL_LOG(WARN, "unexpected expression for in function: %s.", funcSubExprVec[0]->getOriginalString().c_str());
        return NULL;
    }

    AttributeExpression *arg = ET_ARGUMENT == funcSubExprVec[1]->getExpressionType() ? funcSubExprVec[1] : nullptr;
    if (!arg) {
        AUTIL_LOG(
            WARN, "invalid arg expression for in_string function: %s.", funcSubExprVec[1]->getOriginalString().c_str());
        return NULL;
    }
    string replaceStr;
    if (!arg->getConstValue<string>(replaceStr)) {
        AUTIL_LOG(WARN,
                  "invalid arg expression for string_replace function: %s.",
                  funcSubExprVec[1]->getOriginalString().c_str());
        return NULL;
    }
    AttributeExpression *arg2 = ET_ARGUMENT == funcSubExprVec[2]->getExpressionType() ? funcSubExprVec[2] : nullptr;
    if (!arg2) {
        AUTIL_LOG(WARN,
                  "invalid arg 2 expression for string_replace function: %s.",
                  funcSubExprVec[2]->getOriginalString().c_str());
        return NULL;
    }
    string newStr;
    if (!arg2->getConstValue<string>(newStr) || newStr.size() != 1) {
        AUTIL_LOG(WARN,
                  "invalid arg expression for string_replace function: %s.",
                  funcSubExprVec[2]->getOriginalString().c_str());
        return NULL;
    }

    return new StringReplaceFuncInterface(attrExpr, replaceStr, newStr[0]);
}

} // namespace turing
} // namespace suez
