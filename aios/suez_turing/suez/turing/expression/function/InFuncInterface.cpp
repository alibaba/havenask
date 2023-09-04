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
#include "suez/turing/expression/function/InFuncInterface.h"

#include <cstddef>

#include "alog/Logger.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

using namespace std;
using namespace autil;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, TypedInFuncCreator);

template <typename T>
FunctionInterface *TypedInFuncCreator::createTypedFunction(const FunctionSubExprVec &funcSubExprVec, bool existFlag) {
    AttributeExpressionTyped<T> *attrExpr = dynamic_cast<AttributeExpressionTyped<T> *>(funcSubExprVec[0]);
    if (!attrExpr) {
        AUTIL_LOG(WARN, "unexpected expression for in function: %s.", funcSubExprVec[0]->getOriginalString().c_str());
        return NULL;
    }
    auto arg = ET_ARGUMENT == funcSubExprVec[1]->getExpressionType() ? funcSubExprVec[1] : nullptr;
    if (!arg) {
        AUTIL_LOG(WARN, "invalid arg expression for in function: %s.", funcSubExprVec[1]->getOriginalString().c_str());
        return NULL;
    }
    string setStr;
    if (!arg->getConstValue<string>(setStr)) {
        AUTIL_LOG(WARN, "invalid arg expression for in function: %s.", funcSubExprVec[1]->getOriginalString().c_str());
        return NULL;
    }
    StringTokenizer st(setStr, "|", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    set<T> *pSet = new set<T>;
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        T value;
        if (!StringUtil::fromString(st[i], value)) {
            delete pSet;
            AUTIL_LOG(WARN, "invalid parameter for in function: %s.", st[i].c_str());
            return NULL;
        }
        pSet->insert(value);
    }
    return new InFuncInterface<T>(attrExpr, pSet, existFlag);
}

template <>
FunctionInterface *TypedInFuncCreator::createTypedFunction<MultiChar>(const FunctionSubExprVec &funcSubExprVec,
                                                                      bool existFlag) {
    AttributeExpressionTyped<MultiChar> *attrExpr =
        dynamic_cast<AttributeExpressionTyped<MultiChar> *>(funcSubExprVec[0]);
    if (!attrExpr) {
        AUTIL_LOG(WARN, "unexpected expression for in function: %s.", funcSubExprVec[0]->getOriginalString().c_str());
        return NULL;
    }
    auto arg = ET_ARGUMENT == funcSubExprVec[1]->getExpressionType() ? funcSubExprVec[1] : nullptr;
    if (!arg) {
        AUTIL_LOG(WARN, "invalid arg expression for in function: %s.", funcSubExprVec[1]->getOriginalString().c_str());
        return NULL;
    }
    string setStr;
    if (!arg->getConstValue<string>(setStr)) {
        AUTIL_LOG(WARN, "invalid arg expression for in function: %s.", funcSubExprVec[1]->getOriginalString().c_str());
        return NULL;
    }
    StringTokenizer st(setStr, "|", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    set<MultiChar> *pSet = new set<MultiChar>;
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        MultiChar mc;
        char *buffer = MultiValueCreator::createMultiValueBuffer(st[i].data(), st[i].size());
        mc.init(buffer);
        pSet->insert(mc);
    }
    return new InFuncInterface<MultiChar>(attrExpr, pSet, existFlag);
}

FunctionInterface *TypedInFuncCreator::createTypedFunction(const FunctionSubExprVec &funcSubExprVec, bool existFlag) {
    if (funcSubExprVec.size() != 2) {
        AUTIL_LOG(WARN, "function can't accept more than two arg.");
        return NULL;
    }
    if (funcSubExprVec[0]->isMultiValue() && funcSubExprVec[0]->getType() != vt_string) {
        AUTIL_LOG(WARN, "function can't accept multivalue arg.");
        return NULL;
    }
#define CREATE_TYPED_FUNCTION(vt_type)                                                                                 \
    case vt_type: {                                                                                                    \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;                                                    \
        return createTypedFunction<T>(funcSubExprVec, existFlag);                                                      \
    }

    auto vt = funcSubExprVec[0]->getType();
    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(CREATE_TYPED_FUNCTION);
    case vt_string: {
        return createTypedFunction<MultiChar>(funcSubExprVec, existFlag);
    }
    default:
        AUTIL_LOG(WARN, "function can't accept %s type arg.", vt2TypeName(vt).c_str());
        return NULL;
    }
#undef CREATE_TYPED_FUNCTION
    return NULL;
}

template <>
InFuncInterface<autil::MultiChar>::~InFuncInterface() {
    if (_set != nullptr) {
        std::set<autil::MultiChar>::iterator iter = _set->begin();
        for (; iter != _set->end(); iter++) {
            assert(iter->isEmptyData() || iter->hasEncodedCount());
            const char *buffer = iter->getBaseAddress();
            delete[] buffer;
        }
    }
    DELETE_AND_SET_NULL(_set);
}

FunctionInterface *InFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    return TypedInFuncCreator::createTypedFunction(funcSubExprVec, true);
}

FunctionInterface *NotInFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    return TypedInFuncCreator::createTypedFunction(funcSubExprVec, false);
}

} // namespace turing
} // namespace suez
