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
#include "suez/turing/expression/function/HaInFuncInterface.h"

#include <assert.h>
#include <cstddef>

#include "autil/StringUtil.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

using namespace std;
using namespace autil;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, TypedHaInFuncCreator);

#define CREATE_NUMBERIC_SINGLE_FUNCTION(vt_type)                                                                       \
    case vt_type: {                                                                                                    \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;                                                    \
        return new SingleHaInFuncInterface<T>(funcSubExprVec, existFlag);                                              \
    }

#define CREATE_NUMBERIC_MULTI_FUNCTION(vt_type)                                                                        \
    case vt_type: {                                                                                                    \
        typedef VariableTypeTraits<vt_type, true>::AttrExprType T;                                                     \
        return new MultiHaInFuncInterface<T>(funcSubExprVec, existFlag);                                               \
    }

template <typename T1, typename T2>
bool TypedHaInFuncCreator::processFuncArg(const FunctionSubExprVec &funcSubExprVec,
                                          AttributeExpressionTyped<T1> *&attrExpr,
                                          set<T2> *&valueSet) {
    attrExpr = dynamic_cast<AttributeExpressionTyped<T1> *>(funcSubExprVec[0]);
    if (!attrExpr) {
        AUTIL_LOG(WARN, "unexpected expression for in function: %s.", funcSubExprVec[0]->getOriginalString().c_str());
        return false;
    }

    auto valueArg = ET_ARGUMENT == funcSubExprVec[1]->getExpressionType() ? funcSubExprVec[1] : nullptr;

    if (!valueArg) {
        AUTIL_LOG(WARN, "invalid arg expression for in function: %s.", funcSubExprVec[1]->getOriginalString().c_str());
        return false;
    }
    string valueArgStr;
    if (!valueArg->getConstValue<string>(valueArgStr)) {
        AUTIL_LOG(WARN, "invalid arg expression for in function: %s.", funcSubExprVec[1]->getOriginalString().c_str());
        return false;
    }

    string sepArgStr = "|";
    if (funcSubExprVec.size() == 3) {
        auto sepArg = ET_ARGUMENT == funcSubExprVec[2]->getExpressionType() ? funcSubExprVec[2] : nullptr;

        if (!sepArg) {
            AUTIL_LOG(
                WARN, "invalid arg expression for in function: %s.", funcSubExprVec[2]->getOriginalString().c_str());
            return false;
        }
        if (!sepArg->getConstValue<string>(sepArgStr)) {
            AUTIL_LOG(
                WARN, "invalid arg expression for in function: %s.", funcSubExprVec[2]->getOriginalString().c_str());
            return false;
        }
        if (sepArgStr.size() == 0) {
            AUTIL_LOG(
                WARN, "invalid arg expression for in function: %s.", funcSubExprVec[2]->getOriginalString().c_str());
            return false;
        }
    }

    StringTokenizer st(valueArgStr, sepArgStr, StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    set<T2> *pSet = new set<T2>;
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        T2 value;
        if (!StringUtil::fromString(st[i], value)) {
            delete pSet;
            AUTIL_LOG(WARN, "invalid parameter for in function: %s.", st[i].c_str());
            return false;
        }
        pSet->insert(value);
    }
    valueSet = pSet;
    return true;
}

template <typename T>
bool SingleHaInFuncInterface<T>::beginRequest(FunctionProvider *provider) {
    return TypedHaInFuncCreator::processFuncArg<T, SingleValueType>(_exprs, _attrExpr, _set);
}

template <typename T>
bool MultiHaInFuncInterface<T>::beginRequest(FunctionProvider *provider) {
    return TypedHaInFuncCreator::processFuncArg<T, SingleValueType>(_exprs, _attrExpr, _set);
}

FunctionInterface *TypedHaInFuncCreator::createTypedFunction(const FunctionSubExprVec &funcSubExprVec, bool existFlag) {
    if ((funcSubExprVec.size() != 2) && (funcSubExprVec.size() != 3)) {
        AUTIL_LOG(WARN, "function only accept two or three args.");
        return NULL;
    }

    VariableType vt = funcSubExprVec[0]->getType();
    bool bMulti = funcSubExprVec[0]->isMultiValue();

    if (bMulti) {
        switch (vt) {
            NUMERIC_VARIABLE_TYPE_MACRO_HELPER(CREATE_NUMBERIC_MULTI_FUNCTION);
        case vt_string:
            typedef VariableTypeTraits<vt_string, true>::AttrExprType T;
            return new MultiHaInFuncInterface<T>(funcSubExprVec, existFlag);
        default:
            AUTIL_LOG(WARN, "function can't accept %s type arg.", vt2TypeName(vt).c_str());
            break;
        }
    } else {
        switch (vt) {
            NUMERIC_VARIABLE_TYPE_MACRO_HELPER(CREATE_NUMBERIC_SINGLE_FUNCTION);
        case vt_string:
            typedef VariableTypeTraits<vt_string, false>::AttrExprType T;
            return new SingleHaInFuncInterface<T>(funcSubExprVec, existFlag);
        default:
            AUTIL_LOG(WARN, "function can't accept %s type arg.", vt2TypeName(vt).c_str());
            break;
        }
    }

    return NULL;
}

template <typename T>
bool SingleHaInFuncInterface<T>::evaluate(matchdoc::MatchDoc matchDoc) {
    assert(matchdoc::INVALID_MATCHDOC != matchDoc);
    bool isExist = false;
    T value = _attrExpr->evaluateAndReturn(matchDoc);
    typedef typename VariableTypeTraits<TypeHaInfoTraits<T>::VARIABLE_TYPE, false>::AttrItemType SingleItemType;
    typedef VariableTypeTraits<TypeHaInfoTraits<T>::VARIABLE_TYPE, false> SingleVariableTraits;
    SingleItemType itemValue = SingleVariableTraits::convert(value);
    isExist = _set->find(itemValue) != _set->end();
    return isExist == _existFlag;
}

template <typename T>
bool MultiHaInFuncInterface<T>::evaluate(matchdoc::MatchDoc matchDoc) {
    assert(matchdoc::INVALID_MATCHDOC != matchDoc);
    bool isExist = false;
    T value = _attrExpr->evaluateAndReturn(matchDoc);
    typedef typename VariableTypeTraits<TypeHaInfoTraits<T>::VARIABLE_TYPE, true>::AttrItemType MultiItemType;
    typedef VariableTypeTraits<TypeHaInfoTraits<T>::VARIABLE_TYPE, true> MultiVariableTraits;
    MultiItemType itemValues = MultiVariableTraits::convert(value);
    uint32_t valuesNum = itemValues.size();
    if (0 == valuesNum) {
        return _existFlag ? false : true;
    }
    for (uint32_t valueIdx = 0; valueIdx < valuesNum; valueIdx++) {
        if (_set->find(itemValues[valueIdx]) != _set->end()) {
            isExist = true;
            break;
        }
    }
    return isExist == _existFlag;
}

FunctionInterface *HaInFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    return TypedHaInFuncCreator::createTypedFunction(funcSubExprVec, true);
}

FunctionInterface *NotHaInFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    return TypedHaInFuncCreator::createTypedFunction(funcSubExprVec, false);
}

} // namespace turing
} // namespace suez
