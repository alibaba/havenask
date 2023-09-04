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
#include "suez/turing/expression/function/BinaryFuncInterface.h"

#include <assert.h>
#include <cstddef>
#include <math.h>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

using namespace std;
using namespace autil;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, TypedBinaryFuncCreator);

template <typename T>
PowFuncInterface<T>::PowFuncInterface(AttributeExpressionTyped<T> *attrExpr1,
                                      AttributeExpressionTyped<T> *attrExpr2,
                                      T lhs,
                                      T rhs)
    : _attrExpr1(attrExpr1), _attrExpr2(attrExpr2), _lhs(lhs), _rhs(rhs) {}

template <typename T>
PowFuncInterface<T>::~PowFuncInterface() {}

template <typename T>
float PowFuncInterface<T>::evaluate(matchdoc::MatchDoc matchDoc) {
    assert(matchdoc::INVALID_MATCHDOC != matchDoc);
    T attr1, attr2;
    if (nullptr != _attrExpr1) {
        attr1 = _attrExpr1->evaluateAndReturn(matchDoc);
    } else {
        attr1 = _lhs;
    }
    if (nullptr != _attrExpr2) {
        attr2 = _attrExpr2->evaluateAndReturn(matchDoc);
    } else {
        attr2 = _rhs;
    }
    return std::pow(attr1, attr2);
}

template <typename T>
LogFuncInterface<T>::LogFuncInterface(AttributeExpressionTyped<T> *attrExpr1,
                                      AttributeExpressionTyped<T> *attrExpr2,
                                      T lhs,
                                      T rhs)
    : _attrExpr1(attrExpr1), _attrExpr2(attrExpr2), _lhs(lhs), _rhs(rhs) {}

template <typename T>
LogFuncInterface<T>::~LogFuncInterface() {}

template <typename T>
float LogFuncInterface<T>::evaluate(matchdoc::MatchDoc matchDoc) {
    assert(matchdoc::INVALID_MATCHDOC != matchDoc);
    T attr1, attr2;
    if (nullptr != _attrExpr1) {
        attr1 = _attrExpr1->evaluateAndReturn(matchDoc);
    } else {
        attr1 = _lhs;
    }
    if (nullptr != _attrExpr2) {
        attr2 = _attrExpr2->evaluateAndReturn(matchDoc);
    } else {
        attr2 = _rhs;
    }
    if (attr1 <= 0 || attr2 <= 0 || std::log(attr1) == 0) {
        return 0;
    }
    return std::log(attr2) / std::log(attr1);
}

template <typename T, template <typename TT> class InterfaceType>
FunctionInterface *TypedBinaryFuncCreator::createTypedFunction(const FunctionSubExprVec &funcSubExprVec) {
    T lhs = 1, rhs = 1;
    AttributeExpressionTyped<T> *attrExpr1 = nullptr;
    AttributeExpressionTyped<T> *attrExpr2 = nullptr;
    if (ET_ARGUMENT == funcSubExprVec[0]->getExpressionType()) {
        auto expr1 = funcSubExprVec[0];
        if (!expr1 || !expr1->getConstValue(lhs)) {
            AUTIL_LOG(
                WARN, "invalid arg expression for in function: %s.", funcSubExprVec[0]->getOriginalString().c_str());
            return NULL;
        }
    } else {
        attrExpr1 = dynamic_cast<AttributeExpressionTyped<T> *>(funcSubExprVec[0]);
        if (!attrExpr1) {
            AUTIL_LOG(
                WARN, "unexpected expression for in function: %s.", funcSubExprVec[0]->getOriginalString().c_str());
            return NULL;
        }
    }
    if (ET_ARGUMENT == funcSubExprVec[1]->getExpressionType()) {
        auto expr2 = funcSubExprVec[1];
        if (!expr2 || !expr2->getConstValue(rhs)) {
            AUTIL_LOG(
                WARN, "invalid arg expression for in function: %s.", funcSubExprVec[1]->getOriginalString().c_str());
            return NULL;
        }
    } else {
        attrExpr2 = dynamic_cast<AttributeExpressionTyped<T> *>(funcSubExprVec[1]);
        if (!attrExpr2) {
            AUTIL_LOG(
                WARN, "unexpected expression for in function: %s.", funcSubExprVec[1]->getOriginalString().c_str());
            return NULL;
        }
    }
    return new InterfaceType<T>(attrExpr1, attrExpr2, lhs, rhs);
}

template <template <typename TT> class InterfaceType>
FunctionInterface *TypedBinaryFuncCreator::createTypedFunction(const FunctionSubExprVec &funcSubExprVec) {
    if (funcSubExprVec.size() != 2) {
        AUTIL_LOG(WARN, "function can't accept more than two arg.");
        return NULL;
    }
#define CREATE_TYPED_FUNCTION(vt_type)                                                                                 \
    case vt_type: {                                                                                                    \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;                                                    \
        return createTypedFunction<T, InterfaceType>(funcSubExprVec);                                                  \
    }
    // for const value
    auto vt = funcSubExprVec[0]->getType();
    if (funcSubExprVec[0]->getExpressionType() == ET_ARGUMENT) {
        if (funcSubExprVec[1]->getExpressionType() != ET_ARGUMENT) {
            vt = funcSubExprVec[1]->getType();
        } else {
            vt = vt_float;
        }
    }

    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(CREATE_TYPED_FUNCTION);
    default:
        AUTIL_LOG(WARN, "function can't accept %s type arg.", vt2TypeName(vt).c_str());
        return NULL;
    }
#undef CREATE_TYPED_FUNCTION
    return NULL;
}

FunctionInterface *PowFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    return TypedBinaryFuncCreator::createTypedFunction<PowFuncInterface>(funcSubExprVec);
}

FunctionInterface *LogFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    return TypedBinaryFuncCreator::createTypedFunction<LogFuncInterface>(funcSubExprVec);
}

} // namespace turing
} // namespace suez
