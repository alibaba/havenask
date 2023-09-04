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
#include "suez/turing/expression/function/HammingFuncInterface.h"

#include <stdint.h>

#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

namespace suez {
namespace turing {

template <typename T1, typename T2, typename R>
HammingFuncInterface<T1, T2, R>::HammingFuncInterface(AttributeExpressionTyped<T1> *expr1,
                                                      AttributeExpressionTyped<T2> *expr2)
    : _expr1(expr1), _expr2(expr2) {}

template <typename T1, typename T2, typename R>
HammingFuncInterface<T1, T2, R>::~HammingFuncInterface() {}

template <typename T1, typename T2, typename R>
bool HammingFuncInterface<T1, T2, R>::beginRequest(suez::turing::FunctionProvider *functionProvider) {
    assert(functionProvider);
    _pool = functionProvider->getPool();
    if (!_expr1) {
        AUTIL_LOG(ERROR, "unexpected expr1 cast failed");
        return false;
    }
    if (!_expr2) {
        AUTIL_LOG(ERROR, "unexpected expr2 cast failed");
        return false;
    }
    return true;
}

template <typename T1, typename T2, typename R>
R HammingFuncInterface<T1, T2, R>::evaluate(matchdoc::MatchDoc matchdoc) {
    T1 val1 = _expr1->evaluateAndReturn(matchdoc);
    T2 val2 = _expr2->evaluateAndReturn(matchdoc);
    typedef typename UnsignedTypeTraits<T1>::UnsignedType T;
    R hamming_distance;
    if constexpr (std::is_same_v<R, uint16_t>) {
        hamming_distance = (uint16_t)__builtin_popcountll((T)val1 ^ (T)val2);
    } else {
        size_t val2Len = val2.size();
        uint16_t *data = (uint16_t *)_pool->allocate(val2Len * sizeof(uint16_t));
        if (nullptr == data) {
            return hamming_distance;
        }
        for (size_t i = 0; i < val2Len; ++i) {
            data[i] = (uint16_t)__builtin_popcountll((T)val1 ^ (T)val2[i]);
        }
        hamming_distance.init(autil::MultiValueCreator::createMultiValueBuffer<uint16_t>(data, val2Len, _pool));
    }
    return hamming_distance;
}

AUTIL_LOG_SETUP_TEMPLATE_3(function, HammingFuncInterface, T1, T2, R);

/*************************************************************************************************************/
FunctionInterface *HammingFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    if (funcSubExprVec.size() != 2) {
        AUTIL_LOG(ERROR, "function hamming need two args");
        return nullptr;
    }

    VariableType vt1 = funcSubExprVec[0]->getType();
    VariableType vt2 = funcSubExprVec[1]->getType();

    if (vt1 != vt2) {
        AUTIL_LOG(ERROR,
                  "function hamming need same type, but get hamming(%s, %s)",
                  vt2TypeName(vt1).c_str(),
                  vt2TypeName(vt2).c_str());
        return nullptr;
    }

    if (!funcSubExprVec[0]->isMultiValue() && !funcSubExprVec[1]->isMultiValue()) { // two single_value
#define TYPE1_SINGLE_TYPE2_SINGLE(vt_type)                                                                             \
    case vt_type: {                                                                                                    \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;                                                    \
        AttributeExpressionTyped<T> *expr1 = dynamic_cast<AttributeExpressionTyped<T> *>(funcSubExprVec[0]);           \
        AttributeExpressionTyped<T> *expr2 = dynamic_cast<AttributeExpressionTyped<T> *>(funcSubExprVec[1]);           \
        return new HammingFuncInterface<T, T, uint16_t>(expr1, expr2);                                                 \
    }

        switch (vt1) {
            INTEGER_VARIABLE_TYPE_MACRO_HELPER(TYPE1_SINGLE_TYPE2_SINGLE);
        default:
            AUTIL_LOG(ERROR, "function hamming: unsupport vt_type %s", vt2TypeName(vt1).c_str());
            return nullptr;
        }
#undef TYPE1_SINGLE_TYPE2_SINGLE

    } else if (!funcSubExprVec[0]->isMultiValue() && funcSubExprVec[1]->isMultiValue()) { // single_value, multi_value
#define TYPE1_SINGLE_TYPE2_MULTI(vt_type)                                                                              \
    case vt_type: {                                                                                                    \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T1;                                                   \
        typedef VariableTypeTraits<vt_type, true>::AttrExprType T2;                                                    \
        AttributeExpressionTyped<T1> *expr1 = dynamic_cast<AttributeExpressionTyped<T1> *>(funcSubExprVec[0]);         \
        AttributeExpressionTyped<T2> *expr2 = dynamic_cast<AttributeExpressionTyped<T2> *>(funcSubExprVec[1]);         \
        return new HammingFuncInterface<T1, T2, autil::MultiUInt16>(expr1, expr2);                                     \
    }

        switch (vt1) {
            INTEGER_VARIABLE_TYPE_MACRO_HELPER(TYPE1_SINGLE_TYPE2_MULTI);
        default:
            AUTIL_LOG(ERROR, "function hamming: unsupport vt_type %s", vt2TypeName(vt1).c_str());
            return nullptr;
        }
#undef TYPE1_SINGLE_TYPE2_MULTI

    } else {
        AUTIL_LOG(ERROR, "function hamming only support (single,single) or (single,multi))");
        return nullptr;
    }
}
AUTIL_LOG_SETUP(function, HammingFuncInterfaceCreatorImpl);
} // namespace turing
} // namespace suez