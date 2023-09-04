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
#include "suez/turing/expression/function/SubFuncInterface.h"

#include <algorithm>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/function/FunctionMap.h"
#include "suez/turing/expression/function/ProcessorWrapper.h"

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, SubJoinVarFuncInterface);
AUTIL_LOG_SETUP(expression, SubJoinFuncInterfaceCreatorImpl);

class TypedSubFuncCreator {
public:
    template <template <typename T, typename ValueRef> class ProcessorWrapper>
    static FunctionInterface *createTypedFunction(const FunctionSubExprVec &funcSubExprVec,
                                                  const FunctionProtoInfo &funcProto);

    static FunctionInterface *createSubJoinFunction(const FunctionSubExprVec &funcSubExprVec, const std::string &split);
    static FunctionInterface *createSubJoinVarFunction(const FunctionSubExprVec &funcSubExprVec,
                                                       const std::string &split);

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(expression, TypedSubFuncCreator);

template <template <typename T, typename ValueRef> class ProcessorWrapper>
FunctionInterface *TypedSubFuncCreator::createTypedFunction(const FunctionSubExprVec &funcSubExprVec,
                                                            const FunctionProtoInfo &funcProto) {
    if (funcProto != FUNCTION_UNLIMITED_ARGUMENT_COUNT && funcSubExprVec.size() != funcProto.argCount) {
        AUTIL_LOG(
            WARN, "function expect: [%d] args, actual: [%d] args.", funcProto.argCount, (int)funcSubExprVec.size());
        return NULL;
    }
    if (funcProto.argCount == 0) {
        typedef ProcessorWrapper<uint32_t, AttributeExpressionTyped<uint32_t>> Wrapper;
        typename Wrapper::type processor(NULL, Wrapper::INIT_VALUE);
        return new SubFuncInterface<typename Wrapper::result_type, typename Wrapper::type>(processor);
    }
    if (funcSubExprVec[0]->isMultiValue()) {
        AUTIL_LOG(WARN, "function can't accept multivalue arg.");
        return NULL;
    }
    if (!funcSubExprVec[0]->isSubExpression()) {
        AUTIL_LOG(WARN, "%s is not a sub doc expression.", funcSubExprVec[0]->getOriginalString().c_str());
        return NULL;
    }

#define CREATE_TYPED_FUNCTION(vt_type)                                                                                 \
    case vt_type: {                                                                                                    \
        typedef VariableTypeTraits<vt_type, false>::AttrExprType T;                                                    \
        AttributeExpressionTyped<T> *attrExpr = dynamic_cast<AttributeExpressionTyped<T> *>(funcSubExprVec[0]);        \
        if (!attrExpr) {                                                                                               \
            AUTIL_LOG(                                                                                                 \
                WARN, "unexpected expression for in function: %s.", funcSubExprVec[0]->getOriginalString().c_str());   \
            return NULL;                                                                                               \
        }                                                                                                              \
        typedef ProcessorWrapper<T, AttributeExpressionTyped<T>> Wrapper;                                              \
        typename Wrapper::type processor(attrExpr, Wrapper::INIT_VALUE);                                               \
        return new SubFuncInterface<typename Wrapper::result_type, typename Wrapper::type>(processor);                 \
    }

    auto vt = funcSubExprVec[0]->getType();
    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(CREATE_TYPED_FUNCTION);
    default:
        AUTIL_LOG(WARN, "function can't accept %s type arg.", vt2TypeName(vt).c_str());
        return NULL;
    }
#undef CREATE_TYPED_FUNCTION

    return NULL;
}

FunctionInterface *TypedSubFuncCreator::createSubJoinFunction(const FunctionSubExprVec &funcSubExprVec,
                                                              const std::string &split) {
    if (!funcSubExprVec[0]->isSubExpression()) {
        AUTIL_LOG(WARN, "%s is not a sub doc expression.", funcSubExprVec[0]->getOriginalString().c_str());
        return NULL;
    }

#define CREATE_JOIN_FUNCTION(vt_type)                                                                                  \
    case vt_type: {                                                                                                    \
        if (funcSubExprVec[0]->isMultiValue()) {                                                                       \
            typedef VariableTypeTraits<vt_type, true>::AttrExprType T;                                                 \
            AttributeExpressionTyped<T> *attrExpr = dynamic_cast<AttributeExpressionTyped<T> *>(funcSubExprVec[0]);    \
            if (!attrExpr) {                                                                                           \
                AUTIL_LOG(WARN,                                                                                        \
                          "unexpected expression for in function: %s.",                                                \
                          funcSubExprVec[0]->getOriginalString().c_str());                                             \
                return NULL;                                                                                           \
            }                                                                                                          \
            typedef JoinProcessorTypeWrapper<T, AttributeExpressionTyped<T>> Wrapper;                                  \
            Wrapper::type processor(attrExpr, split);                                                                  \
            return new SubFuncInterface<Wrapper::result_type, Wrapper::type>(processor);                               \
        } else {                                                                                                       \
            typedef VariableTypeTraits<vt_type, false>::AttrExprType T;                                                \
            AttributeExpressionTyped<T> *attrExpr = dynamic_cast<AttributeExpressionTyped<T> *>(funcSubExprVec[0]);    \
            if (!attrExpr) {                                                                                           \
                AUTIL_LOG(WARN,                                                                                        \
                          "unexpected expression for in function: %s.",                                                \
                          funcSubExprVec[0]->getOriginalString().c_str());                                             \
                return NULL;                                                                                           \
            }                                                                                                          \
            typedef JoinProcessorTypeWrapper<T, AttributeExpressionTyped<T>> Wrapper;                                  \
            Wrapper::type processor(attrExpr, split);                                                                  \
            return new SubFuncInterface<Wrapper::result_type, Wrapper::type>(processor);                               \
        }                                                                                                              \
    }

    auto vt = funcSubExprVec[0]->getType();
    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_STRING(CREATE_JOIN_FUNCTION);
    default:
        AUTIL_LOG(WARN, "function can't accept %s type arg.", vt2TypeName(vt).c_str());
        return NULL;
    }
#undef CREATE_JOIN_FUNCTION

    return NULL;
}

FunctionInterface *TypedSubFuncCreator::createSubJoinVarFunction(const FunctionSubExprVec &funcSubExprVec,
                                                                 const std::string &split) {
    auto argExpr = ET_ARGUMENT == funcSubExprVec[0]->getExpressionType() ? funcSubExprVec[0] : nullptr;

    if (!argExpr) {
        AUTIL_LOG(WARN, "function arg [%s] is not argument expr.", funcSubExprVec[0]->getOriginalString().c_str());
        return NULL;
    }
    std::string value;
    if (!argExpr->getConstValue(value)) {
        AUTIL_LOG(WARN, "function arg [%s] is not string value.", argExpr->getOriginalString().c_str());
        return NULL;
    }
    return new SubJoinVarFuncInterface(value, split);
}

FunctionInterface *SubFirstFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    return TypedSubFuncCreator::createTypedFunction<FirstProcessorTypeWrapper>(funcSubExprVec, getFuncProtoInfo());
}

FunctionInterface *SubMinFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    return TypedSubFuncCreator::createTypedFunction<MinProcessorTypeWrapper>(funcSubExprVec, getFuncProtoInfo());
}

FunctionInterface *SubMaxFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    return TypedSubFuncCreator::createTypedFunction<MaxProcessorTypeWrapper>(funcSubExprVec, getFuncProtoInfo());
}

FunctionInterface *SubSumFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    return TypedSubFuncCreator::createTypedFunction<SumProcessorTypeWrapper>(funcSubExprVec, getFuncProtoInfo());
}

FunctionInterface *SubCountFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    return TypedSubFuncCreator::createTypedFunction<CountProcessorTypeWrapper>(funcSubExprVec, getFuncProtoInfo());
}

FunctionInterface *SubJoinFuncInterfaceCreatorImpl::createFunction(const FunctionSubExprVec &funcSubExprVec) {
    if (funcSubExprVec.size() > 2) {
        AUTIL_LOG(WARN, "function expect: less than 2 args, actual: [%d] args.", (int)funcSubExprVec.size());
        return NULL;
    }

    std::string split(" ");
    if (funcSubExprVec.size() == 2) {
        auto argExpr = ET_ARGUMENT == funcSubExprVec[1]->getExpressionType() ? funcSubExprVec[1] : nullptr;
        if (!argExpr) {
            AUTIL_LOG(WARN, "function arg [%s] is not argument expr.", funcSubExprVec[0]->getOriginalString().c_str());
            return NULL;
        }
        if (!argExpr->getConstValue(split)) {
            AUTIL_LOG(WARN, "function arg [%s] is not string value.", argExpr->getOriginalString().c_str());
            return NULL;
        }
    }

    if (ET_ARGUMENT == funcSubExprVec[0]->getExpressionType()) {
        return TypedSubFuncCreator::createSubJoinVarFunction(funcSubExprVec, split);
    }
    return TypedSubFuncCreator::createSubJoinFunction(funcSubExprVec, split);
}

} // namespace turing
} // namespace suez
