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
#ifndef ISEARCH_EXPRESSION_SUBFUNCTIONCREATORWRAPPER_H
#define ISEARCH_EXPRESSION_SUBFUNCTIONCREATORWRAPPER_H

#include "expression/common.h"
#include "expression/function/FunctionCreator.h"
#include "expression/function/SubFuncInterface.h"
#include "expression/framework/TypeInfo.h"

namespace expression {

class SubFunctionCreatorWrapper
{
public:
    SubFunctionCreatorWrapper();
    ~SubFunctionCreatorWrapper();

public:
    template<template<typename T> class ProcessorWrapper>
    static FunctionInterface *createTypedFunction(
            const AttributeExpressionVec& funcSubExprVec,
            const FunctionProtoInfo &funcProto);

    static FunctionInterface *createSubCountFunction(
            const AttributeExpressionVec& funcSubExprVec,
            const FunctionProtoInfo &funcProto);

    static FunctionInterface *createSubJoinVarFunction(
            const AttributeExpressionVec& funcSubExprVec,
            const std::string &split);

    static FunctionInterface *createSubJoinFunction(
            const AttributeExpressionVec& funcSubExprVec,
            const std::string &split);
            
private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////
template<template <typename T> class ProcessorWrapper>
FunctionInterface *SubFunctionCreatorWrapper::createTypedFunction(
        const AttributeExpressionVec& funcSubExprVec,
        const FunctionProtoInfo &funcProto)
{
    if (funcSubExprVec.size() != 1 || funcProto.argCount != 1)
    {
        AUTIL_LOG(WARN, "function expect: [%d] args, actual: [%d] args.",
                funcProto.argCount, (int)funcSubExprVec.size());
        return NULL;
    }

    if (funcSubExprVec[0]->isMulti()) {
        AUTIL_LOG(WARN, "function can't accept multivalue arg.");
        return NULL;
    }
    if (!funcSubExprVec[0]->isSubExpression()) {
        AUTIL_LOG(WARN, "%s is not a sub doc expression.",
                funcSubExprVec[0]->getExprString().c_str());
        return NULL;
    }

#define CREATE_TYPED_FUNCTION(evt_type)                                 \
    case evt_type:                                                      \
    {                                                                   \
        typedef ExprValueType2AttrValueType<evt_type, false>::AttrValueType T;  \
        AttributeExpressionTyped<T> *attrExpr =                         \
            dynamic_cast<AttributeExpressionTyped<T> *>(funcSubExprVec[0]); \
        if (!attrExpr) {                                                \
            AUTIL_LOG(WARN, "unexpected expression for in function: %s.", \
                      funcSubExprVec[0]->getExprString().c_str());      \
            return NULL;                                                \
        }                                                               \
        typedef ProcessorWrapper<T> Wrapper;                            \
        typedef std::shared_ptr<typename Wrapper::type> ProcessorPtr; \
        ProcessorPtr processor(new typename Wrapper::type(attrExpr, Wrapper::INIT_VALUE)); \
        return new SubFuncInterface<T, typename Wrapper::type>(processor); \
    }

    ExprValueType evt = funcSubExprVec[0]->getExprValueType();
    switch(evt) {
        NUMERIC_EXPR_VALUE_TYPE_MACRO_HELPER(CREATE_TYPED_FUNCTION);
    default:
        AUTIL_LOG(WARN, "function can't accept %d type arg.",
                  (int32_t)evt);
        return NULL;
    }
#undef CREATE_TYPED_FUNCTION
    
    return NULL;
}


}

#endif //ISEARCH_EXPRESSION_SUBFUNCTIONCREATORWRAPPER_H
