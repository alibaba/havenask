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
#include "expression/function/SubFunctionCreatorWrapper.h"
#include "expression/function/BasicSubProcessorWrapper.h"
#include "expression/function/SubJoinProcessor.h"

using namespace std;

namespace expression {
AUTIL_LOG_SETUP(expression, SubFunctionCreatorWrapper);

SubFunctionCreatorWrapper::SubFunctionCreatorWrapper() {
}

SubFunctionCreatorWrapper::~SubFunctionCreatorWrapper() {
}

FunctionInterface *SubFunctionCreatorWrapper::createSubCountFunction(
        const AttributeExpressionVec& funcSubExprVec,
        const FunctionProtoInfo &funcProto)
{
    if (funcProto.argCount == 0 && funcSubExprVec.empty()) {
        CounterProcessorPtr processor(new CounterProcessor);
        return new SubFuncInterface<uint32_t, CounterProcessor>(processor);
    }
    AUTIL_LOG(WARN, "create subCountFunction failed!");
    return NULL;
}

FunctionInterface *SubFunctionCreatorWrapper::createSubJoinVarFunction(
        const AttributeExpressionVec& funcSubExprVec,
        const string &split)
{
    ArgumentAttributeExpression *argExpr =
        dynamic_cast<ArgumentAttributeExpression*>(funcSubExprVec[0]);
    if (!argExpr) {
        AUTIL_LOG(WARN, "function arg [%s] is not argument expr.",
                  funcSubExprVec[0]->getExprString().c_str());
        return NULL;
    }
    std::string value;
    if (!argExpr->getConstValue(value)) {
        AUTIL_LOG(WARN, "function arg [%s] is not string value.",
                  argExpr->getExprString().c_str());
        return NULL;
    }
    return new SubJoinVarFuncInterface(value, split);
}

FunctionInterface *SubFunctionCreatorWrapper::createSubJoinFunction(
        const AttributeExpressionVec& funcSubExprVec,
        const string &split)
{
    if (!funcSubExprVec[0]->isSubExpression()) {
        AUTIL_LOG(WARN, "%s is not a sub doc expression.",
                  funcSubExprVec[0]->getExprString().c_str());
        return NULL;
    }

#define CREATE_JOIN_FUNCTION(evt_type)                                  \
    case evt_type:                                                      \
    {                                                                   \
        if (funcSubExprVec[0]->isMulti()) {                             \
            typedef typename ExprValueType2AttrValueType<evt_type, true>::AttrValueType T; \
            AttributeExpressionTyped<T> *attrExpr =                              \
                dynamic_cast<AttributeExpressionTyped<T> *>(funcSubExprVec[0]);  \
            if (!attrExpr) {                                            \
                AUTIL_LOG(WARN, "unexpected expression for in function: %s.", \
                          funcSubExprVec[0]->getExprString().c_str());  \
                return NULL;                                            \
            }                                                           \
            typedef SubJoinProcessor<T> ProcessorType;                  \
            typedef std::shared_ptr<ProcessorType> ProcessorTypePtr; \
            ProcessorTypePtr processor(new ProcessorType(attrExpr, split)); \
            return new SubFuncInterface<autil::MultiChar, ProcessorType>(processor); \
        } else {                                                        \
            typedef typename ExprValueType2AttrValueType<evt_type, false>::AttrValueType T; \
            AttributeExpressionTyped<T> *attrExpr =                     \
                dynamic_cast<AttributeExpressionTyped<T> *>(funcSubExprVec[0]);     \
            if (!attrExpr) {                                            \
                AUTIL_LOG(WARN, "unexpected expression for in function: %s.", \
                        funcSubExprVec[0]->getExprString().c_str()); \
                return NULL;                                            \
            }                                                           \
            typedef SubJoinProcessor<T> ProcessorType;                  \
            typedef std::shared_ptr<ProcessorType> ProcessorTypePtr; \
            ProcessorTypePtr processor(new ProcessorType(attrExpr, split)); \
            return new SubFuncInterface<autil::MultiChar, ProcessorType>(processor); \
        }                                                               \
    }

    ExprValueType evt = funcSubExprVec[0]->getExprValueType();
    switch(evt) {
        NUMERIC_EXPR_VALUE_TYPE_MACRO_HELPER(CREATE_JOIN_FUNCTION);
    default:
        AUTIL_LOG(WARN, "function can't accept %d type arg.", int(evt));
        return NULL;
    }
#undef CREATE_JOIN_FUNCTION

    return NULL;
}

}
