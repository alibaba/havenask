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
#ifndef ISEARCH_EXPRESSION_BUILTINSUBFUNCTIONCREATOR_H
#define ISEARCH_EXPRESSION_BUILTINSUBFUNCTIONCREATOR_H

#include "expression/common.h"
#include "expression/function/SubFunctionCreatorWrapper.h"
#include "expression/function/BasicSubProcessorWrapper.h"
#include "expression/function/FunctionCreator.h"

namespace expression {

// for sub_first, sub_min, sub_max, sub_sum (argCount = 1)
#define DECLARE_BASIC_SUB_FUNCTION_CREATOR(classname, funcName, Processor) \
    DECLARE_TEMPLATE_TYPE_FUNCTION_CREATOR(classname, funcName, 1, FUNC_ACTION_SCOPE_MAIN_DOC); \
    class classname##CreatorImpl : public classname##Creator {          \
    public:                                                             \
        FunctionInterface *createFunction(                              \
                const AttributeExpressionVec& funcSubExprVec,           \
                AttributeExpressionCreator *creator)                    \
        {                                                               \
            return SubFunctionCreatorWrapper::createTypedFunction<Processor>( \
                    funcSubExprVec, getFuncProtoInfo());                \
        }                                                               \
        bool init(const KeyValueMap &funcParameter,                     \
                  const resource_reader::ResourceReaderPtr &resourceReader) \
        { return true; }                                                \
    };                                                                  \


DECLARE_BASIC_SUB_FUNCTION_CREATOR(SubFirstFuncInterface, "sub_first",
                                   FirstProcessorTypeWrapper);
DECLARE_BASIC_SUB_FUNCTION_CREATOR(SubMinFuncInterface, "sub_min",
                                   MinProcessorTypeWrapper);
DECLARE_BASIC_SUB_FUNCTION_CREATOR(SubMaxFuncInterface, "sub_max",
                                   MaxProcessorTypeWrapper);
DECLARE_BASIC_SUB_FUNCTION_CREATOR(SubSumFuncInterface, "sub_sum",
                                   SumProcessorTypeWrapper);

// for sub_count
AIOS_DECLARE_TEMPLATE_FUNCTION_CREATOR(SubCountFuncInterface, uint32_t, "sub_count", 0,
                                       FT_NORMAL, FUNC_ACTION_SCOPE_MAIN_DOC);
class SubCountFuncInterfaceCreatorImpl : public SubCountFuncInterfaceCreator {
public:
    /* override */ 
    FunctionInterface *createFunction(
            const AttributeExpressionVec& funcSubExprVec,
            AttributeExpressionCreator *creator);

    /* override */ 
    bool init(const KeyValueMap &funcParameter,
              const resource_reader::ResourceReaderPtr &resourceReader)
    { return true; }
};


// for sub_join
AIOS_DECLARE_TEMPLATE_FUNCTION_CREATOR(SubJoinFuncInterface, autil::MultiChar, "sub_join", 
                                       FUNCTION_UNLIMITED_ARGUMENT_COUNT, FT_NORMAL,
                                       FUNC_ACTION_SCOPE_MAIN_DOC);
class SubJoinFuncInterfaceCreatorImpl : public SubJoinFuncInterfaceCreator {
public:
    /* override */ 
    FunctionInterface *createFunction(
            const AttributeExpressionVec& funcSubExprVec,
            AttributeExpressionCreator *creator);

    /* override */ 
    bool init(const KeyValueMap &funcParameter,
              const resource_reader::ResourceReaderPtr &resourceReader)
    { return true; }
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_BUILTINSUBFUNCTIONCREATOR_H
