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
#include "SumOfFunction.h"

using namespace std;
using namespace suez::turing;

BEGIN_HAVENASK_UDF_NAMESPACE(sumof);

FunctionInterface *SumOfFunctionCreatorImpl::createFunction(
        const FunctionSubExprVec& funcSubExprVec)
{
    if (funcSubExprVec.size() == 0) {
        return NULL;
    }
    if (funcSubExprVec[0]->isMultiValue()) {
        return NULL;
    }
    VariableType vt = funcSubExprVec[0]->getType();
#define CREATE_SUM_FUNCTION_INTERFACE(type)                             \
    case type:                                                          \
    {                                                                   \
        typedef suez::turing::VariableTypeTraits<type, false>::AttrExprType T; \
        auto pAttrs = new vector<suez::turing::AttributeExpressionTyped<T>*>; \
        for (FunctionSubExprVec::const_iterator it = funcSubExprVec.begin(); \
             it != funcSubExprVec.end(); ++it)                          \
        {                                                               \
            pAttrs->push_back(dynamic_cast<suez::turing::AttributeExpressionTyped<T>*>(*it)); \
        }                                                               \
        return new SumOfFunction<T>(pAttrs);                         \
    }

    switch(vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(CREATE_SUM_FUNCTION_INTERFACE);
    default:
        return NULL;
    }
#undef CREATE_SUM_FUNCTION_INTERFACE
    return NULL;
}

END_HAVENASK_UDF_NAMESPACE(sumof);