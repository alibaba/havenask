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
#pragma once

#include "autil/Log.h"
#include "suez/turing/expression/function/FunctionCreator.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "havenask_plugins/udf_plugins/utils/common.h"

BEGIN_HAVENASK_UDF_NAMESPACE(sumof);

template<typename T>
class SumOfFunction : public suez::turing::FunctionInterfaceTyped<T>
{
public:
    SumOfFunction(std::vector<suez::turing::AttributeExpressionTyped<T>*> *pAttrVec)
        : _pAttrVec(pAttrVec)
    {
    }
    ~SumOfFunction() {
        if (_pAttrVec) {
            delete _pAttrVec;
            _pAttrVec = NULL;
        }
    }
public:
    bool beginRequest(suez::turing::FunctionProvider *provider) {
        return true;
    }
    T evaluate(matchdoc::MatchDoc matchDoc) {
        T value = T();
        for (size_t i = 0; i < _pAttrVec->size(); ++i) {
            (void)(*_pAttrVec)[i]->evaluate(matchDoc);
            value += (*_pAttrVec)[i]->getValue(matchDoc);
        }
        return value;
    }
private:
    std::vector<suez::turing::AttributeExpressionTyped<T>*> *_pAttrVec;
private:
    AUTIL_LOG_DECLARE();
};

DECLARE_UNKNOWN_TYPE_FUNCTION_CREATOR(SumOfFunction, "sumOf", suez::turing::FUNCTION_UNLIMITED_ARGUMENT_COUNT);

class SumOfFunctionCreatorImpl : public FUNCTION_CREATOR_CLASS_NAME(SumOfFunction) {
public:
    /* override */ suez::turing::FunctionInterface *createFunction(
            const suez::turing::FunctionSubExprVec& funcSubExprVec);
};

END_HAVENASK_UDF_NAMESPACE(sumof);
