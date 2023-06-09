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
#ifndef UDF_PLUGINS_SUMFUNCINTERFACE_H
#define UDF_PLUGINS_SUMFUNCINTERFACE_H

#include <ha3/isearch.h>
#include <autil/Log.h>
#include <suez/turing/expression/function/FunctionInterface.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <suez/turing/expression/function/FunctionCreator.h>

namespace pluginplatform {
namespace udf_plugins {

template<typename T>
class SumFuncInterface : public suez::turing::FunctionInterfaceTyped<T>
{
public:
    SumFuncInterface(std::vector<suez::turing::AttributeExpressionTyped<T>*> *pAttrVec)
        : _pAttrVec(pAttrVec)
    {
    }
    ~SumFuncInterface() {
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

DECLARE_UNKNOWN_TYPE_FUNCTION_CREATOR(SumFuncInterface, "sumAll", suez::turing::FUNCTION_UNLIMITED_ARGUMENT_COUNT);

class SumFuncInterfaceCreatorImpl : public FUNCTION_CREATOR_CLASS_NAME(SumFuncInterface) {
public:
    /* override */ suez::turing::FunctionInterface *createFunction(
            const suez::turing::FunctionSubExprVec& funcSubExprVec);
};

}}

#endif //UDF_PLUGINS_SUMFUNCINTERFACE_H