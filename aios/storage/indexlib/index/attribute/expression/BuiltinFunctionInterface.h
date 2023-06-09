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

#include "expression/function/FunctionCreator.h"
#include "expression/function/FunctionInterface.h"
#include "matchdoc/MatchDoc.h"
namespace indexlibv2::index {
template <typename Policy>
class BuiltinFunction : public expression::FunctionInterfaceTyped<typename Policy::ResultType>
{
public:
    BuiltinFunction() { _policy.Init(); }
    ~BuiltinFunction() {}

public:
    typename Policy::ResultType evaluate(const matchdoc::MatchDoc& matchDoc) override { return _policy.Func(); }
    void batchEvaluate(matchdoc::MatchDoc* matchDocs, uint32_t docCount) override { assert(false); }

public:
    class BuiltinFunctionCreator : public expression::FunctionCreator
    {
    public:
        TEMPLATE_FUNCTION_DESCRIBE_DEFINE(typename Policy::ResultType, Policy::FuncName(),
                                          expression::FUNCTION_UNLIMITED_ARGUMENT_COUNT, expression::FT_NORMAL,
                                          expression::FUNC_ACTION_SCOPE_ADAPTER, expression::FUNC_BATCH_CUSTOM_MODE);

    public:
        bool init(const expression::KeyValueMap& funcParameter,
                  const resource_reader::ResourceReaderPtr& resourceReader) override
        {
            return true;
        }

        expression::FunctionInterface* createFunction(const expression::AttributeExpressionVec& funcSubExprVec,
                                                      expression::AttributeExpressionCreator* creator) override
        {
            return new BuiltinFunction();
        }
    };

private:
    Policy _policy;
};

} // namespace indexlibv2::index
