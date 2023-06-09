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
#include "indexlib/index/attribute/expression/FunctionConfig.h"

namespace indexlibv2::index {
struct FunctionConfig::Impl {
    std::string name;
    std::string expression;
    Impl() {}
    Impl(std::string name_, std::string expression_) : name(name_), expression(expression_) {}
};

FunctionConfig::FunctionConfig() : _impl(std::make_unique<Impl>()) {}
FunctionConfig::FunctionConfig(std::string name, std::string expression)
    : _impl(std::make_unique<Impl>(name, expression))
{
}

FunctionConfig::~FunctionConfig() {}

const std::string& FunctionConfig::GetExpression() const { return _impl->expression; }
const std::string& FunctionConfig::GetName() const { return _impl->name; }
void FunctionConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("name", _impl->name, _impl->name);
    json.Jsonize("expression", _impl->expression, _impl->expression);
}

AttributeFunctionConfig::AttributeFunctionConfig() {}

AttributeFunctionConfig::~AttributeFunctionConfig() {}

std::shared_ptr<FunctionConfig> AttributeFunctionConfig::GetFunctionConfig(const std::string& name) const
{
    for (const auto& functionConfig : *this) {
        if (name == functionConfig->GetName()) {
            return functionConfig;
        }
    }
    static std::vector<std::pair<std::string, std::string>> builtinFunctions = {
        {ALWAYS_TRUE_FUNCTION_NAME, ALWAYS_TRUE_FUNCTION_EXPRESSION},
        {CURRENT_SECOND_FUNCTION_NAME, CURRENT_SECOND_FUNCTION_EXPRESSION}};

    for (const auto& [builtinName, builtinExpression] : builtinFunctions) {
        if (name == builtinName) {
            return std::make_shared<FunctionConfig>(builtinName, builtinExpression);
        }
    }
    return nullptr;
}

} // namespace indexlibv2::index
