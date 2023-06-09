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

#include "autil/legacy/jsonizable.h"

namespace indexlibv2::index {

class FunctionConfig : public autil::legacy::Jsonizable
{
public:
    FunctionConfig();
    FunctionConfig(std::string name, std::string expression);
    ~FunctionConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    const std::string& GetExpression() const;
    const std::string& GetName() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

class AttributeFunctionConfig : public std::vector<std::shared_ptr<FunctionConfig>>
{
public:
    AttributeFunctionConfig();
    ~AttributeFunctionConfig();
    std::shared_ptr<FunctionConfig> GetFunctionConfig(const std::string& name) const;

public:
    inline static const std::string ALWAYS_TRUE_FUNCTION_NAME = "__always_true_function__";
    inline static const std::string CURRENT_SECOND_FUNCTION_NAME = "__current_second_function__";

private:
    inline static const std::string ALWAYS_TRUE_FUNCTION_EXPRESSION = "always_true()";
    inline static const std::string CURRENT_SECOND_FUNCTION_EXPRESSION = "current_second()";
};

} // namespace indexlibv2::index
