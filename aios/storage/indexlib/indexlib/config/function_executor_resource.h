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

#include <memory>

#include "indexlib/config/module_info.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class FunctionExecutorParam : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("function_name", functionName);
        json.Jsonize("module_name", moduleName, moduleName);
        json.Jsonize("parameters", parameters, parameters);
    }
    std::string functionName;
    util::KeyValueMap parameters;
    std::string moduleName;
};

class FunctionExecutorResource : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("modules", moduleInfos, moduleInfos);
        json.Jsonize("functions", functionParams, functionParams);
    }

public:
    std::vector<config::ModuleInfo> moduleInfos;
    std::vector<FunctionExecutorParam> functionParams;
};

typedef std::shared_ptr<FunctionExecutorResource> FunctionExecutorResourcePtr;
}} // namespace indexlib::config
