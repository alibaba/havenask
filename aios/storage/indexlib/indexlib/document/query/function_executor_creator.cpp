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
#include "indexlib/document/query/function_executor_creator.h"

#include "indexlib/plugin/plugin_manager.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace indexlib::config;
using namespace indexlib::plugin;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, FunctionExecutorCreator);

FunctionExecutorCreator::FunctionExecutorCreator() {}

FunctionExecutorCreator::~FunctionExecutorCreator() {}

bool FunctionExecutorCreator::Init(const string& pluginPath, const FunctionExecutorResource& resource)
{
    mPluginPath = pluginPath;
    PluginManagerPtr pluginManager(new PluginManager(pluginPath));
    for (auto& module : resource.moduleInfos) {
        if (!pluginManager->addModule(module)) {
            IE_LOG(ERROR, "add user-define module [%s] fail.", ToJsonString(module).c_str());
            return false;
        }
        FunctionExecutorFactoryWrapperPtr factoryWrapper(new FunctionExecutorFactoryWrapper);
        if (!factoryWrapper->Init(pluginManager, module.moduleName)) {
            IE_LOG(ERROR, "init user-define function executor factory fail, [%s].", ToJsonString(module).c_str());
            return false;
        }
        mFactoryWrappers[module.moduleName] = factoryWrapper;
    }

    mBuiltinWrapper.reset(new FunctionExecutorFactoryWrapper);
    if (!mBuiltinWrapper->Init(pluginManager, "")) {
        IE_LOG(ERROR, "init builtin function executor factory wrapper fail.");
        return false;
    }

    for (auto& param : resource.functionParams) {
        if (param.moduleName.empty() && mFactoryWrappers.find(param.moduleName) == mFactoryWrappers.end()) {
            IE_LOG(ERROR, "moduleName [%s] not exist.", param.moduleName.c_str());
            return false;
        }
        if (mFuncName2ParamMap.find(param.functionName) != mFuncName2ParamMap.end()) {
            IE_LOG(ERROR, "duplicate function_name [%s] in functions.", param.functionName.c_str());
            return false;
        }
        mFuncName2ParamMap[param.functionName] = param;
    }
    return true;
}

FunctionExecutor* FunctionExecutorCreator::CreateFunctionExecutor(const QueryFunction& function)
{
    FunctionExecutor* ret = nullptr;
    auto iter = mFuncName2ParamMap.find(function.GetFunctionName());
    if (iter != mFuncName2ParamMap.end()) {
        string moduleName = iter->second.moduleName;
        if (moduleName.empty()) {
            IE_LOG(INFO, "create function [%s] from built-in factory with parameter [%s].", function.ToString().c_str(),
                   ToJsonString(iter->second.parameters).c_str());
            ret = mBuiltinWrapper->CreateFunctionExecutor(function, iter->second.parameters);
        } else {
            auto it = mFactoryWrappers.find(moduleName);
            if (it == mFactoryWrappers.end() || it->second.get() == nullptr) {
                IE_LOG(ERROR, "create function [%s] fail.", function.ToString().c_str());
                return nullptr;
            }

            IE_LOG(INFO, "create function [%s] from user-define factory module [%s], with param [%s].",
                   function.ToString().c_str(), moduleName.c_str(), ToJsonString(iter->second.parameters).c_str());
            ret = it->second->CreateFunctionExecutor(function, iter->second.parameters);
        }
        if (ret == nullptr) {
            IE_LOG(ERROR, "create function [%s] fail.", function.ToString().c_str());
        }
        return ret;
    }

    ret = mBuiltinWrapper->CreateFunctionExecutor(function, util::KeyValueMap());
    if (ret != nullptr) {
        IE_LOG(INFO, "create function [%s] from built-in factory.", function.ToString().c_str());
        return ret;
    }

    for (auto& item : mFactoryWrappers) {
        ret = item.second->CreateFunctionExecutor(function, util::KeyValueMap());
        if (ret != nullptr) {
            IE_LOG(INFO, "create function [%s] from user-define factory module [%s].", function.ToString().c_str(),
                   item.first.c_str());
            return ret;
        }
    }
    IE_LOG(ERROR, "create function [%s] fail.", function.ToString().c_str());
    return nullptr;
}
}} // namespace indexlib::document
