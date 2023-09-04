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

#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/plugin/ModuleInfo.h"
#include "indexlib/config/module_info.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/function/FunctionInfo.h"

namespace suez {
namespace turing {

class FuncConfig : public autil::legacy::Jsonizable {
public:
    FuncConfig();
    ~FuncConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("modules", _modules, _modules);
        json.Jsonize("functions", _functionInfos, _functionInfos);
        json.Jsonize("cava_functions", _cavaFunctionInfos, _cavaFunctionInfos);
    }
    bool merge(const FuncConfig &other) {
        if (!mergeModules(other._modules)) {
            return false;
        }
        if (!mergeFunctionInfos(_functionInfos, other._functionInfos)) {
            return false;
        }
        if (!mergeFunctionInfos(_cavaFunctionInfos, other._cavaFunctionInfos)) {
            return false;
        }
        return true;
    }

private:
    bool mergeModules(const build_service::plugin::ModuleInfos &modules) {
        for (auto module : modules) {
            if (findModule(module)) {
                AUTIL_LOG(ERROR, "module name [%s] conflict", module.moduleName.c_str());
                return false;
            }
            _modules.push_back(module);
        }
        return true;
    }

    bool findModule(const build_service::plugin::ModuleInfo &findModule) {
        for (auto module : _modules) {
            if (module.moduleName == findModule.moduleName) {
                return true;
            }
        }
        return false;
    }

    bool mergeFunctionInfos(std::vector<FunctionInfo> &targetInfos, const std::vector<FunctionInfo> &sourceInfos) {
        for (auto functionInfo : sourceInfos) {
            if (findFunctionInfo(targetInfos, functionInfo)) {
                AUTIL_LOG(ERROR, "function name [%s] conflict", functionInfo.funcName.c_str());
                return false;
            }
            targetInfos.push_back(functionInfo);
        }
        return true;
    }

    bool findFunctionInfo(const std::vector<FunctionInfo> &targetInfos, const FunctionInfo &functionInfo) {
        for (auto it : targetInfos) {
            if (it.funcName == functionInfo.funcName) {
                return true;
            }
        }
        return false;
    }

public:
    build_service::plugin::ModuleInfos _modules;
    std::vector<FunctionInfo> _functionInfos;
    std::vector<FunctionInfo> _cavaFunctionInfos;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(FuncConfig);

} // namespace turing
} // namespace suez
