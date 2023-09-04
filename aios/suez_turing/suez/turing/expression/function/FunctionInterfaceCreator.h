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

#include <map>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "build_service/plugin/PlugInManager.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/function/BuildInFunctionFactory.h"
#include "suez/turing/expression/function/FuncConfig.h"
#include "suez/turing/expression/function/FunctionCreatorResource.h"
#include "suez/turing/expression/function/FunctionInfo.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/function/FunctionMap.h"

namespace suez {
namespace turing {
class FunctionCreator;
class SyntaxExpressionFactory;

class FunctionInterfaceCreator {
public:
    FunctionInterfaceCreator();
    ~FunctionInterfaceCreator();

private:
    FunctionInterfaceCreator(const FunctionInterfaceCreator &);
    FunctionInterfaceCreator &operator=(const FunctionInterfaceCreator &);

public:
    bool init(const FuncConfig &funcConfig,
              const FunctionCreatorResource &funcCreatorResource,
              bool initFunctionCreator = true);
    bool init(const FuncConfig &funcConfig,
              const FunctionCreatorResource &funcCreatorResource,
              const std::vector<SyntaxExpressionFactory *> &addFactorys);
    FunctionInterface *createFuncExpression(const std::string &funcName,
                                            const FunctionSubExprVec &funcSubExprVec) const;

    const FuncInfoMap &getFuncInfoMap() const { return _funcInfoMap; }
    FunctionCreator *getFuncCreator(const std::string &name) {
        auto itr = _funcCreatorMap.find(name);
        if (itr != _funcCreatorMap.end()) {
            return itr->second;
        }
        return NULL;
    }
    FunctionCreator *getFuncCreatorFromFuncFactorys(const std::string &name) const;

private:
    typedef std::map<std::string, SyntaxExpressionFactory *> FactoryInfos;

private:
    bool funcParamToMap(const std::vector<FunctionInfo> &funcInfos, std::map<std::string, KeyValueMap> &funcKVMap);

    bool addFunctions(SyntaxExpressionFactory *factory,
                      const std::map<std::string, KeyValueMap> &allFuncKVMap,
                      const FunctionCreatorResource &funcCreatorResource,
                      const std::vector<FunctionInfo> &functionInfos);
    bool addFactory(SyntaxExpressionFactory *factory,
                    const FuncConfig &funcConfig,
                    const std::map<std::string, KeyValueMap> &funcKVMap,
                    bool initFunctionCreator);
    bool initPluginFactory(const FuncConfig &funcConfig,
                           const std::map<std::string, KeyValueMap> &funcKVMap,
                           bool initFunctionCreator);

    bool addFuncCreators(SyntaxExpressionFactory *factory);

    bool addFuncInfoMap(SyntaxExpressionFactory *factory);

    // get all FunctionInfos in module, which class_name != name and class_name not empty
    std::vector<FunctionInfo> getFunctionInfo(const FuncConfig &funcConfig, const std::string &moduleName) {
        std::vector<FunctionInfo> ret;
        for (auto functionInfo : funcConfig._functionInfos) {
            if (functionInfo.moduleName == moduleName && !functionInfo.className.empty() &&
                functionInfo.className != functionInfo.funcName) {
                ret.push_back(functionInfo);
            }
        }
        return ret;
    }

private:
    build_service::plugin::PlugInManagerPtr _plugInManagerPtr;
    FunctionCreatorResource _funcCreatorResource;
    FactoryInfos _factorys;
    BuildInFunctionFactory _buildInFactory;
    FuncInfoMap _funcInfoMap;
    std::map<std::string, FunctionCreator *> _funcCreatorMap;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(FunctionInterfaceCreator);

} // namespace turing
} // namespace suez
