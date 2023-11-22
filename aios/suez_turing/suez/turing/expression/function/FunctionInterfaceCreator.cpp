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
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/plugin/DllWrapper.h"
#include "build_service/plugin/Module.h"
#include "build_service/plugin/ModuleFactory.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/plugin/PlugInManager.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/function/BuildInFunctionFactory.h"
#include "suez/turing/expression/function/FuncConfig.h"
#include "suez/turing/expression/function/FunctionCreator.h"
#include "suez/turing/expression/function/FunctionCreatorResource.h"
#include "suez/turing/expression/function/FunctionInfo.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/function/FunctionMap.h"
#include "suez/turing/expression/function/SyntaxExpressionFactory.h"

using namespace std;
using namespace build_service::plugin;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, FunctionInterfaceCreator);

FunctionInterfaceCreator::FunctionInterfaceCreator() {}

FunctionInterfaceCreator::~FunctionInterfaceCreator() { _factorys.clear(); }

bool FunctionInterfaceCreator::addFactory(SyntaxExpressionFactory *factory,
                                          const FuncConfig &funcConfig,
                                          const std::map<std::string, KeyValueMap> &funcKVMap,
                                          bool initFunctionCreator) {
    AUTIL_LOG(INFO, "begin add factory");
    if (!factory->registeFunctions()) {
        AUTIL_LOG(ERROR, "registe function failed.");
        return false;
    }
    if (initFunctionCreator) {
        auto functionInfos = getFunctionInfo(funcConfig, "");
        if (!addFunctions(factory, funcKVMap, _funcCreatorResource, functionInfos)) {
            AUTIL_LOG(ERROR, "factory add Functions failed");
            return false;
        }
        if (!addFuncCreators(factory)) {
            AUTIL_LOG(ERROR, "addFuncCreators failed.");
            return false;
        }
    }
    if (!addFuncInfoMap(factory)) {
        return false;
    }

    AUTIL_LOG(INFO, "end add factory");
    return true;
} // namespace turing

bool FunctionInterfaceCreator::initPluginFactory(const FuncConfig &funcConfig,
                                                 const std::map<std::string, KeyValueMap> &funcKVMap,
                                                 bool initFunctionCreator) {
    AUTIL_LOG(INFO, "begin initPluginFactory");
    if (!_funcCreatorResource.resourceReader) {
        AUTIL_LOG(ERROR, "resource reader is empty.");
        return false;
    }
    _plugInManagerPtr.reset(new PlugInManager(_funcCreatorResource.resourceReader, MODULE_FUNC_FUNCTION));
    if (!_plugInManagerPtr->addModules(funcConfig._modules)) {
        AUTIL_LOG(ERROR, "load function modules failed : %s.", FastToJsonString(funcConfig._modules).c_str());
        return false;
    }
    auto modules = funcConfig._modules;
    // add empty module
    modules.emplace_back(ModuleInfo());
    size_t sz = modules.size();
    for (size_t i = 0; i < sz; ++i) {
        const string &moduleName = modules[i].moduleName;
        Module *module = NULL;
        if (!moduleName.empty()) {
            module = _plugInManagerPtr->getModule(moduleName);
        } else {
            module = _plugInManagerPtr->getModule();
        }
        if (module == NULL) {
            if (moduleName.empty()) {
                continue;
            }
            AUTIL_LOG(ERROR, "Get module [%s] failed", moduleName.c_str());
            return false;
        }
        SyntaxExpressionFactory *factory = dynamic_cast<SyntaxExpressionFactory *>(module->getModuleFactory());
        if (!factory) {
            AUTIL_LOG(ERROR, "invalid SyntaxExpressionFactory in module[%s].", moduleName.c_str());
            return false;
        }
        if (!factory->registeFunctions()) {
            AUTIL_LOG(ERROR, "registe function failed.");
            return false;
        }
        if (initFunctionCreator) {
            AUTIL_LOG(INFO, "add function module: %s", moduleName.c_str());
            auto functionInfoVec = getFunctionInfo(funcConfig, moduleName);
            if (!addFunctions(factory, funcKVMap, _funcCreatorResource, functionInfoVec)) {
                AUTIL_LOG(ERROR, "module[%s] add Functions failed", moduleName.c_str());
                return false;
            }
            if (!addFuncCreators(factory)) {
                AUTIL_LOG(ERROR, "addFuncCreators failed.");
                return false;
            }
        }
        if (!addFuncInfoMap(factory)) {
            return false;
        }
    }
    AUTIL_LOG(INFO, "end initPluginFactory");
    return true;
}

bool FunctionInterfaceCreator::init(const FuncConfig &funcConfig,
                                    const FunctionCreatorResource &funcCreatorResource,
                                    bool initFunctionCreator) {
    const vector<FunctionInfo> &funcInfos = funcConfig._functionInfos;
    _funcCreatorResource = funcCreatorResource;
    map<string, KeyValueMap> funcKVMap;
    if (!funcParamToMap(funcInfos, funcKVMap)) {
        AUTIL_LOG(INFO, "funcParamToMap failed");
        return false;
    }
    if (!addFactory(&_buildInFactory, funcConfig, funcKVMap, initFunctionCreator)) {
        AUTIL_LOG(INFO, "initBuildInFactory");
        return false;
    }
    if (!initPluginFactory(funcConfig, funcKVMap, initFunctionCreator)) {
        AUTIL_LOG(INFO, "initPluginFactory failed");
        return false;
    }
    AUTIL_LOG(INFO, "init success");
    return true;
}

bool FunctionInterfaceCreator::init(const FuncConfig &funcConfig,
                                    const FunctionCreatorResource &funcCreatorResource,
                                    const std::vector<SyntaxExpressionFactory *> &addFactorys) {
    bool initFunctionCreator = true;
    const vector<FunctionInfo> &funcInfos = funcConfig._functionInfos;
    _funcCreatorResource = funcCreatorResource;
    map<string, KeyValueMap> funcKVMap;
    if (!funcParamToMap(funcInfos, funcKVMap)) {
        AUTIL_LOG(ERROR, "funcParamToMap failed");
        return false;
    }
    if (!addFactory(&_buildInFactory, funcConfig, funcKVMap, initFunctionCreator)) {
        AUTIL_LOG(ERROR, "add buildin factory failed");
        return false;
    }
    for (auto factory : addFactorys) {
        if (factory && !addFactory(factory, funcConfig, funcKVMap, initFunctionCreator)) {
            AUTIL_LOG(ERROR, "add factory failed [%s]", typeid(*factory).name());
            return false;
        }
    }
    if (!initPluginFactory(funcConfig, funcKVMap, initFunctionCreator)) {
        AUTIL_LOG(ERROR, "initPluginFactory failed");
        return false;
    }
    AUTIL_LOG(INFO, "init success");
    return true;
}

bool FunctionInterfaceCreator::addFunctions(SyntaxExpressionFactory *factory,
                                            const map<string, KeyValueMap> &allFuncKVMap,
                                            const FunctionCreatorResource &funcCreatorResource,
                                            const vector<FunctionInfo> &functionInfos) {
    for (auto functionInfo : functionInfos) {
        auto &name = functionInfo.className;
        auto functionCreator = factory->getFuncCreator(name);
        if (!functionCreator) {
            AUTIL_LOG(ERROR,
                      "getFuncCreator class name %s, function name %s failed.",
                      name.c_str(),
                      functionInfo.funcName.c_str());
            return false;
        }
        functionCreator = functionCreator->clone();
        if (!functionCreator) {
            AUTIL_LOG(ERROR,
                      "class name %s need clone function, add function name %s failed.",
                      name.c_str(),
                      functionInfo.funcName.c_str());
            return false;
        }

        if (!factory->registeFunction(functionInfo.funcName, functionCreator)) {
            delete functionCreator;
            AUTIL_LOG(
                ERROR, "register class name %s, function name %s failed.", name.c_str(), functionInfo.funcName.c_str());
            return false;
        }
    }
    const FuncInfoMap &funcMap = factory->getFuncInfos();
    map<string, KeyValueMap> funcKVMap;

    for (FuncInfoMap::const_iterator it = funcMap.begin(); it != funcMap.end(); ++it) {
        const string &funcName = it->first;
        FactoryInfos::iterator fIt = _factorys.find(funcName);
        if (fIt != _factorys.end() && fIt->second != factory) {
            AUTIL_LOG(ERROR, "Failed to init function plugins: conflict function name[%s].", funcName.c_str());
            return false;
        }
        _factorys[funcName] = factory;
        map<string, KeyValueMap>::const_iterator pIt = allFuncKVMap.find(funcName);
        if (pIt != allFuncKVMap.end()) {
            funcKVMap[funcName] = pIt->second;
        }
        AUTIL_LOG(INFO, "add funcName[%s] to funcKVMap", funcName.c_str());
    }

    return factory->initFunctionCreators(funcKVMap, funcCreatorResource);
}

FunctionInterface *FunctionInterfaceCreator::createFuncExpression(const std::string &funcName,
                                                                  const FunctionSubExprVec &funcSubExprVec) const {
    FactoryInfos::const_iterator i = _factorys.find(funcName);
    if (i == _factorys.end()) {
        AUTIL_LOG(DEBUG, "getInterface for %s failed", funcName.c_str());
        return NULL;
    }
    return i->second->createFuncExpression(funcName, funcSubExprVec);
}

FunctionCreator *FunctionInterfaceCreator::getFuncCreatorFromFuncFactorys(const std::string &funcName) const {
    FactoryInfos::const_iterator i = _factorys.find(funcName);
    if (i == _factorys.end()) {
        AUTIL_LOG(DEBUG, "getInterface for %s failed", funcName.c_str());
        return NULL;
    }
    return i->second->getFuncCreator(funcName);
}

bool FunctionInterfaceCreator::funcParamToMap(const vector<FunctionInfo> &funcInfos,
                                              map<string, KeyValueMap> &funcKVMap) {
    for (vector<FunctionInfo>::const_iterator it = funcInfos.begin(); it != funcInfos.end(); ++it) {
        const string &funcName = it->funcName;
        const KeyValueMap &params = it->params;
        map<string, KeyValueMap>::const_iterator fIt = funcKVMap.find(funcName);
        if (fIt != funcKVMap.end()) {
            AUTIL_LOG(ERROR, "conflict parameters for function name[%s] found in modules.", funcName.c_str());
            return false;
        }
        funcKVMap[funcName] = params;
    }
    return true;
}

bool FunctionInterfaceCreator::addFuncCreators(SyntaxExpressionFactory *factory) {
    const vector<string> &funcList = factory->getFuncList();
    for (auto func : funcList) {
        auto itr = _funcCreatorMap.find(func);
        if (itr != _funcCreatorMap.end()) {
            AUTIL_LOG(ERROR, "instance [%s] have already exist", func.c_str());
            return false;
        }
        auto functionCreator = factory->getFuncCreator(func);
        if (!functionCreator) {
            AUTIL_LOG(ERROR, "getFuncCreator function name %s failed.", func.c_str());
            return false;
        }
        AUTIL_LOG(INFO, "add addFuncCreator[%s] to _funcCreatorMap", func.c_str());
        _funcCreatorMap[func] = functionCreator;
    }
    return true;
}

bool FunctionInterfaceCreator::addFuncInfoMap(SyntaxExpressionFactory *factory) {
    AUTIL_LOG(INFO, "begin addFuncInfoMap");
    const FuncInfoMap &funcMap = factory->getFuncInfos();
    for (FuncInfoMap::const_iterator it = funcMap.begin(); it != funcMap.end(); ++it) {
        const string &funcName = it->first;
        if (_funcInfoMap.find(funcName) != _funcInfoMap.end()) {
            AUTIL_LOG(ERROR, "Failed to init function plugins: conflict function name[%s].", funcName.c_str());
            return false;
        }
        const FunctionProtoInfo &funcProto = it->second;
        if (funcProto.resultType == vt_unknown && funcProto.argCount == 0) {
            AUTIL_LOG(ERROR,
                      "bad function[%s] info: result type should not be unknown"
                      "when arg count == 0.",
                      funcName.c_str());
            return false;
        }
        _funcInfoMap[it->first] = it->second;
        AUTIL_LOG(INFO, "add funcName[%s] to _funcInfoMap", funcName.c_str());
    }
    AUTIL_LOG(INFO, "end addFuncInfoMap");
    return true;
}

} // namespace turing
} // namespace suez
