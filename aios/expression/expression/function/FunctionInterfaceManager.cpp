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
#include "expression/function/FunctionInterfaceManager.h"
#include "expression/plugin/Module.h"
#include "expression/framework/AttributeExpressionCreator.h"
#include "expression/function/BuildInFunctionFactory.h"
#include "expression/plugin/PlugInManager.h"
#include "resource_reader/ResourceReader.h"

using namespace std;
using namespace resource_reader;

namespace expression {
AUTIL_LOG_SETUP(expression, FunctionInterfaceManager);

FunctionInterfaceManager::FunctionInterfaceManager() {
}

FunctionInterfaceManager::~FunctionInterfaceManager() {
    _factorys.clear();
    for (size_t i = 0; i < _nonPluginFactorys.size(); i++) {
        DELETE_AND_SET_NULL(_nonPluginFactorys[i]);
    }
    _nonPluginFactorys.clear();
}

bool FunctionInterfaceManager::init(const FunctionConfig &funcConfig,
                                    const ResourceReaderPtr &resourceReaderPtr,
                                    FunctionInterfaceFactory *factory)
{
    const vector<FunctionInfo> &funcInfos = funcConfig._functionInfos;
    map<string, KeyValueMap> funcKVMap;
    if (!funcParamToMap(funcInfos, funcKVMap)) {
        return false;
    }

    if (!addBuildInFunctions(funcKVMap, resourceReaderPtr)) {
        return false;
    }
    
    if (!addNonPluginFunctions(factory, funcKVMap, resourceReaderPtr, false)) {
        return false;
    }
 
    _plugInManagerPtr.reset(new PlugInManager(resourceReaderPtr, MODULE_FUNC_FUNCTION));
    if (!_plugInManagerPtr->addModules(funcConfig._modules)) {
        AUTIL_LOG(ERROR, "load function modules failed : %s.", ToJsonString(funcConfig._modules).c_str());
        return false;
    }
    size_t sz = funcConfig._modules.size();
    for(size_t i = 0; i < sz; ++i) {
        const string &moduleName = funcConfig._modules[i].moduleName;
        Module *module = _plugInManagerPtr->getModule(moduleName);
        if (module == NULL) {
            AUTIL_LOG(ERROR, "Get module [%s] failed", moduleName.c_str());
            return false;
        }
        FunctionInterfaceFactory *moduleFactory
            = dynamic_cast<FunctionInterfaceFactory*>(module->getModuleFactory());
        if (!moduleFactory) {
            AUTIL_LOG(ERROR, "invalid FunctionInterfaceFactory in module[%s].", moduleName.c_str());
            return false;
        }
        if (!addFunctions(moduleFactory, funcKVMap, resourceReaderPtr)) {
            return false;
        }
    }

    return true;
}

bool FunctionInterfaceManager::addBuildInFunctions(
        const map<string, KeyValueMap> &allFuncKVMap,
        const ResourceReaderPtr &resourceReader)
{
    unique_ptr<BuildInFunctionFactory> buildInFactory(new BuildInFunctionFactory());
    KeyValueMap parameters;
    if (!buildInFactory->init(parameters, resourceReader)) {
        AUTIL_LOG(ERROR, "init build in function factory failed.");
        return false;
    }

    return addNonPluginFunctions(buildInFactory.release(), allFuncKVMap, resourceReader);
}

bool FunctionInterfaceManager::addNonPluginFunctions(
        FunctionInterfaceFactory *factory,
        const map<string, KeyValueMap> &allFuncKVMap,
        const ResourceReaderPtr &resourceReader, bool keepFactory)
{
    if (!factory) {
        return true;
    }

    if (keepFactory)
    {
        _nonPluginFactorys.push_back(factory);
    }

    if (!addFunctions(factory, allFuncKVMap, resourceReader)) {
        return false;
    }
    return true;
}

bool FunctionInterfaceManager::addFunctions(
        FunctionInterfaceFactory *factory,
        const map<string, KeyValueMap> &allFuncKVMap,
        const ResourceReaderPtr &resourceReaderPtr) 
{
    if (!factory->HasRegisted() && !factory->registerFunctions()) {
        AUTIL_LOG(ERROR, "register function failed.");
        return false;
    }
    const FuncProtoInfoMap &funcMap = factory->getFuncProtoInfos();
    map<string, KeyValueMap> funcKVMap;

    for (FuncProtoInfoMap::const_iterator it = funcMap.begin();
         it != funcMap.end(); ++it)
    {
        const string &funcName = it->first;
        FactoryInfos::iterator fIt = _factorys.find(funcName);
        if (fIt != _factorys.end()) {
            AUTIL_LOG(ERROR, "Failed to init function plugins: conflict function name[%s].",
                    funcName.c_str());
            return false;
        }
        _factorys[funcName] = factory;
        map<string, KeyValueMap>::const_iterator pIt = allFuncKVMap.find(funcName);
        if (pIt != allFuncKVMap.end()) {
            funcKVMap[funcName] = pIt->second;
        }
    }

    return factory->initFunctionCreators(funcKVMap, resourceReaderPtr);
}

bool FunctionInterfaceManager::getFunctionProtoInfo(const string &funcName,
        FunctionProtoInfo &funcProto) const 
{
    FactoryInfos::const_iterator i = _factorys.find(funcName);
    if(i == _factorys.end()) {
        AUTIL_LOG(WARN, "getInterface for %s failed", funcName.c_str());
        return false;
    }
    return i->second->getFunctionProtoInfo(funcName, funcProto);
}

FunctionInterface* FunctionInterfaceManager::createFunctionInterface(
        const string &funcName,
        const AttributeExpressionVec &funcSubExprVec,
        AttributeExpressionCreator *attrExprCreator) const
{
    FactoryInfos::const_iterator i = _factorys.find(funcName);
    if(i == _factorys.end()) {
        AUTIL_LOG(WARN, "getInterface for %s failed", funcName.c_str());
        return NULL;
    }
    FunctionInterface *function = i->second->createFunctionInterface(
            funcName, funcSubExprVec, attrExprCreator);
    if (function == NULL) {
        AUTIL_LOG(WARN, "create function [%s] failed", funcName.c_str());
    }
    return function;
}

bool FunctionInterfaceManager::funcParamToMap(
        const vector<FunctionInfo> &funcInfos, map<string, KeyValueMap> &funcKVMap)
{
    for (vector<FunctionInfo>::const_iterator it = funcInfos.begin();
         it != funcInfos.end(); ++it)
    {
        const string &funcName = it->getFuncName();
        const KeyValueMap &params = it->getParams();
        map<string, KeyValueMap>::const_iterator fIt = funcKVMap.find(funcName);
        if (fIt != funcKVMap.end()) {
            AUTIL_LOG(ERROR, "conflict parameters for function name[%s] found in modules.",
                    funcName.c_str());
            return false;
        }
        funcKVMap[funcName] = params;
    }
    return true;
}

}

