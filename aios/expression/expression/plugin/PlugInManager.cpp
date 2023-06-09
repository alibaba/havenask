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
#include "expression/plugin/PlugInManager.h"
#include "expression/plugin/Module.h"
#include "resource_reader/ResourceReader.h"

using namespace std;

namespace expression {

AUTIL_LOG_SETUP(plugin, PlugInManager);

PlugInManager::PlugInManager(
        const resource_reader::ResourceReaderPtr &resourceReaderPtr,
        const string &moduleFuncSuffix)
    : _moduleFuncSuffix(moduleFuncSuffix)
    , _resourceReaderPtr(resourceReaderPtr)
{
}

PlugInManager::~PlugInManager() {
    clear();
}

void PlugInManager::clear() {
    for (map<string, Module*>::iterator it = _name2moduleMap.begin();
         it != _name2moduleMap.end(); ++it)
    {
        delete it->second;
    }
    _name2moduleMap.clear();
    _name2moduleInfoMap.clear();
}

bool PlugInManager::addModule(const ModuleInfo &moduleInfo) {
    string moduleName = moduleInfo.moduleName;
    pair<map<string, ModuleInfo>::iterator ,bool> ret = _name2moduleInfoMap.insert(
            make_pair(moduleName, moduleInfo));
    if (!ret.second) {
        AUTIL_LOG(WARN, "conflict module name[%s]", moduleName.c_str());
        return false;
    }
    return true;
}

bool PlugInManager::addModules(const ModuleInfos &moduleInfos) {
    for (size_t i = 0; i < moduleInfos.size(); ++i) {
        if (!addModule(moduleInfos[i])) {
            return false;
        }
    }
    return true;
}

Module* PlugInManager::getModule(const string &moduleName) {
    map<string, Module*>::iterator it = _name2moduleMap.find(moduleName);
    if (it != _name2moduleMap.end()) {
        return it->second;
    }
    ModuleInfo moduleInfo = getModuleInfo(moduleName);
    if (moduleInfo.moduleName.empty()) {
        AUTIL_LOG(WARN, "module [%s] does not exist", moduleName.c_str());
        return NULL;
    }

    string pluginRoot;
    KeyValueMap::const_iterator mIt = moduleInfo.parameters.find("__inpackage__");
    if (mIt == moduleInfo.parameters.end() || mIt->second != "true") {
        pluginRoot = _resourceReaderPtr->getPluginPath();
    }
    Module* module = new Module(moduleInfo.modulePath, pluginRoot,
                                _moduleFuncSuffix);
    if (!module->init(moduleInfo.parameters, _resourceReaderPtr)) {
        AUTIL_LOG(WARN, "init module [%s] failed, error message: %s",
                  moduleName.c_str(), module->getErrorMsg().c_str());
        delete module;
        return NULL;
    }
    _name2moduleMap[moduleName] = module;
    return module;
}

ModuleInfo PlugInManager::getModuleInfo(const string &moduleName) {
    map<string, ModuleInfo>::iterator it = _name2moduleInfoMap.find(moduleName);
    if (it == _name2moduleInfoMap.end()) {
        return ModuleInfo();
    }
    return it->second;
}

}
