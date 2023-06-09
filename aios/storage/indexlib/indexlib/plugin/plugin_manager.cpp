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
#include "indexlib/plugin/plugin_manager.h"

#include <cassert>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::util;
namespace indexlib { namespace plugin {
IE_LOG_SETUP(plugin, PluginManager);

PluginManager::PluginManager(const std::string& moduleRootPath) : _moduleRootPath(moduleRootPath) {}

PluginManager::~PluginManager() { clear(); }

void PluginManager::clear()
{
    ScopedLock l(mMapLock);

    for (map<string, Module*>::iterator it = _name2moduleMap.begin(); it != _name2moduleMap.end(); ++it) {
        delete it->second;
    }
    _name2moduleMap.clear();
    _name2moduleInfoMap.clear();
}

bool PluginManager::addModule(const ModuleInfo& moduleInfo)
{
    string moduleName = moduleInfo.moduleName;
    pair<map<string, ModuleInfo>::iterator, bool> ret = _name2moduleInfoMap.insert(make_pair(moduleName, moduleInfo));
    if (!ret.second) {
        stringstream ss;
        ss << "conflict module name[" << moduleName << "] for module[" << _name2moduleInfoMap[moduleName].modulePath
           << "] and module[" << moduleInfo.modulePath << "]";
        string errorMsg = ss.str();
        IE_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool PluginManager::addModules(const ModuleInfos& moduleInfos)
{
    ScopedLock l(mMapLock);

    for (size_t i = 0; i < moduleInfos.size(); ++i) {
        if (!addModule(moduleInfos[i])) {
            return false;
        }
    }
    return true;
}

Module* PluginManager::getModule(const string& moduleName)
{
    ScopedLock l(mMapLock);

    map<string, Module*>::iterator it = _name2moduleMap.find(moduleName);
    if (it != _name2moduleMap.end()) {
        return it->second;
    }
    ModuleInfo moduleInfo = getModuleInfo(moduleName);
    if (moduleInfo.moduleName.empty()) {
        string errorMsg = "module[" + moduleName + "] not exist.";
        IE_LOG(WARN, "%s", errorMsg.c_str());
        return NULL;
    }

    vector<string> pathCandidates;
    string pluginSearchPath = autil::EnvUtil::getEnv(string("INDEXLIB_PLUGIN_SEARCH_PATH"), string(""));
    if (!pluginSearchPath.empty()) {
        pathCandidates = StringUtil::split(pluginSearchPath, ":");
    }
    if (!_moduleRootPath.empty()) {
        pathCandidates.push_back(_moduleRootPath);
    }
    // compatible with rtp array table
    string indexlibPluginPath = autil::EnvUtil::getEnv(string("INDEXLIB_PLUGIN_PATH"), string(""));
    if (!indexlibPluginPath.empty()) {
        pathCandidates.push_back(indexlibPluginPath);
    }

    for (const auto& ldPath : pathCandidates) {
        std::pair<bool, Module*> ret = {false, NULL};
        if (ldPath == "__PLUGIN_DIR__") {
            ret = loadModuleFromDir(_moduleRootPath, moduleInfo);
        } else {
            ret = loadModuleFromDir(ldPath, moduleInfo);
        }
        if (ret.first == false) {
            continue;
        }
        if (ret.second == NULL) {
            return NULL;
        }
        _name2moduleMap[moduleName] = ret.second;
        return ret.second;
    }
    // compatible with test, try load lib from LD_LIBRARY_PATH, see dlopen manual for further details of lib path
    // priorities
    if (_moduleRootPath.empty()) {
        Module* module = new Module(moduleInfo.modulePath, "");
        if (!module->init()) {
            IE_LOG(WARN, "Init module [%s] failed", moduleName.c_str());
            delete module;
            return NULL;
        }
        _name2moduleMap[moduleName] = module;
        return module;
    }
    return NULL;
}

pair<bool, Module*> PluginManager::loadModuleFromDir(const string& dir, ModuleInfo& moduleInfo)
{
    bool fileExist = false;
    Module* module = NULL;
    string fullLibPath = util::PathUtil::JoinPath(dir, moduleInfo.modulePath);
    if (fslib::fs::FileSystem::isFile(fullLibPath) != fslib::EC_TRUE) {
        fileExist = false;
        IE_LOG(WARN, "load module[%s] from Dir[%s] failed, file not exists", moduleInfo.modulePath.c_str(),
               dir.c_str());
        return make_pair(fileExist, module);
    }
    fileExist = true;

    module = new Module(moduleInfo.modulePath, dir);
    if (!module->init()) {
        IE_LOG(WARN, "module[%s] init failed from [%s]", moduleInfo.modulePath.c_str(), dir.c_str());
        delete module;
        module = NULL;
        return make_pair(fileExist, module);
    }
    IE_LOG(INFO, "module[%s] init success from [%s]", moduleInfo.modulePath.c_str(), dir.c_str());
    return make_pair(fileExist, module);
}

ModuleInfo PluginManager::getModuleInfo(const string& moduleName)
{
    map<string, ModuleInfo>::iterator it = _name2moduleInfoMap.find(moduleName);
    if (it == _name2moduleInfoMap.end()) {
        return ModuleInfo();
    }
    return it->second;
}

}} // namespace indexlib::plugin
