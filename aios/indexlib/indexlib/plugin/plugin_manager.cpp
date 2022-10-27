#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/plugin/Module.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/common/common_type.h"
#include "indexlib/util/path_util.h"
#include <cassert>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(plugin);
IE_LOG_SETUP(plugin, PluginManager);

PluginManager::PluginManager(
    const std::string &moduleRootPath) 
    : _moduleRootPath(moduleRootPath)
{
}

PluginManager::~PluginManager() {
    clear();
}

void PluginManager::clear() {
    ScopedLock l(mMapLock);
    
    for (map<string, Module*>::iterator it = _name2moduleMap.begin();
         it != _name2moduleMap.end(); ++it)
    {
        delete it->second;
    }
    _name2moduleMap.clear();
    _name2moduleInfoMap.clear();
}

bool PluginManager::addModule(const ModuleInfo &moduleInfo) {
    string moduleName = moduleInfo.moduleName;
    pair<map<string, ModuleInfo>::iterator ,bool> ret = _name2moduleInfoMap.insert(
            make_pair(moduleName, moduleInfo));
    if (!ret.second) {
        stringstream ss;
        ss << "conflict module name[" << moduleName
           <<"] for module["<< _name2moduleInfoMap[moduleName].modulePath
           <<"] and module[" << moduleInfo.modulePath << "]";
        string errorMsg = ss.str();
        IE_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool PluginManager::addModules(const ModuleInfos &moduleInfos) {
    ScopedLock l(mMapLock);
    
    for (size_t i = 0; i < moduleInfos.size(); ++i) {
        if (!addModule(moduleInfos[i])) {
            return false;
        }
    }
    return true;
}

Module* PluginManager::getModule(const string &moduleName) {
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
    string fullPath = util::PathUtil::JoinPath(_moduleRootPath, moduleInfo.modulePath);
    if (fslib::fs::FileSystem::isFile(fullPath) != fslib::EC_TRUE) {
        char* pluginPathEnv = getenv("INDEXLIB_PLUGIN_PATH");
        _moduleRootPath = pluginPathEnv ? pluginPathEnv : _moduleRootPath;
        IE_LOG(WARN, "[%s] not exists, get from [%s]", fullPath.c_str(), _moduleRootPath.c_str());
    }
    Module* module = new Module(moduleInfo.modulePath, _moduleRootPath);
    if (!module->init()) {
        string errorMsg = "Init module[" + moduleName
                          +"] failed";
        IE_LOG(WARN, "%s", errorMsg.c_str());
        delete module;
        return NULL;
    }
    _name2moduleMap[moduleName] = module;
    return module;
}

ModuleInfo PluginManager::getModuleInfo(const string &moduleName) {
    map<string, ModuleInfo>::iterator it = _name2moduleInfoMap.find(moduleName);
    if (it == _name2moduleInfoMap.end()) {
        return ModuleInfo();
    }
    return it->second;
}

IE_NAMESPACE_END(plugin);

