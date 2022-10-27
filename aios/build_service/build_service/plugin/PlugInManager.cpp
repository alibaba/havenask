#include "build_service/plugin/PlugInManager.h"
#include "build_service/plugin/Module.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/FileUtil.h"
#include <cassert>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::util;

namespace build_service {
namespace plugin {
BS_LOG_SETUP(plugin, PlugInManager);

PlugInManager::PlugInManager(const ResourceReaderPtr &resourceReaderPtr,
                             const std::string& pluginPath,
                             const string &moduleFuncSuffix)
    : _moduleFuncSuffix(moduleFuncSuffix)
    , _resourceReaderPtr(resourceReaderPtr)
    , _pluginPath(pluginPath)
{
}

PlugInManager::PlugInManager(const ResourceReaderPtr &resourceReaderPtr,
                             const string &moduleFuncSuffix)
    : _moduleFuncSuffix(moduleFuncSuffix)
    , _resourceReaderPtr(resourceReaderPtr)
    , _pluginPath(resourceReaderPtr->getPluginPath())
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
    unique_lock<mutex> lock(_mu);
    string moduleName = moduleInfo.moduleName;
    pair<map<string, ModuleInfo>::iterator ,bool> ret = _name2moduleInfoMap.insert(
            make_pair(moduleName, moduleInfo));
    if (!ret.second) {
        stringstream ss;
        ss << "conflict module name[" << moduleName
           <<"] for module["<< _name2moduleInfoMap[moduleName].modulePath
           <<"] and module[" << moduleInfo.modulePath << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
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

// get default module
Module* PlugInManager::getModule() {
    unique_lock<mutex> lock(_mu);
    std::string moduleName;
    map<string, Module*>::iterator it = _name2moduleMap.find(moduleName);
    if (it != _name2moduleMap.end()) {
        return it->second;
    }
    Module* module = new Module("", "", _moduleFuncSuffix);
    KeyValueMap parameters;
    if (!module->init(parameters, _resourceReaderPtr)) {
        string errorMsg = "Init module[" + moduleName
                          +"] failed, error msg: " + module->getErrorMsg();
        BS_LOG(WARN, "%s", errorMsg.c_str());
        delete module;
        return NULL;
    }
    _name2moduleMap[moduleName] = module;
    return module;
}

std::string PlugInManager::getPluginPath(const std::string& moduleFileName) {
    char* env = getenv("LD_LIBRARY_PATH");
    string paths;
    if (NULL == env) {
        paths = _pluginPath + ":" + _resourceReaderPtr->getPluginPath() + ":./";
    } else {
        paths = _pluginPath + ":" + _resourceReaderPtr->getPluginPath() + ":./:" + string(env);
    }

    AUTIL_LOG(INFO, "scan path: [%s]", paths.c_str());
    StringUtil::trim(paths);
    StringTokenizer tokens(paths, ":", StringTokenizer::TOKEN_TRIM);
    StringTokenizer::Iterator it = tokens.begin();
    while (it != tokens.end()) {
      if (parseSoUnderOnePath((*it), moduleFileName)) {
	    AUTIL_LOG(INFO, "find [%s] under [%s]",
		      moduleFileName.c_str(), (*it).c_str());
	    return *it;
        }
        it++;
    }
    AUTIL_LOG(ERROR, "can not find module file: [%s], return config plugin path",
              moduleFileName.c_str());
    return _resourceReaderPtr->getPluginPath();
}

bool PlugInManager::parseSoUnderOnePath(
    const std::string& path, const std::string& moduleFileName)
{
    vector<string> fileList;
    if (!FileUtil::listDir(path, fileList)) {
        return false;
    }
    for (auto& filePath : fileList) {
        if (filePath == moduleFileName) {
            return true;
        }
    }
    return false;
}


Module* PlugInManager::getModule(const string &moduleName) {
    unique_lock<mutex> lock(_mu);
    map<string, Module*>::iterator it = _name2moduleMap.find(moduleName);
    if (it != _name2moduleMap.end()) {
        return it->second;
    }
    ModuleInfo moduleInfo = getModuleInfo(moduleName);
    if (moduleInfo.moduleName.empty()) {
        string errorMsg = "module[" + moduleName + "] not exist.";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return NULL;
    }
    string pluginPath = getPluginPath(moduleInfo.modulePath);
    Module* module = new Module(moduleInfo.modulePath, pluginPath,
                                _moduleFuncSuffix);
    if (!module->init(moduleInfo.parameters, _resourceReaderPtr)) {
        string errorMsg = "Init module[" + moduleName
                          +"] failed, error msg: " + module->getErrorMsg();
        BS_LOG(WARN, "%s", errorMsg.c_str());
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
}
