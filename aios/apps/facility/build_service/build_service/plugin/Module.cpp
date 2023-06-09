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
#include "build_service/plugin/Module.h"

#include <cassert>
#include <dlfcn.h>

#include "autil/StringUtil.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/plugin/ModuleFactory.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
namespace build_service { namespace plugin {

BS_LOG_SETUP(plugin, Module);

const std::string Module::CREATE_FACTORY_FUNCTION_NAME = "createFactory";
const std::string Module::DESTROY_FACTORY_FUNCTION_NAME = "destroyFactory";

Module::Module(const string& path, const std::string& root, const std::string& moduleFuncSuffix)
    : _moduleRootDir(root)
    , _moduleFileName(path)
    , _moduleFuncSuffix(moduleFuncSuffix)
    , _dllWrapper(fslib::util::FileUtil::joinFilePath(root, path))
    , _moduleFactory(NULL)
    , _createFactoryFun(NULL)
    , _destroyFactoryFun(NULL)
{
}

Module::~Module() { destroy(); }

bool Module::init(const KeyValueMap& parameters, const ResourceReaderPtr& resourceReaderPtr)
{
    bool ret = doInit(parameters, resourceReaderPtr);
    if (!ret) {
        string errorMsg = _errorMsg;
        BS_LOG(WARN, "%s", errorMsg.c_str());
        destroy();
    }
    return ret;
}

void Module::destroy()
{
    if (_moduleFactory && _destroyFactoryFun) {
        _destroyFactoryFun(_moduleFactory);
    }
    _moduleFactory = NULL;
    _destroyFactoryFun = NULL;
    _createFactoryFun = NULL;
}

ModuleFactory* Module::getModuleFactory()
{
    unique_lock<mutex> lock(_mu);
    if (!_moduleFactory && _createFactoryFun) {
        _moduleFactory = _createFactoryFun();
    }
    return _moduleFactory;
}

//"dl_open_mode" : "RTLD_LAZY|RTLD_NOW|RTLD_GLOBAL|RTLD_LOCAL|RTLD_NODELETE|RTLD_NOLOAD|RTLD_DEEPBIND"
bool Module::getDlOpenMode(const KeyValueMap& parameters, int& mode)
{
    string openModeStr = getValueFromKeyValueMap(parameters, "dl_open_mode");
    if (openModeStr.empty()) {
        mode = RTLD_NOW;
        return true;
    }
    return parseDlOpenMode(openModeStr, mode, _errorMsg);
}

bool Module::parseDlOpenMode(const std::string& openModeStr, int& mode, std::string& error)
{
    if (openModeStr.empty()) {
        error += "dl_open_mode is empty";
        return false;
    }
    mode = 0;
    vector<string> modeStrs;
    StringUtil::fromString(openModeStr, modeStrs, "|");
    for (size_t i = 0; i < modeStrs.size(); i++) {
        if (!appendOpenMode(modeStrs[i], mode)) {
            error += "dl_open_mode not right: [" + openModeStr + "]";
            return false;
        }
    }
    return true;
}

bool Module::appendOpenMode(const string& modeStr, int& mode)
{
#define CHANGE_MODE(modeStr, modeTag, mode, result)                                                                    \
    if (modeStr == modeTag) {                                                                                          \
        result |= mode;                                                                                                \
        return true;                                                                                                   \
    }

    CHANGE_MODE(modeStr, "RTLD_LAZY", RTLD_LAZY, mode);
    CHANGE_MODE(modeStr, "RTLD_NOW", RTLD_NOW, mode);
    CHANGE_MODE(modeStr, "RTLD_GLOBAL", RTLD_GLOBAL, mode);
    CHANGE_MODE(modeStr, "RTLD_LOCAL", RTLD_LOCAL, mode);
    CHANGE_MODE(modeStr, "RTLD_NODELETE", RTLD_NODELETE, mode);
    CHANGE_MODE(modeStr, "RTLD_NOLOAD", RTLD_NOLOAD, mode);
    CHANGE_MODE(modeStr, "RTLD_DEEPBIND", RTLD_DEEPBIND, mode);
    return false;

#undef CHANGE_MODE
}

bool Module::doInit(const KeyValueMap& parameters, const ResourceReaderPtr& resourceReaderPtr)
{
    int dlopenMode = 0;
    if (!getDlOpenMode(parameters, dlopenMode)) {
        return false;
    }

    if (!_dllWrapper.dlopen(dlopenMode)) {
        _errorMsg += "open dll fail:[" + _moduleFileName + "]";
        BS_LOG(ERROR, "dlopen %s failed, try dopen self for static link factory, dlerror [%s]", _moduleFileName.c_str(),
               _dllWrapper.dlerror().c_str());
        return false;
    }
    bool ret = initModuleFunctions(_moduleFuncSuffix);
    if (!ret) {
        // init without suffix
        ret = initModuleFunctions();
    }
    if (!ret) {
        return ret;
    }
    ModuleFactory* factory = getModuleFactory();
    if (!factory) {
        _errorMsg += string("Get ModuleFactory module [") + _dllWrapper.getLocalLibPath() + "] fail";
        return false;
    }
    if (!factory->init(parameters, resourceReaderPtr)) {
        _errorMsg += string("ModuleFactory init failed in module [") + _dllWrapper.getLocalLibPath() + "]";
        return false;
    }
    _errorMsg = "";
    return true;
}

bool Module::initModuleFunctions(const string& moduleFuncSuffix)
{
    _createFactoryFun = (CreateFactoryFun)_dllWrapper.dlsym(CREATE_FACTORY_FUNCTION_NAME + moduleFuncSuffix);
    if (!_createFactoryFun) {
        _errorMsg += "fail to get symbol[createFactory] from module[" + _moduleFileName + "].";
        return false;
    }
    BS_LOG(TRACE3, "get symbol createFactory ");
    _destroyFactoryFun = (DestroyFactoryFun)_dllWrapper.dlsym(DESTROY_FACTORY_FUNCTION_NAME + moduleFuncSuffix);
    if (!_destroyFactoryFun) {
        _errorMsg += "fail to get symbol[destroyFactory] from module[" + _moduleFileName + "].";
        return false;
    }
    return true;
}

}} // namespace build_service::plugin
