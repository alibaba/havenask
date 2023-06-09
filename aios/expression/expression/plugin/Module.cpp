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
#include "expression/plugin/Module.h"
#include "expression/plugin/ModuleFactory.h"
#include <dlfcn.h>
#include <cassert>
#include "fslib/fslib.h"
#include "resource_reader/ResourceReader.h"

using namespace std;
namespace expression {

AUTIL_LOG_SETUP(plugin, Module);

const std::string Module::CREATE_FACTORY_FUNCTION_NAME = "createFactory";
const std::string Module::DESTROY_FACTORY_FUNCTION_NAME = "destroyFactory";

Module::Module(const string &path, const std::string &root,
               const std::string &moduleFuncSuffix)
    : _moduleRootDir(root)
    , _moduleFileName(path)
    , _moduleFuncSuffix(moduleFuncSuffix)
    , _dllWrapper(fslib::fs::FileSystem::joinFilePath(root, path))
    , _moduleFactory(NULL)
    , _createFactoryFun(NULL)
    , _destroyFactoryFun(NULL)
{
}

Module::~Module() {
    destroy();
}

bool Module::init(const KeyValueMap &parameters, const resource_reader::ResourceReaderPtr &resourceReader)
{
    bool ret = doInit(parameters, resourceReader);
    if (!ret) {
        AUTIL_LOG(ERROR, "%s", _errorMsg.c_str());
        destroy();
    }
    return ret;
}

void Module::destroy() {
    if (_moduleFactory && _destroyFactoryFun) {
        _destroyFactoryFun(_moduleFactory);
    }
    _moduleFactory = NULL;
    _destroyFactoryFun = NULL;
    _createFactoryFun = NULL;
}

ModuleFactory* Module::getModuleFactory() {
    if (!_moduleFactory && _createFactoryFun) {
        _moduleFactory = _createFactoryFun();
    }
    return _moduleFactory;
}

bool Module::doInit(const KeyValueMap &parameters, const resource_reader::ResourceReaderPtr &resourceReader)
{
    if (!_dllWrapper.dlopen()) {
        _errorMsg += "open dll fail:[" + _moduleFileName + "]";
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
    ModuleFactory *factory = getModuleFactory();
    if (!factory) {
        _errorMsg += string("Get ModuleFactory module [")
                     + _dllWrapper.getLocalLibPath()
                     + "] fail";
        return false;
    }
    if (!factory->init(parameters, resourceReader)) {
        _errorMsg += string("ModuleFactory init failed in module [")
                     + _dllWrapper.getLocalLibPath()
                     + "]";
        return false;
    }
    _errorMsg = "";
    return true;
}

bool Module::initModuleFunctions(const string &moduleFuncSuffix) {
    _createFactoryFun = (CreateFactoryFun)_dllWrapper.dlsym(
            CREATE_FACTORY_FUNCTION_NAME + moduleFuncSuffix);
    if (!_createFactoryFun) {
        _errorMsg += "fail to get symbol[createFactory] from module["
                     + _moduleFileName + "].";
        return false;
    }
    AUTIL_LOG(TRACE3, "get symbol createFactory ");
    _destroyFactoryFun = (DestroyFactoryFun)_dllWrapper.dlsym(
            DESTROY_FACTORY_FUNCTION_NAME + moduleFuncSuffix);
    if (!_destroyFactoryFun) {
        _errorMsg += "fail to get symbol[destroyFactory] from module["
                     + _moduleFileName + "].";
        return false;
    }
    return true;
}

}

