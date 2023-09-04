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
#include "navi/engine/ModuleManager.h"
#include "navi/log/NaviLogger.h"
#include "navi/util/CommonUtil.h"
#include <dlfcn.h>

namespace navi {

const std::string Module::BUILDIN_MODULE_ID = "buildin";

Module::Module(const std::string &path)
    : _path(path)
    , _handle(nullptr)
{
}

Module::~Module() {
    _resourceRegistry.clear();
    _kernelRegistry.clear();
    _typeRegistry.clear();
    if (_handle) {
        NAVI_KERNEL_LOG(INFO, "module [%s] unloaded", _path.c_str());
        ::dlclose(_handle);
    }
}

void Module::setHandle(void *handle) {
    _handle = handle;
}

bool Module::init() {
    if (!_resourceRegistry.initModule(this)) {
        NAVI_KERNEL_LOG(ERROR, "init resource registry module failed");
        return false;
    }
    if (!_kernelRegistry.initModule(this)) {
        NAVI_KERNEL_LOG(ERROR, "init kernel registry module failed");
        return false;
    }
    if (!_typeRegistry.initModule(this)) {
        NAVI_KERNEL_LOG(ERROR, "init type registry module failed");
        return false;
    }
    _resourceRegistry.setInited();
    _kernelRegistry.setInited();
    _typeRegistry.setInited();
    return true;
}

CreatorRegistry &Module::getResourceRegistry() {
    return _resourceRegistry;
}

CreatorRegistry &Module::getKernelRegistry() {
    return _kernelRegistry;
}

CreatorRegistry &Module::getTypeRegistry() {
    return _typeRegistry;
}

const std::string &Module::getPath() const {
    return _path;
}

const std::string &Module::getModulePath(Module *module) {
    if (module) {
        return module->getPath();
    } else {
        return BUILDIN_MODULE_ID;
    }
}

ModuleManager::ModuleManager()
{
}

ModuleManager::~ModuleManager() {
}

ModulePtr ModuleManager::loadModule(const std::string &configPath,
                                    const std::string &modulePath)
{
    auto absPath = CommonUtil::getConfigAbsPath(configPath, modulePath);
    auto it = _moduleMap.find(absPath);
    if (it != _moduleMap.end()) {
        return it->second;
    }
    if (absPath.empty()) {
        NAVI_KERNEL_LOG(ERROR, "empty module path, check module config");
        return nullptr;
    }
    ModulePtr module(new Module(absPath));
    CreatorRegistry::setCurrent(RT_RESOURCE, &module->getResourceRegistry());
    CreatorRegistry::setCurrent(RT_KERNEL, &module->getKernelRegistry());
    CreatorRegistry::setCurrent(RT_TYPE, &module->getTypeRegistry());
    auto handle = ::dlopen(absPath.c_str(), RTLD_GLOBAL|RTLD_NOW);
    if (!handle) {
        NAVI_KERNEL_LOG(ERROR, "load module [%s] failed, error [%s]",
                        absPath.c_str(), ::dlerror());
        return nullptr;
    }
    module->setHandle(handle);
    if (!module->init()) {
        NAVI_KERNEL_LOG(ERROR, "init module [%s] failed", absPath.c_str());
        return nullptr;
    }
    _moduleMap.emplace(absPath, module);
    NAVI_KERNEL_LOG(INFO, "module [%s] loaded", absPath.c_str());
    return module;
}

void ModuleManager::cleanup() {
    auto moduleMap = _moduleMap;
    for (const auto &pair : moduleMap) {
        if (pair.second.use_count() == 1) {
            _moduleMap.erase(pair.first);
        }
    }
}

}

