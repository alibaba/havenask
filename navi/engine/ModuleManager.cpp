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
    _moduleMap.emplace(absPath, module);
    NAVI_KERNEL_LOG(INFO, "module [%s] loaded", absPath.c_str());
    return module;
}

}

