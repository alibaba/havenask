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
#include "navi/engine/CreatorManager.h"

namespace navi {

CreatorManager::CreatorManager() {
}

CreatorManager::~CreatorManager() {
}

bool CreatorManager::init(const NaviConfig &config) {
    std::map<std::string, ModulePtr> modules;
    if (!initModules(config, modules)) {
        return false;
    }
    return initRegistrys(modules);
}

const KernelCreator *CreatorManager::getKernelCreator(
        const std::string &kernelName) const
{
    Creator *creator = _kernelRegistry.getCreator(kernelName);
    auto kernelCreator = dynamic_cast<KernelCreator *>(creator);
    if (!kernelCreator) {
        NAVI_KERNEL_LOG(ERROR, "kernel [%s] not registered",
                        kernelName.c_str());
        return nullptr;
    }
    return kernelCreator;
}

const ResourceCreator *CreatorManager::getResourceCreator(
        const std::string &resourceName) const
{
    auto creator = dynamic_cast<ResourceCreator *>(
        _resourceRegistry.getCreator(resourceName));
    if (!creator) {
        return nullptr;
    }
    return creator;
}

const Type *CreatorManager::getType(const std::string &typeId) const {
    auto dataType =
        dynamic_cast<const Type *>(_typeRegistry.getCreator(typeId));
    if (!dataType) {
        return nullptr;
    }
    return dataType;
}

bool CreatorManager::initModules(const NaviConfig &config,
                                 std::map<std::string, ModulePtr> &modules)
{
    const auto &configPath = config.configPath;
    for (const auto &moduleInfo : config.modules) {
        auto module = _moduleManager.loadModule(configPath, moduleInfo);
        if (!module) {
            return false;
        }
        modules.insert(std::make_pair(module->getPath(), module));
    }
    return true;
}

bool CreatorManager::initRegistrys(
    const std::map<std::string, ModulePtr> &modules)
{
    for (const auto &pair : modules) {
        _resourceRegistry.merge(pair.second->getResourceRegistry());
        _kernelRegistry.merge(pair.second->getKernelRegistry());
        _typeRegistry.merge(pair.second->getTypeRegistry());
    }
    _resourceRegistry.merge(*CreatorRegistry::buildin(RT_RESOURCE));
    _kernelRegistry.merge(*CreatorRegistry::buildin(RT_KERNEL));
    _typeRegistry.merge(*CreatorRegistry::buildin(RT_TYPE));
    if (!moduleInit()) {
        return false;
    }
    if (!initResourceRegistry(_resourceRegistry, _resourceLoadTypeMap) ||
        !initKernelRegistry(_kernelRegistry) ||
        !initTypeRegistry(_typeRegistry))
    {
        return false;
    }
    return true;
}

bool CreatorManager::moduleInit() {
    CreatorRegistry::setCurrent(RT_RESOURCE, &_resourceRegistry);
    CreatorRegistry::setCurrent(RT_KERNEL, &_kernelRegistry);
    CreatorRegistry::setCurrent(RT_TYPE, &_typeRegistry);
    return _resourceRegistry.initModule()
        && _kernelRegistry.initModule()
        && _typeRegistry.initModule();
}

bool CreatorManager::initResourceRegistry(CreatorRegistry &resourceRegistry,
                                          ResourceLoadTypeMap &resourceLoadTypeMap)
{
    if (!resourceRegistry.initCreators()) {
        NAVI_KERNEL_LOG(ERROR, "init resource creator failed");
        return false;
    }
    const auto &creatorMap = resourceRegistry.getCreatorMap();
    for (const auto &pair : creatorMap) {
        auto creator = dynamic_cast<ResourceCreator *>(pair.second.get());
        if (!creator) {
            NAVI_KERNEL_LOG(ERROR, "invalid resource registry [%s]",
                            pair.first.c_str());
            return false;
        }
        auto loadType = creator->getLoadType();
        NAVI_KERNEL_LOG(INFO, "register resource[%s], loadType: %s",
                        pair.first.c_str(),
                        ResourceLoadType_Name(loadType).c_str());
        resourceLoadTypeMap[loadType].push_back(pair.first);
    }
    return true;
}

std::vector<std::string> CreatorManager::getResourceByLoadType(
        ResourceLoadType type) const
{
    auto it = _resourceLoadTypeMap.find(type);
    if (_resourceLoadTypeMap.end() != it) {
        return it->second;
    } else {
        return {};
    }
}

bool CreatorManager::initKernelRegistry(CreatorRegistry &kernelRegistry) {
    if (!kernelRegistry.initCreators()) {
        NAVI_KERNEL_LOG(ERROR, "init kernel creator failed");
        return false;
    }
    const auto &creatorMap = kernelRegistry.getCreatorMap();
    for (const auto &pair : creatorMap) {
        auto creator = dynamic_cast<KernelCreator *>(pair.second.get());
        if (!creator) {
            NAVI_KERNEL_LOG(ERROR, "invalid kernel registry [%s]",
                            pair.first.c_str());
            return false;
        }
    }
    return true;
}

bool CreatorManager::initTypeRegistry(CreatorRegistry &typeRegistry) {
    if (!typeRegistry.initCreators()) {
        NAVI_KERNEL_LOG(ERROR, "init type creator failed");
        return false;
    }
    const auto &creatorMap = typeRegistry.getCreatorMap();
    for (const auto &pair : creatorMap) {
        auto type = dynamic_cast<Type *>(pair.second.get());
        if (!type) {
            NAVI_KERNEL_LOG(ERROR, "invalid type registry [%s]",
                            pair.first.c_str());
            return false;
        }
    }
    return true;
}

}
