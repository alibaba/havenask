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
#include "swift/admin/ModuleManager.h"

#include <functional>

#include "autil/TimeUtility.h"
#include "swift/common/Common.h"
#include "swift/common/CreatorRegistry.h"

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, ModuleManager);

using namespace autil;
using namespace swift::common;

ModuleManager::ModuleManager()
    : _moduleCreatorFunc(CreatorRegistry<RegisterType::RT_MODULE>::getInstance()->getCreatorFuncMap()) {}
ModuleManager::~ModuleManager() { stop(); }

bool ModuleManager::init(AdminStatusManager *adminStatusManager,
                         SysController *sysController,
                         config::AdminConfig *adminConfig) {
    ScopedLock lock(_lock);
    _adminStatusManager = adminStatusManager;
    _sysController = sysController;
    _adminConfig = adminConfig;
    if (!createModule()) {
        AUTIL_LOG(ERROR, "module regist failed");
        return false;
    }
    if (!bindDependModule()) {
        AUTIL_LOG(ERROR, "module bind failed");
        return false;
    }
    return true;
}

bool ModuleManager::start() {
    ScopedLock lock(_lock);
    if (!_stopped) {
        return true;
    }
    if (!loadSysModule()) {
        AUTIL_LOG(ERROR, "load sys module failed");
        return false;
    }
    _stopped = false;
    return true;
}

bool ModuleManager::update(Status status) {
    ScopedLock lock(_lock);
    if (_stopped) {
        return true;
    }
    AUTIL_LOG(INFO, "module update status[%s] ", status.toString().c_str());
    for (auto iter = _modules.begin(); iter != _modules.end(); ++iter) {
        const auto &moduleName = iter->first;
        const auto &module = iter->second;
        if (!module->isSysModule()) {
            continue;
        }
        if (!module->update(status)) {
            AUTIL_LOG(ERROR, "update sys module fail, module[%s]", moduleName.c_str());
            return false;
        }
    }
    for (auto iter = _modules.begin(); iter != _modules.end(); ++iter) {
        const auto &moduleName = iter->first;
        const auto &module = iter->second;
        if (module->isSysModule()) {
            continue;
        }
        if (!unload(status, module)) {
            AUTIL_LOG(ERROR, "unload module fail, module[%s]", moduleName.c_str());
            return false;
        }
        AUTIL_LOG(INFO, "unload module success, module[%s]", moduleName.c_str());
    }
    for (auto iter = _modules.begin(); iter != _modules.end(); ++iter) {
        const auto &moduleName = iter->first;
        const auto &module = iter->second;
        if (module->isSysModule()) {
            continue;
        }
        if (!load(status, module)) {
            AUTIL_LOG(ERROR, "load module fail, module[%s]", moduleName.c_str());
            return false;
        }
        AUTIL_LOG(INFO, "load module success, module[%s]", moduleName.c_str());
    }
    return true;
}

void ModuleManager::stop() {
    ScopedLock lock(_lock);
    for (auto iter = _modules.begin(); iter != _modules.end(); ++iter) {
        const auto &moduleName = iter->first;
        const auto &module = iter->second;
        if (module->isSysModule()) {
            continue;
        }
        if (!unloadForStop(module)) {
            AUTIL_LOG(ERROR, "unload module fail, module[%s]", moduleName.c_str());
        }
        module->clear();
        AUTIL_LOG(INFO, "unload module success, module[%s]", moduleName.c_str());
    }
    for (auto iter = _modules.begin(); iter != _modules.end(); ++iter) {
        const auto &moduleName = iter->first;
        const auto &module = iter->second;
        if (!module->isSysModule()) {
            continue;
        }
        if (!unloadForStop(module)) {
            AUTIL_LOG(ERROR, "unload module fail, module[%s]", moduleName.c_str());
        }
        module->clear();
        AUTIL_LOG(INFO, "unload module success, module[%s]", moduleName.c_str());
    }
    _modules.clear();
    _stopped = true;
}

bool ModuleManager::createModule() {
    for (const auto &p : _moduleCreatorFunc) {
        const auto &name = p.first;
        const auto &func = p.second;
        if (!func) {
            AUTIL_LOG(ERROR, "module regist failed, module type[%s]", name.c_str());
            return false;
        }
        auto module = func();
        if (!module) {
            AUTIL_LOG(ERROR, "module create failed, module type[%s]", name.c_str());
            return false;
        }
        if (!module->init(this, _sysController, _adminConfig)) {
            AUTIL_LOG(ERROR, "module init failed, module name[%s]", name.c_str());
            return false;
        }
        _modules.emplace(name, std::move(module));
        AUTIL_LOG(INFO, "module regist succ, module type[%s]", name.c_str());
    }
    return true;
}

bool ModuleManager::bindDependModule() {
    for (auto &p : _modules) {
        const auto &moduleName = p.first;
        auto &module = p.second;
        module->initDepends();
        auto bindFuncMap = module->getBindFuncMap();
        for (auto funcMapIt : bindFuncMap) {
            auto dependModuleName = funcMapIt.first;
            auto func = funcMapIt.second;
            auto dependModule = getModule(dependModuleName);
            if (!dependModule) {
                AUTIL_LOG(WARN,
                          "module[%s] bind failed, depends module[%s] not regist",
                          moduleName.c_str(),
                          dependModuleName.c_str());
                return false;
            }
            if (func) {
                func(dependModule);
            }
            dependModule->beDepended(moduleName, module);
        }
    }
    return true;
}

bool ModuleManager::loadSysModule() {
    Status status;
    for (auto p : _modules) {
        const auto &moduleName = p.first;
        const auto &module = p.second;
        if (!module->isSysModule()) {
            continue;
        }
        if (!load(status, module)) {
            AUTIL_LOG(ERROR, "load sys module fail, module[%s]", moduleName.c_str());
            return false;
        }
        AUTIL_LOG(INFO, "load sys module success, module[%s]", moduleName.c_str());
    }
    return true;
}

bool ModuleManager::load(Status status, const std::shared_ptr<BaseModule> &module, int32_t depth) {
    if (depth >= MODULE_MAX_DEPTH) {
        AUTIL_LOG(ERROR, "check module depend, recursion depth exceeded limit");
        return false;
    }
    if (!module->needLoad(status)) {
        return true;
    }
    const auto &moduleName = module->getName();
    for (auto dependIter : module->getDependMap()) {
        auto dependModuleName = dependIter.first;
        auto dependModule = dependIter.second;
        if (!dependModule) {
            AUTIL_LOG(ERROR, "depend module is nullptr, module[%s]", dependModuleName.c_str());
            return false;
        }
        if (dependModule->isLoad()) {
            continue;
        }
        if (!load(status, dependModule, depth + 1)) {
            AUTIL_LOG(ERROR, "depend module load failed, module[%s]", dependModuleName.c_str());
            return false;
        }
    }
    if (!module->load()) {
        AUTIL_LOG(ERROR, "load module fail, module[%s]", moduleName.c_str());
        return false;
    }
    AUTIL_LOG(INFO, "load module success, module[%s]", moduleName.c_str());
    if (!module->update(status)) {
        AUTIL_LOG(ERROR, "update base module fail, module[%s]", moduleName.c_str());
        return false;
    }
    return true;
}

bool ModuleManager::unload(Status status, const std::shared_ptr<BaseModule> &module, int32_t depth) {
    if (depth >= MODULE_MAX_DEPTH) {
        AUTIL_LOG(ERROR, "check module depend, recursion depth exceeded limit");
        return false;
    }
    if (module->needLoad(status)) {
        return true;
    }
    for (auto dependedPair : module->getBeDependedMap()) {
        auto dependedModuleName = dependedPair.first;
        auto dependedModule = dependedPair.second;
        if (!dependedModule) {
            AUTIL_LOG(ERROR, "depended module is nullptr, module[%s]", dependedModuleName.c_str());
            return false;
        }
        if (!dependedModule->isLoad()) {
            continue;
        }
        if (!unload(status, dependedModule, depth + 1)) {
            AUTIL_LOG(ERROR, "depended module load failed, module[%s]", dependedModuleName.c_str());
            return false;
        }
    }
    if (!module->unload()) {
        AUTIL_LOG(ERROR, "unload module fail, module[%s]", module->getName().c_str());
        return false;
    }
    return true;
}

bool ModuleManager::unloadForStop(const std::shared_ptr<BaseModule> &module, int32_t depth) {
    if (depth >= MODULE_MAX_DEPTH) {
        AUTIL_LOG(ERROR, "check module depend, recursion depth exceeded limit");
        return false;
    }
    for (auto dependedPair : module->getBeDependedMap()) {
        auto dependedModuleName = dependedPair.first;
        auto dependedModule = dependedPair.second;
        if (!dependedModule) {
            AUTIL_LOG(ERROR, "depended module is nullptr, module[%s]", dependedModuleName.c_str());
            return false;
        }
        if (!dependedModule->isLoad()) {
            continue;
        }
        if (!unloadForStop(dependedModule, depth + 1)) {
            AUTIL_LOG(ERROR, "depended module load failed, module[%s]", dependedModuleName.c_str());
            return false;
        }
    }
    if (!module->unload()) {
        AUTIL_LOG(ERROR, "stop module fail, module[%s]", module->getName().c_str());
        return false;
    }
    return true;
}

std::shared_ptr<BaseModule> ModuleManager::getModule(const std::string &moduleName) {
    auto iter = _modules.find(moduleName);
    if (iter == _modules.end()) {
        AUTIL_LOG(WARN, "module not found, module name[%s]", moduleName.c_str());
        return nullptr;
    }
    return iter->second;
}

} // namespace admin
} // namespace swift
