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
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/admin/AdminStatusManager.h"
#include "swift/admin/modules/BaseModule.h"

namespace swift {
namespace config {
class AdminConfig;
}
namespace admin {
class SysController;

class ModuleManager {
    using ModuleMap = std::map<std::string, std::shared_ptr<BaseModule>>;
    using ModuleVec = std::vector<std::pair<std::string, std::shared_ptr<BaseModule>>>;

public:
    ModuleManager();
    ~ModuleManager();
    ModuleManager(const ModuleManager &) = delete;
    ModuleManager &operator=(const ModuleManager &) = delete;

public:
    bool
    init(AdminStatusManager *adminStatusManager, admin::SysController *sysController, config::AdminConfig *adminConfig);
    bool start();
    bool update(Status status);
    void stop();

public:
    template <typename ModuleClass>
    std::shared_ptr<ModuleClass> getModule();
    AdminStatusManager *getAdminStatusManager() { return _adminStatusManager; }

private:
    bool createModule();
    bool bindDependModule();
    bool loadSysModule();
    bool load(Status status, const std::shared_ptr<BaseModule> &module, int32_t depth = 0);
    bool unload(Status status, const std::shared_ptr<BaseModule> &module, int32_t depth = 0);
    bool unloadForStop(const std::shared_ptr<BaseModule> &module, int32_t depth = 0);
    std::shared_ptr<BaseModule> getModule(const std::string &moduleName);

private:
    // for unit test
    template <typename ModuleClass>
    bool loadModule();
    template <typename ModuleClass>
    bool unLoadModule();

private:
    AdminStatusManager *_adminStatusManager = nullptr;
    admin::SysController *_sysController = nullptr;
    config::AdminConfig *_adminConfig = nullptr;
    autil::ThreadMutex _lock;
    ModuleMap _modules;
    std::atomic_bool _stopped = true;
    std::unordered_map<std::string, ModuleCreatorFunc> _moduleCreatorFunc;

private:
    static constexpr int32_t MODULE_MAX_DEPTH = 128;
    AUTIL_LOG_DECLARE();
};

template <typename ModuleClass>
std::shared_ptr<ModuleClass> ModuleManager::getModule() {
    std::string moduleName(typeid(ModuleClass).name());
    auto iter = _modules.find(moduleName);
    if (iter == _modules.end()) {
        AUTIL_LOG(WARN, "module not found, module name[%s]", moduleName.c_str());
        return nullptr;
    }
    return std::dynamic_pointer_cast<ModuleClass>(iter->second);
}

// for unit test
template <typename ModuleClass>
bool ModuleManager::loadModule() {
    std::string moduleName(typeid(ModuleClass).name());
    auto module = getModule<ModuleClass>();
    if (!module || !module->load()) {
        AUTIL_LOG(ERROR, "module load failed, module name[%s]", moduleName.c_str());
        return false;
    }
    return true;
}

template <typename ModuleClass>
bool ModuleManager::unLoadModule() {
    std::string moduleName(typeid(ModuleClass).name());
    auto module = getModule<ModuleClass>();
    if (!module) {
        AUTIL_LOG(ERROR, "module not register, module name[%s]", moduleName.c_str());
        return false;
    }
    if (!module->unload()) {
        AUTIL_LOG(ERROR, "module unload failed, module name[%s]", moduleName.c_str());
        return false;
    }
    return true;
}

} // namespace admin
} // namespace swift
