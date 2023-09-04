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
#include "swift/admin/modules/BaseModule.h"

#include <bitset>
#include <typeinfo>

#include "swift/admin/ModuleManager.h"

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, BaseModule);

using namespace swift::common;

const LoadStatus MASTER_MASK_BIT("111000");
const LoadStatus LEADER_MASK_BIT("000111");

BaseModule::BaseModule() : _load(false) {}
BaseModule::~BaseModule() {}

void BaseModule::setLoadStatus(LoadStatus loadStatus) { _loadStatus = loadStatus; }

void BaseModule::setSysModule() { _sysModule = true; }

bool BaseModule::isSysModule() const { return _sysModule; }

void BaseModule::setName(const std::string &name) { _name = name; }

const std::string &BaseModule::getName() const { return _name; }

bool BaseModule::init(ModuleManager *moduleManager,
                      admin::SysController *sysController,
                      config::AdminConfig *adminConfig) {
    if (!moduleManager || !sysController || !adminConfig) {
        AUTIL_LOG(ERROR, "base module init failed, sysController[%p], adminConfig[%p].", sysController, adminConfig);
        return false;
    }
    _moduleManager = moduleManager;
    _sysController = sysController;
    _adminConfig = adminConfig;
    if (!doInit()) {
        AUTIL_LOG(WARN, "doInit failed, module[%s]", typeid(this).name());
        return false;
    }
    return true;
}

bool BaseModule::load() {
    if (_load) {
        AUTIL_LOG(DEBUG, "module has loaded, module[%s]", typeid(this).name());
        return true;
    }
    if (!doLoad()) {
        AUTIL_LOG(ERROR, "load module failed, module[%s]", typeid(this).name());
        return false;
    }
    _load = true;
    return true;
}

bool BaseModule::unload() {
    if (!_load) {
        AUTIL_LOG(DEBUG, "module has unloaded, module[%s]", typeid(this).name());
        return true;
    }
    if (!doUnload()) {
        AUTIL_LOG(ERROR, "unload module failed, module[%s]", typeid(this).name());
        return false;
    }
    _load = false;
    return true;
}

void BaseModule::clear() {
    _dependMap.clear();
    _beDependedMap.clear();
    _bindFuncMap.clear();
}

bool BaseModule::update(Status status) {
    if (!doUpdate(status)) {
        AUTIL_LOG(ERROR, "update module failed, module[%s]", typeid(this).name());
        return false;
    }
    return true;
}

bool BaseModule::needLoad(Status status) const {
    if (isSysModule()) {
        return true;
    }
    LoadStatus loadStatus;
    loadStatus.set(status.msStatus);
    loadStatus.set(status.lfStatus);
    auto masterBit = _loadStatus & loadStatus & MASTER_MASK_BIT;
    auto leaderBit = _loadStatus & loadStatus & LEADER_MASK_BIT;
    return masterBit.any() && leaderBit.any();
}

bool BaseModule::isLoad() const { return _load; }

bool BaseModule::isMaster() const {
    auto adminStatusManager = _moduleManager->getAdminStatusManager();
    if (adminStatusManager) {
        return adminStatusManager->isMaster();
    }
    return false;
}

bool BaseModule::isLeader() const {
    auto adminStatusManager = _moduleManager->getAdminStatusManager();
    if (adminStatusManager) {
        return adminStatusManager->isLeader();
    }
    return false;
}

LoadStatus BaseModule::calcLoadStatus(const std::string &masterTag, const std::string &leaderTag) {
    LoadStatus tag;
    if (masterTag.find('M') != std::string::npos) {
        tag.set(MS_MASTER);
    }
    if (masterTag.find('S') != std::string::npos) {
        tag.set(MS_SLAVE);
    }
    if (masterTag.find('U') != std::string::npos) {
        tag.set(MS_UNKNOWN);
    }
    if (leaderTag.find('L') != std::string::npos) {
        tag.set(LF_LEADER);
    }
    if (leaderTag.find('F') != std::string::npos) {
        tag.set(LF_FOLLOWER);
    }
    if (leaderTag.find('U') != std::string::npos) {
        tag.set(LF_UNKNOWN);
    }
    return tag;
}

void BaseModule::beDepended(const std::string &name, const std::shared_ptr<BaseModule> &module) {
    _beDependedMap[name] = module;
}

BaseModule::ModuleMap &BaseModule::getDependMap() { return _dependMap; }

BaseModule::ModuleMap &BaseModule::getBeDependedMap() { return _beDependedMap; }

BaseModule::BindFuncMap BaseModule::getBindFuncMap() const { return _bindFuncMap; }

} // namespace admin
} // namespace swift
