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
#include "swift/admin/WorkerManager.h"

#include <cstddef>
#include <map>
#include <memory>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "hippo/DriverCommon.h"
#include "master_framework/proto/SimpleMaster.pb.h"
#include "swift/admin/AppPlanMaker.h"
#include "swift/common/PathDefine.h"
#include "swift/config/ConfigVersionManager.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, WorkerManager);

using namespace std;
using namespace autil;
using namespace hippo;
using namespace swift::config;
using namespace swift::common;
using namespace swift::protocol;

USE_MASTER_FRAMEWORK_NAMESPACE(simple_master);

string WorkerManager::BROKER_GROUP_NAME = "broker";
string WorkerManager::BROKER_ROLE_NAME_PREFIX = "broker_";

WorkerManager::WorkerManager() : _scheduler(NULL) {}

WorkerManager::~WorkerManager() {
    DELETE_AND_SET_NULL(_scheduler);
    while (!_appPlanMakerQueue.empty()) {
        AppPlanMaker *appPlanMaker = _appPlanMakerQueue.back();
        DELETE_AND_SET_NULL(appPlanMaker);
        _appPlanMakerQueue.pop_back();
    }
}

bool WorkerManager::init(const string &hippoRoot, const string &applicationId, LeaderChecker *leaderChecker) {
    _applicationId = applicationId;
    _scheduler = createSimpleMasterScheduler(hippoRoot, _applicationId, leaderChecker);
    if (!_scheduler) {
        return false;
    }
    if (!_scheduler->start()) {
        AUTIL_LOG(WARN, "AdminWorker init failed : start schduler failed!");
        return false;
    }
    return true;
}

void WorkerManager::fillBrokerPlan(const ConfigVersionManager &versionManager, AppPlan &appPlan) {
    if (versionManager.currentBrokerRoleVersion == versionManager.targetBrokerRoleVersion &&
        versionManager.currentBrokerConfigPath == versionManager.targetBrokerConfigPath) {
        AppPlanMaker *appPlanMaker =
            getAppPlanMaker(versionManager.targetBrokerConfigPath, versionManager.targetBrokerRoleVersion);
        if (!appPlanMaker) {
            return;
        }
        appPlanMaker->makeBrokerRolePlan(appPlan);
    } else {
        AppPlanMaker *currentAppPlanMaker =
            getAppPlanMaker(versionManager.currentBrokerConfigPath, versionManager.currentBrokerRoleVersion);
        if (!currentAppPlanMaker) {
            return;
        }
        currentAppPlanMaker->makeBrokerRolePlan(appPlan);
        AppPlanMaker *targetAppPlanMaker =
            getAppPlanMaker(versionManager.targetBrokerConfigPath, versionManager.targetBrokerRoleVersion);
        if (!targetAppPlanMaker) {
            return;
        }
        targetAppPlanMaker->makeBrokerRolePlan(appPlan);
        AUTIL_LOG(INFO,
                  "add broker role from config [%s], role version [%s]",
                  versionManager.targetBrokerConfigPath.c_str(),
                  versionManager.targetBrokerRoleVersion.c_str());
    }
}

void WorkerManager::updateAppPlan(const AppPlan &appPlan) { _scheduler->setAppPlan(appPlan); }

AppPlanMaker *WorkerManager::getAppPlanMaker(const string &configPath, const string &roleVersion) {
    for (size_t i = 0; i < _appPlanMakerQueue.size(); i++) {
        if (_appPlanMakerQueue[i]->getConfigPath() == configPath &&
            _appPlanMakerQueue[i]->getRoleVersion() == roleVersion) {
            return _appPlanMakerQueue[i];
        }
    }
    if (_appPlanMakerQueue.size() > 10) {
        AppPlanMaker *appPlanMaker = _appPlanMakerQueue.back();
        DELETE_AND_SET_NULL(appPlanMaker);
        _appPlanMakerQueue.pop_back();
    }
    AppPlanMaker *appPlanMaker = new AppPlanMaker();
    if (!appPlanMaker->init(configPath, roleVersion)) {
        DELETE_AND_SET_NULL(appPlanMaker);
        return NULL;
    }
    _appPlanMakerQueue.push_front(appPlanMaker);
    return appPlanMaker;
}

uint32_t
WorkerManager::getRoleCount(const string &configPath, const string &roleVersion, const protocol::RoleType &roleType) {
    AppPlanMaker *appPlanMaker = getAppPlanMaker(configPath, roleVersion);
    if (!appPlanMaker) {
        return 0;
    }
    return appPlanMaker->getRoleCount(roleType);
}

void WorkerManager::getRoleNames(const string &configPath,
                                 const string &roleVersion,
                                 const protocol::RoleType &roleType,
                                 vector<string> &roleNames) {
    AppPlanMaker *appPlanMaker = getAppPlanMaker(configPath, roleVersion);
    if (!appPlanMaker) {
        return;
    }
    roleNames = appPlanMaker->getRoleName(roleType);
}

void WorkerManager::getReadyRoleCount(const protocol::RoleType roleType,
                                      const string &configPath,
                                      const string &roleVersion,
                                      uint32_t &total,
                                      uint32_t &ready) {
    vector<string> allRoleName;
    AppPlanMaker *appPlanMaker = getAppPlanMaker(configPath, roleVersion);
    if (!appPlanMaker) {
        return;
    }
    allRoleName = appPlanMaker->getRoleName(roleType);

    map<string, SlotInfos> roleSlots;
    map<string, SlotInfos>::iterator iter;
    _scheduler->getAllRoleSlots(roleSlots);

    total = allRoleName.size();
    ready = 0;
    for (size_t i = 0; i < allRoleName.size(); i++) {
        string &roleName = allRoleName[i];
        iter = roleSlots.find(roleName);
        if (iter == roleSlots.end()) {
            continue;
        }
        SlotInfos &slotInfos = iter->second;
        for (size_t i = 0; i < slotInfos.size(); i++) {
            hippo::SlotInfo &slotInfo = slotInfos[i];
            vector<hippo::ProcessStatus> &processStatus = slotInfo.processStatus;
            if (processStatus.size() != 0 && processStatus[0].status == ProcessStatus::PS_RUNNING) {
                ready++;
                break;
            }
        }
    }
}

bool WorkerManager::getIp(const string &address, string &ip) {
    uint16_t port;
    if (!PathDefine::parseAddress(address, ip, port)) {
        AUTIL_LOG(WARN, "parse slot address failed [%s]", address.c_str());
        return false;
    }
    return true;
}

void WorkerManager::releaseSlotsPref(const vector<string> &roleNames) {
    hippo::ReleasePreference pref;
    pref.type = hippo::ReleasePreference::RELEASE_PREF_PROHIBIT;
    pref.leaseMs = 10 * 60 * 1000; // 10 min
    vector<hippo::SlotId> toReleaseSlots;
    vector<string> slotNames;
    vector<string> slotAddrs;
    for (size_t roleIdx = 0; roleIdx < roleNames.size(); ++roleIdx) {
        const SlotInfos &slotInfos = _scheduler->getRoleSlots(roleNames[roleIdx]);
        if (slotInfos.empty()) {
            AUTIL_LOG(INFO, "release slot not found, role[%s]", roleNames[roleIdx].c_str());
        }
        for (size_t slotIdx = 0; slotIdx < slotInfos.size(); ++slotIdx) {
            toReleaseSlots.push_back(slotInfos[slotIdx].slotId);
            slotNames.push_back(roleNames[roleIdx]);
            slotAddrs.push_back(slotInfos[slotIdx].slotId.slaveAddress);
        }
    }
    if (toReleaseSlots.empty()) {
        return;
    }
    AUTIL_LOG(INFO,
              "will release roles[%s], address[%s]",
              StringUtil::toString(slotNames, ",").c_str(),
              StringUtil::toString(slotAddrs, ",").c_str());
    _scheduler->releaseSlots(toReleaseSlots, pref);
}

SimpleMasterScheduler *WorkerManager::createSimpleMasterScheduler(const string &hippoRoot,
                                                                  const string &applicationId,
                                                                  LeaderChecker *leaderChecker) const {
    SimpleMasterScheduler *scheduler = new SimpleMasterScheduler;
    if (!scheduler->init(hippoRoot, leaderChecker, applicationId)) {
        AUTIL_LOG(ERROR,
                  "Init hippo master scheduler failed. hippoRoot[%s], appId[%s]",
                  hippoRoot.c_str(),
                  applicationId.c_str());
        delete scheduler;
        return NULL;
    }
    return scheduler;
}

} // namespace admin
} // namespace swift
