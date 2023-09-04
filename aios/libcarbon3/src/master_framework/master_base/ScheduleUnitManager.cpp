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
#include "master_framework/ScheduleUnitManager.h"
#include "common/Log.h"

using namespace std;
using namespace autil;
using namespace hippo;

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

MASTER_FRAMEWORK_LOG_SETUP(master_base, ScheduleUnitManager);

#define SCHEDULER_UNIT_MANAGER_NAME "__sum_name__"

ScheduleUnitManager::ScheduleUnitManager() {
    _hippoMasterDriver = hippo::MasterDriver::createDriver("master");
    _comboScheduleUnitPtr.reset(new ComboScheduleUnit(
                    SCHEDULER_UNIT_MANAGER_NAME,
                    _hippoMasterDriver));
    _canSchedule = false;
}

ScheduleUnitManager::~ScheduleUnitManager() {
    stop();
    delete _hippoMasterDriver;
}

bool ScheduleUnitManager::init(
        const string &hippoZkRoot,
        worker_framework::LeaderElector *leaderElector,
        const string &applicationId)
{
    if (!_hippoMasterDriver->init(hippoZkRoot,
                                  leaderElector,
                                  applicationId))
    {
        MF_LOG(ERROR, "init master driver failed.");
        return false;
    }

    MF_LOG(INFO, "init schedule manager success.");
    return true;
}

bool ScheduleUnitManager::start() {
    if (!_hippoMasterDriver->start()) {
        MF_LOG(ERROR, "start master driver failed.");
        return false;
    }
    
    _scheduleThreadPtr = LoopThread::createLoopThread(
            std::bind(&ScheduleUnitManager::schedule, this),
            SCHEDULE_LOOPINTERVAL);
    if (_scheduleThreadPtr == NULL) {
        MF_LOG(ERROR, "init loop thread failed.");
        return false;
    }

    MF_LOG(INFO, "start schedule.");
    return true;
}

bool ScheduleUnitManager::stop() {
    if (!_hippoMasterDriver->stop()) {
        MF_LOG(ERROR, "stop master driver failed.");
        return false;
    }
    
    if (_scheduleThreadPtr != NULL) {
        _scheduleThreadPtr.reset();
    }
    return true;
}

void ScheduleUnitManager::release(const ScheduleUnitPtr &scheduleUnitPtr) {
    _comboScheduleUnitPtr->releaseScheduleUnit(scheduleUnitPtr->getName());
}

ScheduleUnitPtr ScheduleUnitManager::createScheduleUnit(
        const std::string &name,
        const ScheduleUnitFactoryPtr &scheduleUnitFactoryPtr)
{
    ScheduleUnitPtr scheduleUnitPtr =
        _comboScheduleUnitPtr->getScheduleUnit(name);
     if (!scheduleUnitPtr) {
        scheduleUnitPtr =  scheduleUnitFactoryPtr->createScheduleUnit(name,
                _hippoMasterDriver);
        _comboScheduleUnitPtr->addScheduleUnit(scheduleUnitPtr);
     }
     
     return scheduleUnitPtr;
}

void ScheduleUnitManager::setProhibitedIps(const std::vector<std::string>& ips) {
    ScopedLock lock(_lock);
    _prohibitedIps.clear();
    _prohibitedIps = ips;
}

void ScheduleUnitManager::snapshotProhibitedIps(std::vector<std::string>& ips) {
    ScopedLock lock(_lock);
    ips.clear();
    ips = _prohibitedIps;
}

void ScheduleUnitManager::schedule() {
    assert(_hippoMasterDriver);

    if (!_canSchedule) {
        return;
    }
    
    RoleSlotInfos roleSlots;
    getRoleSlotInfos(roleSlots);

    RolePlanMap rolePlans;
    _comboScheduleUnitPtr->getRolePlans(rolePlans);
    std::vector<std::string> prohibitedIps;
    snapshotProhibitedIps(prohibitedIps);
    setRolePlans(rolePlans, prohibitedIps);
    releaseUselessRoles(roleSlots, rolePlans);
    
    _comboScheduleUnitPtr->schedule(roleSlots);
}

void ScheduleUnitManager::getRoleSlotInfos(RoleSlotInfos &roleSlots) {
    SlotInfos slotInfos;
    _hippoMasterDriver->getSlots(&slotInfos);
    
    for (size_t i = 0; i < slotInfos.size(); i++) {
        const SlotInfo &slotInfo = slotInfos[i];
        const string &role = slotInfo.role;
        roleSlots[role].push_back(slotInfo);
    }
}

void ScheduleUnitManager::setRolePlans(const RolePlanMap &rolePlans,
                                       const std::vector<std::string>& prohibitedIps) {
    for (RolePlanMap::const_iterator it = rolePlans.begin();
         it != rolePlans.end(); it++)
    {
        const string &role = it->first;
        const RolePlan &rolePlan = it->second;
        RoleRequest request;
        request.options = rolePlan.slotResources;
        request.declare = rolePlan.declareResources;
        request.queue = rolePlan.queue;
        request.groupId = "";
        request.allocateMode = rolePlan.allocateMode;
        request.count = rolePlan.count;
        request.priority = rolePlan.priority;
        request.metaTags = rolePlan.metaTags;
        request.cpusetMode = hippo::CpusetMode::RESERVED;
        request.constraints.prohibitedIps = prohibitedIps;
        request.containerConfigs = rolePlan.containerConfigs;
        
        ProcessContext processContext;
        processContext.pkgs = rolePlan.packageInfos;
        processContext.processes = rolePlan.processInfos;
        processContext.datas = rolePlan.dataInfos;

        request.context = processContext;
        _hippoMasterDriver->updateRoleRequest(role, request);

        if (rolePlan.properties.find("useSpecifiedRestartCountLimit") ==
            rolePlan.properties.end())
        {
            modifyProcessRestartCountLimit(processContext.processes);
        } 
        
        _hippoMasterDriver->setRoleProcess(role, processContext);
        
        MF_LOG(DEBUG, "set role plans, role:[%s].", role.c_str());
    }
}

void ScheduleUnitManager::modifyProcessRestartCountLimit(
        vector<ProcessInfo> &processInfos)
{
    for (size_t i = 0; i < processInfos.size(); i++) {
        ProcessInfo &processInfo = processInfos[i];
        processInfo.restartCountLimit = 1000000;
    }
}

void ScheduleUnitManager::releaseUselessRoles(
        const RoleSlotInfos &roleSlots,
        const RolePlanMap &rolePlans)
{
    ReleasePreference pref;
    for (RoleSlotInfos::const_iterator it = roleSlots.begin();
         it != roleSlots.end(); it++)
    {
        const string &role = it->first;
        if (rolePlans.find(role) != rolePlans.end()) {
            continue;
        }
        
        _hippoMasterDriver->releaseRoleSlots(role, pref);
        _hippoMasterDriver->clearRoleProcess(role);
        MF_LOG(INFO, "release useless role [%s] slots.", role.c_str());
    }
}

void ScheduleUnitManager::startSchedule() {
    if (_canSchedule) {
        return;
    }

    _canSchedule = true;
    MF_LOG(INFO, "start do scheduling.");
}

void ScheduleUnitManager::stopSchedule() {
    if (!_canSchedule) {
        return;
    }

    _canSchedule = false;
    MF_LOG(INFO, "stop to scheduling.");
}

hippo::LeaderSerializer* ScheduleUnitManager::createLeaderSerializer(
        const string &zookeeperRoot,
        const string &fileName,
        const string &backupRoot)
{
    // TODO: check the zk host
    return _hippoMasterDriver->createLeaderSerializer(zookeeperRoot,
            fileName, backupRoot);
}

void ScheduleUnitManager::releaseSlots(
        const std::vector<hippo::SlotId> &slotIds,
        const hippo::ReleasePreference &pref)
{
    for (size_t i = 0; i < slotIds.size(); i++) {
        _hippoMasterDriver->releaseSlot(slotIds[i], pref);
    }
}


END_MASTER_FRAMEWORK_NAMESPACE(master_base);

