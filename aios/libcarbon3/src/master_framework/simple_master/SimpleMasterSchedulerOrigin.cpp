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
#include "simple_master/SimpleMasterSchedulerOrigin.h"
#include "simple_master/SimpleMasterServiceImpl.h"
#include "common/Log.h"
#include "autil/StringUtil.h"
#include "fslib/fslib.h"

using namespace std;
using namespace hippo;
using namespace autil;
USE_MASTER_FRAMEWORK_NAMESPACE(master_base);
BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

MASTER_FRAMEWORK_LOG_SETUP(simple_master, SimpleMasterSchedulerOrigin);

SimpleMasterSchedulerOrigin::SimpleMasterSchedulerOrigin() {
    _scheduleUnitManagerPtr.reset(new ScheduleUnitManager());
    _partScheduleUnitFactoryPtr.reset(new PartitionScheduleUnitFactory());
}

SimpleMasterSchedulerOrigin::~SimpleMasterSchedulerOrigin() {
    stop();
}

bool SimpleMasterSchedulerOrigin::init(
        const std::string &hippoZkRoot,
        worker_framework::LeaderChecker *leaderChecker,
        const std::string &applicationId)
{
    if (!_scheduleUnitManagerPtr->init(hippoZkRoot,
                    leaderChecker,
                    applicationId))
    {
        MF_LOG(ERROR, "init ScheduleUnitManager failed!");
        return false;
    }
        
    return true;
}

bool SimpleMasterSchedulerOrigin::start() {
    if (!_scheduleUnitManagerPtr->start()) {
        MF_LOG(ERROR, "start ScheduleUnitManager failed!");
        return false;
    }
    return true;
}

bool SimpleMasterSchedulerOrigin::stop() {
    _scheduleUnitManagerPtr->stopSchedule();
    return _scheduleUnitManagerPtr->stop();
}

SlotInfos SimpleMasterSchedulerOrigin::getRoleSlots(const string &roleName) {
    map<string, SlotInfos> roleSlots;
    _scheduleUnitManagerPtr->getRoleSlotInfos(roleSlots);
    
    if (roleSlots.find(roleName) == roleSlots.end()) {
        return SlotInfos();
    }
    
    return roleSlots[roleName];
}

void SimpleMasterSchedulerOrigin::getAllRoleSlots(
        map<string, SlotInfos> &roleSlots)
{
    _scheduleUnitManagerPtr->getRoleSlotInfos(roleSlots);
}

void SimpleMasterSchedulerOrigin::setAppPlan(const AppPlan &appPlan) {
    RolePlanMap rolePlans;
    RolePlanMap::const_iterator rolePlanIt;
    for (rolePlanIt = appPlan.rolePlans.begin();
         rolePlanIt != appPlan.rolePlans.end(); rolePlanIt++)
    {
        RolePlan &rolePlan = rolePlans[rolePlanIt->first];
        rolePlan = rolePlanIt->second;

        if (rolePlan.packageInfos.empty()) {
            rolePlan.packageInfos = appPlan.packageInfos;
        }

        if (rolePlan.count == -1) {
            rolePlan.count = INF_SLOT_COUNT;
        }
    }
    _scheduleUnitManagerPtr->setProhibitedIps(appPlan.prohibitedIps);
    updateScheduleUnits(rolePlans);
    _scheduleUnitManagerPtr->startSchedule();
}

void SimpleMasterSchedulerOrigin::updateScheduleUnits(
        const RolePlanMap &rolePlans)
{
    ScopedLock lock(_lock);
    RolePlanMap::const_iterator rolePlanIt = rolePlans.begin();
    map<string, ScheduleUnitPtr>::iterator scheduleUnitIt =
        _roleSchedules.begin();

    for (; (rolePlanIt != rolePlans.end()
            && scheduleUnitIt != _roleSchedules.end());)
    {
        const string &roleInPlan = rolePlanIt->first;
        const string &roleInScheduleUnit = scheduleUnitIt->first;
        if (roleInPlan < roleInScheduleUnit) {
            ScheduleUnitPtr suPtr =
                _scheduleUnitManagerPtr->createScheduleUnit(
                        roleInPlan, _partScheduleUnitFactoryPtr);
            suPtr->setPlan(&rolePlanIt->second);
            _roleSchedules[roleInPlan] = suPtr;
            rolePlanIt++;
        } else if (roleInPlan > roleInScheduleUnit) {
            _scheduleUnitManagerPtr->release(scheduleUnitIt->second);
            _roleSchedules.erase(scheduleUnitIt++);
        } else {
            scheduleUnitIt->second->setPlan(&rolePlanIt->second);
            rolePlanIt++;
            scheduleUnitIt++;
        }
    }

    for (; rolePlanIt != rolePlans.end(); rolePlanIt++) {
        ScheduleUnitPtr suPtr =
            _scheduleUnitManagerPtr->createScheduleUnit(
                    rolePlanIt->first, _partScheduleUnitFactoryPtr);
        suPtr->setPlan(&rolePlanIt->second);
        _roleSchedules[rolePlanIt->first] = suPtr;
    }

    for (; scheduleUnitIt != _roleSchedules.end();) {
        _scheduleUnitManagerPtr->release(scheduleUnitIt->second);
        _roleSchedules.erase(scheduleUnitIt++);
    }
}

void SimpleMasterSchedulerOrigin::releaseSlots(const vector<hippo::SlotId> &slots,
        const hippo::ReleasePreference &pref)
{
    _scheduleUnitManagerPtr->releaseSlots(slots, pref);
}

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

