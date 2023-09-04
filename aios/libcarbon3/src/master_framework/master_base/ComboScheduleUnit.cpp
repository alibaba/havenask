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
#include "master_framework/ComboScheduleUnit.h"
#include "master_base/ScheduleHelper.h"
#include "common/Log.h"

using namespace std;
using namespace autil;

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

MASTER_FRAMEWORK_LOG_SETUP(master_base, ComboScheduleUnit);

ComboScheduleUnit::ComboScheduleUnit(
        const std::string &name,
        hippo::MasterDriver *hippoMasterDriver) :
    ScheduleUnit(name, hippoMasterDriver)
{
    
}

ComboScheduleUnit::~ComboScheduleUnit() {
}

void ComboScheduleUnit::addScheduleUnit(
        const ScheduleUnitPtr &scheduleUnitPtr)
{
    ScopedLock lock(_mapLock);
    
    const string &name = scheduleUnitPtr->getName();
    map<string, ScheduleUnitPtr>::const_iterator it =
        _scheduleUnitMap.find(name);
    if (it != _scheduleUnitMap.end() && it->second != NULL) {
        return;
    }

    _scheduleUnitMap[name]  = scheduleUnitPtr;
}

ScheduleUnitPtr ComboScheduleUnit::getScheduleUnit(
        const string &name) const
{
    ScopedLock lock(_mapLock);
    map<string, ScheduleUnitPtr>::const_iterator it =
        _scheduleUnitMap.find(name);
    if (it != _scheduleUnitMap.end()) {
        return it->second;
    }
    return ScheduleUnitPtr();
}

void ComboScheduleUnit::releaseScheduleUnit(const string &name) {
    ScopedLock lock(_mapLock);

    map<string, ScheduleUnitPtr>::iterator it =
        _scheduleUnitMap.find(name);
    if (it != _scheduleUnitMap.end()) {
        _scheduleUnitMap.erase(it);
    }
}

void ComboScheduleUnit::getRolePlans(RolePlanMap &rolePlans) const {
    ScopedLock lock(_mapLock);

    map<string, ScheduleUnitPtr>::const_iterator it;
    for (it = _scheduleUnitMap.begin();
         it != _scheduleUnitMap.end(); it++)
    {
        it->second->getRolePlans(rolePlans);
        MF_LOG(DEBUG, "role plan size:[%zd]", rolePlans.size());
    }
}

void ComboScheduleUnit::getSlotInfos(SlotInfos &slotInfos) const {
    ScopedLock lock(_mapLock);

    map<string, ScheduleUnitPtr>::const_iterator it;
    for (it = _scheduleUnitMap.begin();
         it != _scheduleUnitMap.end(); it++)
    {
        it->second->getSlotInfos(slotInfos);
    }
}

void ComboScheduleUnit::getRoles(set<string> &roles) const {
    ScopedLock lock(_mapLock);

    map<string, ScheduleUnitPtr>::const_iterator it;
    for (it = _scheduleUnitMap.begin();
         it != _scheduleUnitMap.end(); it++)
    {
        it->second->getRoles(roles);
    }
}

void ComboScheduleUnit::schedule(const RoleSlotInfos &slotInfos) {
    ScopedLock lock(_mapLock);

    map<string, ScheduleUnitPtr>::const_iterator it;
    for (it = _scheduleUnitMap.begin();
         it != _scheduleUnitMap.end(); it++)
    {
        set<string> roles;
        it->second->getRoles(roles);
        RoleSlotInfos roleSlotInfos;
        ScheduleHelper::getRolesSlotInfos(slotInfos, roles, roleSlotInfos);
        it->second->schedule(roleSlotInfos);
    }
}

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

