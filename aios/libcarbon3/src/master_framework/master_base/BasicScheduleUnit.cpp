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
#include "master_framework/BasicScheduleUnit.h"
#include "common/Log.h"

using namespace std;
using namespace autil;
BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

//MASTER_FRAMEWORK_LOG_SETUP(master_base, BasicScheduleUnit);

BasicScheduleUnit::BasicScheduleUnit(
        const std::string &name,
        hippo::MasterDriver *hippoMasterDriver) :
    ScheduleUnit(name, hippoMasterDriver)
{
    
}

BasicScheduleUnit::~BasicScheduleUnit() {
}

void BasicScheduleUnit::getRolePlans(RolePlanMap &rolePlans) const {
    ScopedLock lock(_lock);
    rolePlans.insert(_rolePlans.begin(), _rolePlans.end());
}

void BasicScheduleUnit::setRolePlans(const RolePlanMap &rolePlans) {
    ScopedLock lock(_lock);
    _rolePlans = rolePlans;
}

void BasicScheduleUnit::getSlotInfos(SlotInfos &slotInfos) const {
    ScopedLock lock(_lock);
    slotInfos.insert(slotInfos.end(), _slotInfos.begin(), _slotInfos.end());
}

void BasicScheduleUnit::setSlotInfos(const SlotInfos &slotInfos) {
    ScopedLock lock(_lock);
    _slotInfos = slotInfos;
}

void BasicScheduleUnit::getRoles(set<string> &roles) const {
    ScopedLock lock(_lock);
    for (RolePlanMap::const_iterator it = _rolePlans.begin();
         it != _rolePlans.end(); it++)
    {
        roles.insert(it->first);
    }
}

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

