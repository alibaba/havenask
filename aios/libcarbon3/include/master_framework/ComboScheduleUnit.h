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
#ifndef MASTER_FRAMEWORK_COMBOSCHEDULEUNIT_H
#define MASTER_FRAMEWORK_COMBOSCHEDULEUNIT_H

#include "master_framework/common.h"
#include "master_framework/ScheduleUnit.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

class ComboScheduleUnit : public ScheduleUnit
{
public:
    ComboScheduleUnit(
            const std::string &name,
            hippo::MasterDriver *hippoMasterDriver);
    ~ComboScheduleUnit();
private:
    ComboScheduleUnit(const ComboScheduleUnit &);
    ComboScheduleUnit& operator=(const ComboScheduleUnit &);
    
public:
    void addScheduleUnit(const ScheduleUnitPtr &scheduleUnitPtr);
    
    void releaseScheduleUnit(const std::string &name);
    
    ScheduleUnitPtr getScheduleUnit(const std::string &name) const;

public:
    /* override */ bool setPlan(const Plan *plan) { return true; }
    
    /* override */ ScheduleUnitType getType() const { return SUT_COMBO; }

    /* override */ void getRolePlans(RolePlanMap &rolePlans) const;

    /* override */ void getSlotInfos(SlotInfos &slotInfos) const;

    /* override */ void schedule(const RoleSlotInfos &slotInfos);

    /* override */ virtual void getRoles(std::set<std::string> &roles) const;

public: // for test
    std::map<std::string, ScheduleUnitPtr> getScheduleUnits() {
        return _scheduleUnitMap;
    }
    
private:
    std::map<std::string, ScheduleUnitPtr> _scheduleUnitMap;
    mutable autil::ThreadMutex _mapLock;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(ComboScheduleUnit);

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

#endif //MASTER_FRAMEWORK_COMBOSCHEDULEUNIT_H
