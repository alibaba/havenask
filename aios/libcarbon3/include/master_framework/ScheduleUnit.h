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
#ifndef MASTER_FRAMEWORK_SCHEDULEUNIT_H
#define MASTER_FRAMEWORK_SCHEDULEUNIT_H

#include "master_framework/common.h"
#include "master_framework/Plan.h"
#include "master_framework/AppPlan.h"
#include "hippo/DriverCommon.h"
#include "hippo/MasterDriver.h"

typedef std::vector<hippo::SlotInfo> SlotInfos;
typedef std::map<std::string, SlotInfos> RoleSlotInfos;

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

enum ScheduleUnitType {
    SUT_UNKNOWN = 0,
    SUT_BASIC = 1,
    SUT_COMBO = 2
};

class ScheduleUnit
{
public:
    ScheduleUnit(const std::string &name,
                 hippo::MasterDriver *hippoMasterDriver);
    virtual ~ScheduleUnit();
private:
    ScheduleUnit(const ScheduleUnit &);
    ScheduleUnit& operator=(const ScheduleUnit &);
    
public:
    const std::string& getName() const { return _name; }

public:
    virtual ScheduleUnitType getType() const = 0;
    
    virtual bool setPlan(const Plan *plan) = 0;

    virtual void getRolePlans(RolePlanMap &rolePlans) const = 0;

    virtual void getSlotInfos(SlotInfos &slotInfos) const = 0;
    
    virtual void schedule(const RoleSlotInfos &slotInfos) = 0;

    virtual void getRoles(std::set<std::string> &roles) const = 0;

protected:
    std::string _name;
    hippo::MasterDriver *_hippoMasterDriver;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(ScheduleUnit);

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

#endif //MASTER_FRAMEWORK_SCHEDULEUNIT_H
