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
#ifndef MASTER_FRAMEWORK_BASICSCHEDULEUNIT_H
#define MASTER_FRAMEWORK_BASICSCHEDULEUNIT_H

#include "master_framework/common.h"
#include "master_framework/Plan.h"
#include "master_framework/AppPlan.h"
#include "master_framework/ScheduleUnit.h"
#include "hippo/DriverCommon.h"
#include "hippo/MasterDriver.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

class BasicScheduleUnit : public ScheduleUnit
{
public:
    BasicScheduleUnit(const std::string &name,
                 hippo::MasterDriver *hippoMasterDriver);
    virtual ~BasicScheduleUnit();
private:
    BasicScheduleUnit(const BasicScheduleUnit &);
    BasicScheduleUnit& operator=(const BasicScheduleUnit &);
    
public:
    /* override */ virtual ScheduleUnitType getType() const {
        return SUT_BASIC;
    }

    /* override */ virtual void getRolePlans(RolePlanMap &rolePlans) const;

    /* override */ virtual void getSlotInfos(SlotInfos &slotInfos) const;

    /* override */ virtual void getRoles(std::set<std::string> &roles) const;
    
protected:
    void setRolePlans(const RolePlanMap &rolePlans);

    void setSlotInfos(const SlotInfos &slotInfos);
    
protected:
    RolePlanMap _rolePlans;
    SlotInfos _slotInfos;
    mutable autil::ThreadMutex _lock;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(BasicScheduleUnit);

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

#endif //MASTER_FRAMEWORK_BASICSCHEDULEUNIT_H
