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
#ifndef MASTER_FRAMEWORK_SCHEDULEHELPER_H
#define MASTER_FRAMEWORK_SCHEDULEHELPER_H

#include "master_framework/common.h"
#include "master_framework/RolePlan.h"
#include "hippo/DriverCommon.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

typedef std::vector<hippo::SlotInfo> SlotInfos;

class ScheduleHelper
{
public:
    ScheduleHelper();
    ~ScheduleHelper();
private:
    ScheduleHelper(const ScheduleHelper &);
    ScheduleHelper& operator=(const ScheduleHelper &);
public:
    static void getRolesSlots(const std::set<std::string> &roles,
                              const std::vector<hippo::SlotInfo> &slotInfos,
                              std::vector<hippo::SlotInfo> &roleSlotInfos);

    static void getRolesSlotInfos(
            const std::map<std::string, SlotInfos> &allRoleSlotInfos,
            const std::set<std::string> &roles,
            std::map<std::string, SlotInfos> &roleSlotInfos);
        
    static hippo::ProcessContext getProcessContext(const RolePlan &rolePlan);
};

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

#endif //MASTER_FRAMEWORK_SCHEDULEHELPER_H
