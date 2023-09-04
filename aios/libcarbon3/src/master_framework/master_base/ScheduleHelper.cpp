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
#include "master_base/ScheduleHelper.h"
#include "common/Log.h"

using namespace std;
using namespace hippo;

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

MASTER_FRAMEWORK_LOG_SETUP(master_base, ScheduleHelper);

ScheduleHelper::ScheduleHelper() {
}

ScheduleHelper::~ScheduleHelper() {
}

void ScheduleHelper::getRolesSlots(const set<string> &roles,
                                   const vector<SlotInfo> &slotInfos,
                                   vector<SlotInfo> &roleSlotInfos)
{
    for (size_t i = 0; i < slotInfos.size(); i++) {
        const string &name = slotInfos[i].role;
        if (roles.count(name) > 0) {
            roleSlotInfos.push_back(slotInfos[i]);
        }
    }
}

void ScheduleHelper::getRolesSlotInfos(
        const map<string, SlotInfos> &allRoleSlotInfos,
        const set<string> &roles,
        map<string, SlotInfos> &roleSlotInfos)
{
    for (set<string>::const_iterator it = roles.begin();
         it != roles.end(); it++)
    {
        map<string, SlotInfos>::const_iterator roleIt =
            allRoleSlotInfos.find(*it);
        if (roleIt == allRoleSlotInfos.end()) {
            continue;
        }
        roleSlotInfos[roleIt->first] = roleIt->second;
    }
}

ProcessContext ScheduleHelper::getProcessContext(
        const RolePlan &rolePlan)
{
    ProcessContext pcxt;
    pcxt.pkgs = rolePlan.packageInfos;
    pcxt.processes = rolePlan.processInfos;
    pcxt.datas = rolePlan.dataInfos;

    return pcxt;
}

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

