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
#ifndef CARBON_DEFAULTGROUPSCHEDULER_H
#define CARBON_DEFAULTGROUPSCHEDULER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/GroupScheduler.h"
#include "carbon/GroupPlan.h"

BEGIN_CARBON_NAMESPACE(master);

class DefaultGroupScheduler : public GroupScheduler
{
public:
    DefaultGroupScheduler();
    ~DefaultGroupScheduler();
private:
    DefaultGroupScheduler(const DefaultGroupScheduler &);
    DefaultGroupScheduler& operator=(const DefaultGroupScheduler &);
public:
    /* override */ void init(const std::string &configStr);
    /* override */ void generateScheduleParams(
            const version_t &latestGroupVersion,
            const GroupPlan &targetPlan,
            const RoleMap &curRoles,
            std::map<roleid_t, ScheduleParams> *scheduleParamsMap) const;
    
private:
    void getRoleVerAvlPercent(
        const RoleMap &curRoles,
        std::map<roleid_t, PercentMap> *roleVerAvlPercentMap) const;

    void fillScheduleParamsMap(const GroupPlan &targetPlan,
                               const RoleMap &curRoles,
                               std::map<roleid_t, PercentMap> &roleVerHoldPercent,
                               int64_t timeStamp,
                               std::map<roleid_t, ScheduleParams> *scheduleParamsMap) const;

public:
    friend class DefaultGroupSchedulerTest;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(DefaultGroupScheduler);

END_CARBON_NAMESPACE(master);

#endif //CARBON_DEFAULTGROUPSCHEDULER_H
