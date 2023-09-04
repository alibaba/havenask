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
#ifndef CARBON_GROUPSCHEDULER_H
#define CARBON_GROUPSCHEDULER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/Role.h"
#include "carbon/GroupPlan.h"

BEGIN_CARBON_NAMESPACE(master);

class GroupScheduler
{
public:
    GroupScheduler();
    virtual ~GroupScheduler();
private:
    GroupScheduler(const GroupScheduler &);
    GroupScheduler& operator=(const GroupScheduler &);
public:
    virtual void init(const std::string &configStr) = 0;
    virtual void generateScheduleParams(
            const version_t &latestGroupVersion,
            const GroupPlan &targetPlan,
            const RoleMap &curRoles,
            std::map<std::string, ScheduleParams> *scheduleParamsMap) const = 0;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(GroupScheduler);

END_CARBON_NAMESPACE(master);

#endif //CARBON_GROUPSCHEDULER_H
