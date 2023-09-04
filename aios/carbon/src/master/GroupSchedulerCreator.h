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
#ifndef CARBON_GROUPSCHEDULERCREATOR_H
#define CARBON_GROUPSCHEDULERCREATOR_H

#include "common/common.h"
#include "master/GroupScheduler.h"
#include "carbon/GroupPlan.h"

BEGIN_CARBON_NAMESPACE(master);

class GroupSchedulerCreator
{
public:
    GroupSchedulerCreator();
    ~GroupSchedulerCreator();
private:
    GroupSchedulerCreator(const GroupSchedulerCreator &);
    GroupSchedulerCreator& operator=(const GroupSchedulerCreator &);
public:
    GroupSchedulerPtr createGroupScheduler(const GroupSchedulerConfig &config);
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(GroupSchedulerCreator);

END_CARBON_NAMESPACE(master);

#endif //CARBON_GROUPSCHEDULERCREATOR_H
