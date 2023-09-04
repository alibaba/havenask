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
#ifndef MASTER_FRAMEWORK_TRANSFERAPPPLANUTIL_H
#define MASTER_FRAMEWORK_TRANSFERAPPPLANUTIL_H

#include "master_framework/common.h"
#include "master_framework/AppPlan.h"
#include "carbon/GroupPlan.h"
#include "carbon/Status.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

class TransferAppPlanUtil
{
public:
    //AppPlan.rolePlans -> groupPlans.rolePlan 注: 两边RolePlan不一样
    //packageInfos-> groupPlan->rolePlan->VersionedPlan.LaunchPlan.packageInfos
    static void transferAppPlanToGroupPlans(const AppPlan &c2AppPlan, carbon::GroupPlanMap &groupPlans);
        
    //GroupStatus.roles.nodes.curWorkerNodeStatus.slotInfo -> SlotInfo
    static void transferGroupStatusesToSlotInfos(const std::vector<carbon::GroupStatus> &GroupStatuses, 
        std::map<std::string, SlotInfos> &roleSlots);
    
private:
    static void transferMFRolePlanToGroupPlan(const std::string &roleName, const master_base::RolePlan &mfRolePlan, 
        const std::vector<hippo::PackageInfo> &packageInfos, carbon::GroupPlan &groupPlan);
    
    static void transferMFRolePlanToc2RolePlan(const std::string &roleName, const master_base::RolePlan &mfRolePlan,
        carbon::RolePlan &c2RolePlan);

    static void generateGlobalPlan(const master_base::RolePlan &mfRolePlan, carbon::GlobalPlan &global);

    static void generateVersionedPlan(const master_base::RolePlan &mfRolePlan, carbon::VersionedPlan &version);

    static void generateResourcePlan(const master_base::RolePlan &mfRolePlan, carbon::ResourcePlan &resourcePlan);

    static void generateLaunchPlan(const master_base::RolePlan &mfRolePlan, carbon::LaunchPlan &launchPlan);

    static void fillPackageInfos(const std::vector<hippo::PackageInfo> &packageInfos, carbon::RolePlan &rolePlan);
};



END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

#endif