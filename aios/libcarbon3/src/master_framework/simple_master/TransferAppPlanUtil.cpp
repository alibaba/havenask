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
#include "simple_master/TransferAppPlanUtil.h"

#include "master_framework/common.h"
#include "master_framework/AppPlan.h"
#include "common/Log.h"
#include "carbon/CommonDefine.h"
#include "carbon/GroupPlan.h"
#include "carbon/Status.h"

using namespace autil;
BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

//MASTER_FRAMEWORK_LOG_SETUP(simple_master, TransferAppPlanUtil);

void TransferAppPlanUtil::transferAppPlanToGroupPlans(const AppPlan &c2AppPlan, carbon::GroupPlanMap &groupPlans){
    for (auto& kv : c2AppPlan.rolePlans) {
        const std::string &roleName = kv.first;
        const master_base::RolePlan &mfRolePlan = kv.second;
        carbon::GroupPlan groupPlan;
        transferMFRolePlanToGroupPlan(roleName, mfRolePlan, c2AppPlan.packageInfos, groupPlan); 
        groupPlans[roleName] = groupPlan;
    }
}

void TransferAppPlanUtil::transferGroupStatusesToSlotInfos(const std::vector<carbon::GroupStatus> &groupStatuses,
        std::map<std::string, master_base::SlotInfos> &roleSlots){
    for (auto &groupStatus : groupStatuses) {
        const std::map<carbon::roleid_t, carbon::RoleStatus> &roleMap = groupStatus.roles;
        if (!roleMap.empty()){
            for (auto &kv : roleMap) {
                const std::string &roleName = kv.first;
                const carbon::RoleStatus &roleStatus = kv.second;
                
                master_base::SlotInfos &slotInfos = roleSlots[roleName];
                for(auto& node :roleStatus.nodes){
                    const hippo::SlotInfo &slotInfo = node.curWorkerNodeStatus.slotInfo;
                    if (slotInfo.slotId.slaveAddress.empty()) {
                        continue;
                    }
                    slotInfos.push_back(slotInfo);
                }
            }
        }
    }
}

void TransferAppPlanUtil::transferMFRolePlanToGroupPlan(const std::string &roleName, 
        const master_base::RolePlan &mfRolePlan, const std::vector<hippo::PackageInfo> &packageInfos,
        carbon::GroupPlan &groupPlan){
    groupPlan.groupId = roleName;
    std::map<carbon::roleid_t, carbon::RolePlan> c2RolePlans;
    carbon::RolePlan c2RolePlan;
    transferMFRolePlanToc2RolePlan(roleName, mfRolePlan, c2RolePlan);
    fillPackageInfos(
        mfRolePlan.packageInfos.empty() ? packageInfos : mfRolePlan.packageInfos,
        c2RolePlan);
    c2RolePlans[roleName] = c2RolePlan;
    groupPlan.rolePlans = c2RolePlans;
}
   
void TransferAppPlanUtil::transferMFRolePlanToc2RolePlan(const std::string &roleName, 
        const master_base::RolePlan &mfRolePlan, carbon::RolePlan &c2RolePlan){
    c2RolePlan.roleId = roleName;
    carbon::GlobalPlan global;
    generateGlobalPlan(mfRolePlan, global);
    c2RolePlan.global = global;
    carbon::VersionedPlan version;
    generateVersionedPlan(mfRolePlan, version);
    c2RolePlan.version = version;
}

void TransferAppPlanUtil::generateGlobalPlan(const master_base::RolePlan &mfRolePlan, 
        carbon::GlobalPlan &global){
    global.count = mfRolePlan.count;
    global.properties = mfRolePlan.properties;
    // 默认行为为下老起新
    if (global.properties.find(GLOBAL_PLAN_PROP_SMOOTH_RECOVER) == global.properties.end()) {
        global.properties[GLOBAL_PLAN_PROP_SMOOTH_RECOVER] = PROPERTY_FALSE_VALUE;
    }
    if (global.properties.find(MF_MODE_FLAG) == global.properties.end()) {
        global.properties[MF_MODE_FLAG] = PROPERTY_TRUE_VALUE;
    } 
}

void TransferAppPlanUtil::generateVersionedPlan(const master_base::RolePlan &mfRolePlan, 
        carbon::VersionedPlan &version){
    carbon::ResourcePlan resourcePlan;
    generateResourcePlan(mfRolePlan, resourcePlan);
    version.resourcePlan = resourcePlan;
    carbon::LaunchPlan launchPlan;
    generateLaunchPlan(mfRolePlan, launchPlan);
    version.launchPlan = launchPlan;
}

void TransferAppPlanUtil::generateResourcePlan(const master_base::RolePlan &mfRolePlan, 
        carbon::ResourcePlan &resourcePlan){
    resourcePlan.resources = mfRolePlan.slotResources;
    resourcePlan.declarations = mfRolePlan.declareResources;
    resourcePlan.allocateMode = mfRolePlan.allocateMode;
    resourcePlan.queue = mfRolePlan.queue;
    resourcePlan.group = mfRolePlan.group;
    resourcePlan.priority = mfRolePlan.priority;
    resourcePlan.containerConfigs  = mfRolePlan.containerConfigs;
    resourcePlan.metaTags = mfRolePlan.metaTags;
}

void TransferAppPlanUtil::generateLaunchPlan(const master_base::RolePlan &mfRolePlan, 
        carbon::LaunchPlan &launchPlan){
    launchPlan.processInfos = mfRolePlan.processInfos;
    launchPlan.dataInfos = mfRolePlan.dataInfos;
    //std::string podDesc;
}

void TransferAppPlanUtil::fillPackageInfos(const std::vector<hippo::PackageInfo> &packageInfos, 
        carbon::RolePlan &rolePlan){
    rolePlan.version.launchPlan.packageInfos = packageInfos;
}

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);
