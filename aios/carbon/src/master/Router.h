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
// route group plan and group status to/from carbon-proxy (to k8s controller)
#ifndef CARBON_MASTER_ROUTER_H
#define CARBON_MASTER_ROUTER_H

#include "common/common.h"
#include "master/Flag.h"
#include "master/ProxyClient.h"
#include "carbon/Log.h"
#include "carbon/GroupPlan.h"
#include "carbon/Status.h"
#include "carbon/GlobalConfig.h"

BEGIN_CARBON_NAMESPACE(master);

CARBON_PREDECL_PTR(GroupPlanManager);
CARBON_PREDECL_PTR(GroupManager);
CARBON_PREDECL_PTR(ProxyClient);
CARBON_PREDECL_PTR(SysConfigManager);

class Router
{
    typedef version_t (GroupPlanManager::*GroupPlanManagerFn) (const GroupPlan &plan, ErrorInfo *errorInfo);
    typedef bool (ProxyClient::*ProxyClientFn) (const GroupPlan &plan, ErrorInfo *errorInfo);

public:
    struct SyncOptions {
        SyncOptions(int fixN = 0, bool drun = false) : 
            fixCount(fixN), dryRun(drun) {}
        int fixCount; // if global.count==0, and `properties' missing, fix count by this
        bool dryRun;
    };

public:
    Router(const std::string& app, const RouterBaseConf& conf, GroupPlanManagerPtr planManagerPtr, 
            GroupManagerPtr groupManagerPtr, SysConfigManagerPtr sysConfigPtr);

    version_t addGroup(const GroupPlan &plan, ErrorInfo *errorInfo);
    version_t updateGroup(const GroupPlan &plan, ErrorInfo *errorInfo);
    void delGroup(const groupid_t &groupId, ErrorInfo *errorInfo);
    void setGroups(const GroupPlanMap& groupPlanMap, ErrorInfo *errorInfo);
    void getGroupStatusList(const std::vector<groupid_t>& ids, std::vector<GroupStatus>* statusList, ErrorInfo *errorInfo);
    void syncGroups(const SyncOptions& opts, ErrorInfo* errorInfo, std::string* data);
    bool releaseSlots(const std::vector<ReclaimWorkerNode> &toReclaimWorkerNodeList, 
    const uint64_t leaseMs, ErrorInfo* errorInfo);
    void genReclaimWorkerNodeList(const std::vector<SlotId> &slotIds,
        std::vector<ReclaimWorkerNode> *reclaimWorkerNodeList);
private:
    void doSetGroups(const GroupPlanMap& groupPlanMap, ErrorInfo *errorInfo);
    version_t doRoute(GroupPlanManagerFn locFn, ProxyClientFn proxyFn, const GroupPlan &plan, ErrorInfo *errorInfo);
    bool rewriteCounts(GroupPlan& plan, const GroupPlan& origPlan, const RouterConfig& conf, bool loc);
    bool rewriteMapCounts(GroupPlanMap& planMap, const GroupPlanMap& origPlanMap, const RouterConfig& conf, bool loc);
    void mergeStatus(const std::vector<GroupStatus>& proxyStatusList, std::vector<GroupStatus>* statusList);
    void mergeStatus(const GroupStatus& src, GroupStatus& dst);
    void mergeStatus(const RoleStatus& src, RoleStatus& dst);
    std::map<groupid_t, GroupStatus> toMapStatus(const std::vector<GroupStatus>& statusList);
    bool isRoutedGroup(const groupid_t& id, const RouterConfig& conf);
    GroupPlan& patchGroupPlan(const SyncOptions& opts, GroupPlan& plan);
    bool rewriteRatio(const groupid_t& id, const RouterConfig& conf, int& r, bool loc);
    bool isMatchGroup(const std::string& regex, const std::string& groupId);
    int getRatio(const groupid_t& id, const RouterConfig& conf, bool loc);
    bool isCall(const std::vector<groupid_t>& ids, const RouterConfig& conf, bool loc);
    bool reclaimWorkerNodes(const std::vector<ReclaimWorkerNode> &workerNodes, 
    uint64_t leaseMs, ErrorInfo *errorInfo);


private:
    GroupPlanManagerPtr _groupPlanManagerPtr;
    GroupManagerPtr _groupManagerPtr;
    ProxyClientPtr _proxy;
    SysConfigManagerPtr _sysConfigPtr;
    autil::ThreadMutex _mutex;
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(Router);

END_CARBON_NAMESPACE(master);

#endif
