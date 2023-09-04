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
#include "master/Router.h"
#include "master/GroupManager.h"
#include "master/GroupPlanManager.h"
#include "master/ProxyClient.h"
#include "master/SysConfigManager.h"
#include "master/ProxyClient.h"
#include "autil/StringUtil.h"
#include <regex>
#include <math.h>

BEGIN_CARBON_NAMESPACE(master);

using namespace autil;
using namespace autil::legacy;
CARBON_LOG_SETUP(master, Router);

#define K_TOTAL_COUNT "_repcount_"

Router::Router(const std::string& app, const RouterBaseConf& conf, GroupPlanManagerPtr planManagerPtr, 
        GroupManagerPtr groupManagerPtr, SysConfigManagerPtr sysConfigPtr) :
    _groupPlanManagerPtr(planManagerPtr), _groupManagerPtr(groupManagerPtr), _sysConfigPtr(sysConfigPtr) {
    _proxy.reset(new ProxyClient(conf.host, app, conf.single, conf.useJsonEncode));
}

version_t Router::addGroup(const GroupPlan &plan, ErrorInfo *errorInfo) {
    return doRoute(&GroupPlanManager::addGroup, &ProxyClient::addGroup, plan, errorInfo);
}

version_t Router::updateGroup(const GroupPlan &plan, ErrorInfo *errorInfo) {
    return doRoute(&GroupPlanManager::updateGroup, &ProxyClient::updateGroup, plan, errorInfo);
}

void Router::delGroup(const groupid_t &groupId, ErrorInfo *errorInfo) {
    const RouterConfig& conf = _sysConfigPtr->getConfig().router;
    if (getRatio(groupId, conf, true) >= 0) {
        _groupPlanManagerPtr->delGroup(groupId, errorInfo);
    }
    if (getRatio(groupId, conf, false) >= 0) {
        _proxy->delGroup(groupId, errorInfo);
    }
}

void Router::syncGroups(const SyncOptions& opts, ErrorInfo* errorInfo, std::string* data) {
    ScopedLock lock(_mutex);
    const RouterConfig& conf = _sysConfigPtr->getConfig().router;
    if (conf.routeAll) {
        return;
    }
    auto versionedPlanMap = _groupPlanManagerPtr->getGroupPlans();
    GroupPlanMap groupPlanMap;
    for (auto& kv : versionedPlanMap) {
        groupPlanMap[kv.first] = patchGroupPlan(opts, kv.second.plan);
    }
    if (opts.dryRun && data != NULL) {
        *data = ToJsonString(groupPlanMap, true);
    }
    if (!opts.dryRun) {
        doSetGroups(groupPlanMap, errorInfo);
    }
}

void Router::setGroups(const GroupPlanMap& groupPlanMap, ErrorInfo *errorInfo) {
    ScopedLock lock(_mutex);
    doSetGroups(groupPlanMap, errorInfo);
}


void Router::doSetGroups(const GroupPlanMap& groupPlanMap, ErrorInfo *errorInfo) {
    const RouterConfig& conf = _sysConfigPtr->getConfig().router;
    GroupPlanMap tmpGroupPlans(groupPlanMap);
    ErrorCode proxyErrorCode = EC_NONE;
    if (rewriteMapCounts(tmpGroupPlans, groupPlanMap, conf, false)) {
        if (!_proxy->setGroups(tmpGroupPlans, errorInfo)) {
            CARBON_LOG(ERROR, "call carbon-proxy setGroups failed: %s", ToJsonString(errorInfo).c_str());
            proxyErrorCode = errorInfo->errorCode;
        }
    }
    tmpGroupPlans = groupPlanMap;
    if (rewriteMapCounts(tmpGroupPlans, groupPlanMap, conf, true)) {
        _groupPlanManagerPtr->updateAllGroups(tmpGroupPlans, errorInfo);
        if (errorInfo->errorCode != EC_NONE) {
            return;
        }
    }
    if (proxyErrorCode != EC_NONE) {
        errorInfo->errorCode = proxyErrorCode;
    }
}

GroupPlan& Router::patchGroupPlan(const SyncOptions& opts, GroupPlan& plan) {
    for (auto& kv : plan.rolePlans) {
        auto& rolePlan = kv.second;
        int32_t old = rolePlan.global.count;
        auto& props = rolePlan.global.properties;
        auto it = props.find(K_TOTAL_COUNT);
        if (it != props.end()) {
            rolePlan.global.count = StringUtil::strToInt32WithDefault(it->second.c_str(), rolePlan.global.count);
        } else if (rolePlan.global.count == 0) {
            rolePlan.global.count = opts.fixCount;
        }
        if (old != rolePlan.global.count) {
            CARBON_LOG(INFO, "patch role replica count: %s, %d, %d", rolePlan.roleId.c_str(), old, rolePlan.global.count);
        }
    }
    return plan;
}

void Router::getGroupStatusList(const std::vector<groupid_t>& ids, std::vector<GroupStatus>* statusList, ErrorInfo *errorInfo) {
    std::vector<GroupStatus> proxyStatusList;
    const RouterConfig& conf = _sysConfigPtr->getConfig().router;

    if (isCall(ids, conf, false)) { // to get empty status
        if (!_proxy->getGroupStatusList(ids, proxyStatusList, errorInfo)) {
            // if error happened, return empty status, and its client responsibility to handle empty response.
            CARBON_LOG(WARN, "get group status from proxy failed: %s", ToJsonString(ids, true).c_str());
            return;
        }
    }
    std::vector<GroupStatus> locStatusList;
    if (isCall(ids, conf, true)) {
        _groupManagerPtr->getGroupStatus(ids, &locStatusList);
        statusList->swap(locStatusList);
        mergeStatus(proxyStatusList, statusList);
    } else {
        statusList->swap(proxyStatusList);
    }
}

bool Router::releaseSlots(const std::vector<ReclaimWorkerNode> &toReclaimWorkerNodeList, const uint64_t leaseMs, ErrorInfo* errorInfo){
    CARBON_LOG(INFO, "releaseIps: %s, leaseMs: %lu", ToJsonString(toReclaimWorkerNodeList).c_str(), leaseMs);
    const RouterConfig& conf = _sysConfigPtr->getConfig().router;
    std::vector<groupid_t> ids;
    if (isCall(ids, conf, false)) {
        return reclaimWorkerNodes(toReclaimWorkerNodeList, leaseMs, errorInfo);
    }
    return true;
}

void Router::genReclaimWorkerNodeList(const std::vector<SlotId> &slotIds,
        std::vector<ReclaimWorkerNode> *reclaimWorkerNodeList){
    for (auto &slotId: slotIds) {
        ReclaimWorkerNode workerNode("", slotId);
        reclaimWorkerNodeList->push_back(workerNode);
    }
}

bool Router::reclaimWorkerNodes(
    const std::vector<ReclaimWorkerNode> &workerNodes, 
    uint64_t leaseMs, ErrorInfo *errorInfo) {
    CARBON_LOG(INFO, "releaseWorkerNodes: %s, leaseMs: %lu", ToJsonString(workerNodes).c_str(), leaseMs);
    if (workerNodes.empty()) {
        return true;
    }

    if (leaseMs == 0) {
        return _proxy->reclaimWorkerNodes(workerNodes, NULL, errorInfo);
    }
    ProxyWorkerNodePreference pref(HIPPO_PREFERENCE_SCOPE_APP, HIPPO_PREFERENCE_TYPE_PROHIBIT, leaseMs / 1000);
    return _proxy->reclaimWorkerNodes(workerNodes, &pref, errorInfo);
}

void Router::mergeStatus(const std::vector<GroupStatus>& proxyStatusList, std::vector<GroupStatus>* statusList) {
    std::set<groupid_t> merged;
    auto proxyStatusMap = toMapStatus(proxyStatusList);
    for (auto& status : *statusList) {
        auto pit = proxyStatusMap.find(status.groupId);
        // NOTE: what if status missing from proxy ?
        if (pit == proxyStatusMap.end()) {
            continue;
        }
        const auto& proxyStatus = pit->second;
        mergeStatus(proxyStatus, status);
        merged.insert(status.groupId);
    }
    // append status in proxy, but not in local.
    // NOTE: the status from proxy maybe missing some fields, e.g: VersionPlan 
    for (const auto& kv : proxyStatusMap) {
        if (merged.find(kv.first) == merged.end()) {
            statusList->push_back(kv.second);
        }
    }
}

void Router::mergeStatus(const GroupStatus& src, GroupStatus& dst) {
    for (const auto& kv : src.roles) {
        mergeStatus(kv.second, dst.roles[kv.first]);
    }
}

#define REWRITE_VER(n, f, v0, v1) \
    n.f = (n.f == v0) ? v1 : v0

void Router::mergeStatus(const RoleStatus& src, RoleStatus& dst) {
    auto ver = src.latestVersion;
    auto rewriteVer = dst.latestVersion;
    for (const auto& n : src.nodes) {
        auto it = dst.nodes.insert(dst.nodes.end(), n);
        auto& node = *it;
        REWRITE_VER(node.curWorkerNodeStatus, curVersion, ver, rewriteVer);
        REWRITE_VER(node.curWorkerNodeStatus, nextVersion, ver, rewriteVer);
        REWRITE_VER(node.curWorkerNodeStatus, finalVersion, ver, rewriteVer);
        REWRITE_VER(node.backupWorkerNodeStatus, curVersion, ver, rewriteVer);
        REWRITE_VER(node.backupWorkerNodeStatus, nextVersion, ver, rewriteVer);
        REWRITE_VER(node.backupWorkerNodeStatus, finalVersion, ver, rewriteVer);
    }
    dst.globalPlan.count += src.globalPlan.count;
    dst.readyForCurVersion = dst.readyForCurVersion && src.readyForCurVersion;
}

std::map<groupid_t, GroupStatus> Router::toMapStatus(const std::vector<GroupStatus>& statusList) {
    std::map<groupid_t, GroupStatus> statusMap;
    for (const auto& status : statusList) {
        statusMap[status.groupId] = status;
    }
    return statusMap;
}

#define CALL_MEMBER_FN(object,ptrToMember)  (*(object.get()).*(ptrToMember))

version_t Router::doRoute(GroupPlanManagerFn locFn, ProxyClientFn proxyFn, const GroupPlan &plan, ErrorInfo *errorInfo) {
    version_t ver;
    GroupPlan tmpPlan = plan;
    ErrorCode proxyErrorCode = EC_NONE;
    const RouterConfig& conf = _sysConfigPtr->getConfig().router;
    if (rewriteCounts(tmpPlan, plan, conf, false)) {
        if (!CALL_MEMBER_FN(_proxy, proxyFn)(tmpPlan, errorInfo)) {
            CARBON_LOG(ERROR, "call carbon-proxy failed: %s", ToJsonString(errorInfo).c_str());
            proxyErrorCode = errorInfo->errorCode;
        }
    }
    if (rewriteCounts(tmpPlan, plan, conf, true)) {
        ver = CALL_MEMBER_FN(_groupPlanManagerPtr, locFn)(tmpPlan, errorInfo);
        if (errorInfo->errorCode != EC_NONE) {
            return ver;
        }
    }
    if (proxyErrorCode != EC_NONE) {
        errorInfo->errorCode = proxyErrorCode;
    }
    return ver;
}

bool Router::rewriteMapCounts(GroupPlanMap& planMap, const GroupPlanMap& origPlanMap, const RouterConfig& conf, bool loc) {
    if (!loc && conf.proxyRatio == -1 && conf.starRouterList.empty()){
        return false;
    }
    for (const auto& kv : origPlanMap) {
        if (!rewriteCounts(planMap[kv.first], kv.second, conf, loc)) {
            if (loc) {
                return false;//本地不处理
            } else {
                planMap.erase(kv.first);//删除这个group
            }
        }
    }
    
    return true;
}

// return true indicates rewrite success
bool Router::rewriteCounts(GroupPlan& plan, const GroupPlan& origPlan, const RouterConfig& conf, bool loc) {
    int r = loc ? conf.locRatio : conf.proxyRatio;
    
    bool routed = isRoutedGroup(origPlan.groupId, conf);
    // If routed=false, a). for loc, don't rewrite (ratio=100); b). for proxy, don't rewrite (no request)
    if (!routed) {
        if (!loc) {
            r = -1;
        } else {
            r = 100;
        }
    }

    rewriteRatio(origPlan.groupId, conf, r, loc);

    if (r < 0) return false;

    for (const auto& kv : origPlan.rolePlans) {
        int32_t n = kv.second.global.count;
        int32_t target = (int32_t) ceil(n * r / 100.0);
        plan.rolePlans[kv.first].global.count = target;
        plan.rolePlans[kv.first].global.properties[K_TOTAL_COUNT] = StringUtil::toString(n);
    }
    return true;
}

bool Router::isRoutedGroup(const groupid_t& id, const RouterConfig& conf) {
    if (conf.blackGroups.end() != std::find(conf.blackGroups.begin(), conf.blackGroups.end(), id)) {
        return false;
    }
    if (conf.routeAll) {
        return true;
    }
    return conf.routeGroups.end() != std::find(conf.routeGroups.begin(), conf.routeGroups.end(), id);
}

bool Router::rewriteRatio(const groupid_t& id, const RouterConfig& conf, int& r, bool loc) {
    const std::vector<RouterRatioModel> &starRouterList = conf.starRouterList;
    for (auto& routerRatioModel : starRouterList) {
        const std::string &group = routerRatioModel.group;
        if(isMatchGroup(group, id)){
            r = loc ? routerRatioModel.locRatio : routerRatioModel.proxyRatio;
            return true;
        }
    }
    return false;
}

bool Router::isMatchGroup(const std::string& regex, const std::string& groupId){
    if (regex == "") {
        return false;
    }
    return std::regex_match(groupId, std::regex(regex));
}

int Router::getRatio(const groupid_t& id, const RouterConfig& conf, bool loc){
    int r = loc ? conf.locRatio : conf.proxyRatio;
    rewriteRatio(id, conf, r, loc);
    return r;
}

bool Router::isCall(const std::vector<groupid_t>& ids, const RouterConfig& conf, bool loc){
    if (!loc && conf.proxyRatio == -1 && conf.starRouterList.empty()){
        return false;
    }
    if (loc && conf.routeAll){
        return false;
    }
    if (ids.empty()){
        return true;
    }
    for (auto& id : ids) {
        if (getRatio(id, conf, loc) >= 0){
            return true;
        }
    }
    return false;
}

END_CARBON_NAMESPACE(master);
