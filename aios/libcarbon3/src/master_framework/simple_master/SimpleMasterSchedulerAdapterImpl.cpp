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
#include "common/Log.h"
#include "simple_master/SimpleMasterSchedulerAdapterImpl.h"
#include "master/SerializerCreator.h"
#include "carbon/CarbonDriverWrapper.h"
#include "carbon/Status.h"
#include "carbon/ErrorDefine.h"
#include "TransferAppPlanUtil.h"
#include "common/PathUtil.h"
#include "autil/StringUtil.h"
#include "fslib/fslib.h"

using namespace std;
using namespace hippo;
using namespace autil;

USE_CARBON_NAMESPACE(common);

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

MASTER_FRAMEWORK_LOG_SETUP(simple_master, SimpleMasterSchedulerAdapter);

SimpleMasterSchedulerAdapter* SimpleMasterSchedulerAdapter::createSimpleMasterSchedulerAdapter() {
    return new master_framework::simple_master::SimpleMasterSchedulerAdapterImpl;
}

SimpleMasterSchedulerAdapterImpl::SimpleMasterSchedulerAdapterImpl() {
    _simpleMasterSchedulerOriginPtr.reset(new SimpleMasterSchedulerOrigin());
    _serializerCreatorPtr.reset();
    _routerPtr.reset();
    _carbonDriverWrapper.reset(new carbon::master::CarbonDriverWrapperImpl());
}

SimpleMasterSchedulerAdapterImpl::~SimpleMasterSchedulerAdapterImpl() {
    stop();
    _simpleMasterSchedulerOriginPtr.reset();
    _serializerCreatorPtr.reset();
    _routerPtr.reset();
    _carbonDriverWrapper.reset();
}

bool SimpleMasterSchedulerAdapterImpl::init(
        const std::string &hippoZkRoot,
        worker_framework::LeaderChecker *leaderChecker,
        const std::string &applicationId)
{
    MF_LOG(INFO, "init master_framework scheduler, applicationId:[%s], hippoZk:[%s]", 
        applicationId.c_str(), hippoZkRoot.c_str());

    string carbonZk = wrapCarbonZk(leaderChecker);
    carbon::DriverOptions ops;
    ops.schType = carbon::DriverOptions::SCH_NODE;
    if (!_carbonDriverWrapper->init(applicationId, hippoZkRoot, carbonZk, "", leaderChecker, "", "", ops)){
        MF_LOG(ERROR, "carbonDriverWrapper init failed");
        return false;
    }
    if (!_simpleMasterSchedulerOriginPtr->init(hippoZkRoot, leaderChecker, applicationId)) {
        MF_LOG(ERROR, "simpleMasterSchedulerOrigin init failed");
        return false;
    }
    _routerPtr.reset(new Router(applicationId, _carbonDriverWrapper->getGlobalConfigManager()));
    return true;
}

// There's no way to get the zookeeper host from LeaderChecker, and we should not write to hippoZk either.
// So here's the tricky way to format a fake carbon zookeeper address to cheat hippo/src/util/PathUtil.cpp this
// is a valid zookeeper address, besides it read/write to zookeeper only by a created ZkWrapper, the zookeeper host
// here is not used.
string SimpleMasterSchedulerAdapterImpl::wrapCarbonZk(worker_framework::LeaderChecker *leaderChecker) {
    // strip `LeaderElection/leader_election0000000000`
    string adminRootPath = PathUtil::getParentDir(PathUtil::getParentDir(leaderChecker->getLeaderInfoPath()));
    return "zfs://mf-fake" + adminRootPath;
}

bool SimpleMasterSchedulerAdapterImpl::start() {
    // NOTE: remove this when we can register rpc service.
    if (!_carbonDriverWrapper->getGlobalConfigManager()->bgSyncFromZK()) {
        MF_LOG(ERROR, "GlobalConfigManager start bgSyncFromZK failed");
        return false;
    }
    if (!_simpleMasterSchedulerOriginPtr->start()) {    
        MF_LOG(ERROR, "simpleMasterSchedulerOrigin start failed");
        return false;
    }
    if (!_carbonDriverWrapper->start()) {
        MF_LOG(ERROR, "carbonDriverWrapper start failed");
    }
    return true;
}

bool SimpleMasterSchedulerAdapterImpl::stop() {
    if (_simpleMasterSchedulerOriginPtr != NULL) {
        _simpleMasterSchedulerOriginPtr->stop();
    } 
    if (_carbonDriverWrapper != NULL) {
        _carbonDriverWrapper->stop();
    }
    return true;
}

bool SimpleMasterSchedulerAdapterImpl::isLeader() {
    return _leaderChecker->isLeader();
}

SlotInfos SimpleMasterSchedulerAdapterImpl::getRoleSlots(const string &roleName) {
    master_base::SlotInfos slotInfos = _simpleMasterSchedulerOriginPtr->getRoleSlots(roleName);
    
    std::vector<carbon::GroupStatus> allGroupStatus;
    std::vector<carbon::groupid_t> groupids;
    groupids.push_back(roleName);
    _carbonDriverWrapper->getGroupStatus(groupids,
            &allGroupStatus);
    if (!allGroupStatus.empty()) {
        map<string, master_base::SlotInfos> c2RoleSlots;
        TransferAppPlanUtil::transferGroupStatusesToSlotInfos(allGroupStatus, c2RoleSlots);
        if (c2RoleSlots.find(roleName) != c2RoleSlots.end()){
            mergeSlotInfos(c2RoleSlots[roleName], slotInfos);
        }
    }
    return slotInfos;
}

void SimpleMasterSchedulerAdapterImpl::getAllRoleSlots(
        map<string, SlotInfos> &roleSlots)
{
    _simpleMasterSchedulerOriginPtr->getAllRoleSlots(roleSlots);
    
    std::vector<carbon::GroupStatus> allGroupStatus;
    _carbonDriverWrapper->getAllGroupStatus(&allGroupStatus);
    if (!allGroupStatus.empty()) {
        map<string, master_base::SlotInfos> c2RoleSlots;
        TransferAppPlanUtil::transferGroupStatusesToSlotInfos(allGroupStatus, c2RoleSlots);
        if (!c2RoleSlots.empty()){
            mergeRoleInfos(c2RoleSlots, roleSlots);
        }
    }
}

void SimpleMasterSchedulerAdapterImpl::setAppPlan(const AppPlan &appPlan) {
    AppPlan c2AppPlan;
    AppPlan mfAppPlan;
    _routerPtr->splitAppPlans(appPlan, c2AppPlan, mfAppPlan);

    _simpleMasterSchedulerOriginPtr->setAppPlan(mfAppPlan);
    
    carbon::GroupPlanMap groupPlans;
    TransferAppPlanUtil::transferAppPlanToGroupPlans(c2AppPlan, groupPlans);

    carbon::ErrorInfo errorInfo;
    _carbonDriverWrapper->updateGroupPlans(groupPlans, &errorInfo);
    if (errorInfo.errorCode != carbon::EC_NONE) {
        MF_LOG(ERROR, "call updateGroupPlans error, errorCode:[%d], errorMsg:[%s]",
            errorInfo.errorCode, errorInfo.errorMsg.c_str());
        return;
    }

    //releaseSlot
    if (!c2AppPlan.prohibitedIps.empty()){
        carbon::ErrorInfo errorInfo;
        _carbonDriverWrapper->releaseIps(c2AppPlan.prohibitedIps, RECLAIM_WORKERNODE_TTL, &errorInfo);
        if (errorInfo.errorCode != carbon::EC_NONE) {
            MF_LOG(ERROR, "call releaseIps error, errorCode:[%d], errorMsg:[%s]",
                errorInfo.errorCode, errorInfo.errorMsg.c_str());
        }
    }
}

void SimpleMasterSchedulerAdapterImpl::releaseSlots(const vector<hippo::SlotId> &slots,
        const hippo::ReleasePreference &pref)
{
    _simpleMasterSchedulerOriginPtr->releaseSlots(slots, pref);
    carbon::ErrorInfo errorInfo;
    _carbonDriverWrapper->releaseSlots(slots, RECLAIM_WORKERNODE_TTL, &errorInfo);
    if (errorInfo.errorCode != carbon::EC_NONE) {
        MF_LOG(ERROR, "call releaseSlots error, errorCode:[%d], errorMsg:[%s]",
                errorInfo.errorCode, errorInfo.errorMsg.c_str());
    }
}

hippo::LeaderSerializer* SimpleMasterSchedulerAdapterImpl::createLeaderSerializer(
        const std::string &zookeeperRoot, const std::string &fileName,
        const std::string &backupRoot)
{
    return _simpleMasterSchedulerOriginPtr->createLeaderSerializer(zookeeperRoot,
            fileName, backupRoot);
}

hippo::MasterDriver* SimpleMasterSchedulerAdapterImpl::getMasterDriver() {
    return _simpleMasterSchedulerOriginPtr->getMasterDriver();
}

void SimpleMasterSchedulerAdapterImpl::setMasterDriver(hippo::MasterDriver* masterDriver) {
    _simpleMasterSchedulerOriginPtr->setMasterDriver(masterDriver);
}

void SimpleMasterSchedulerAdapterImpl::mergeRoleInfos(const map<string, master_base::SlotInfos> &roleSlots, map<string, master_base::SlotInfos> &targetRoleSlots){
    for (auto &kv : roleSlots) {
        if (kv.second.empty()) {
            continue;
        }
        mergeSlotInfos(kv.second, targetRoleSlots[kv.first]);
    }
}

void SimpleMasterSchedulerAdapterImpl::mergeSlotInfos(const master_base::SlotInfos &SlotInfos, master_base::SlotInfos &targetSlotInfos){
    for (auto &slotInfo : SlotInfos) {
        targetSlotInfos.push_back(slotInfo);
    }
}

::google::protobuf::Service* SimpleMasterSchedulerAdapterImpl::getService() {
    if (_carbonDriverWrapper != NULL) {
        return _carbonDriverWrapper->getService();
    }
    return NULL;
}

::google::protobuf::Service* SimpleMasterSchedulerAdapterImpl::getOpsService() {
    if (_carbonDriverWrapper != NULL) {
        return _carbonDriverWrapper->getOpsService();
    }
    return NULL;
}

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

