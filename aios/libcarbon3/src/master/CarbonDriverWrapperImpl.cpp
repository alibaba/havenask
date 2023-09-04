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
#include "master/CarbonDriverWrapperImpl.h"
#include "carbon/Log.h"
#include "monitor/MonitorUtil.h"

namespace carbon {
CarbonDriverWrapper* CarbonDriverWrapper::createCarbonDriverWrapper() {
    return new carbon::master::CarbonDriverWrapperImpl;
}
}

using namespace std;
using namespace autil::legacy;
USE_CARBON_NAMESPACE(service);
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, CarbonDriverWrapperImpl);

CarbonDriverWrapperImpl::CarbonDriverWrapperImpl() {
    _carbonDriverPtr.reset(new CarbonDriver());
    _slotWorkerNodeManagerPtr.reset(new SlotWorkerNodeManager());
    _carbonMasterService = new CarbonMasterServiceImpl(_carbonDriverPtr);
    _carbonOpsService = new CarbonOpsServiceImpl(_carbonDriverPtr);
}

CarbonDriverWrapperImpl::~CarbonDriverWrapperImpl() {
    delete _carbonMasterService;
    _carbonMasterService = NULL;
     delete _carbonOpsService;
    _carbonOpsService = NULL;
    _carbonDriverPtr.reset();
    _driverCachePtr.reset();
    _slotWorkerNodeManagerPtr.reset();
}

bool CarbonDriverWrapperImpl::init(const string &applicationId,
                                   const string &hippoZk,
                                   const string &carbonZk,
                                   const string &backupRoot,
                                   worker_framework::LeaderChecker *leaderChecker,
                                   bool isNewStart,
                                   uint32_t amonitorPort, bool withBuffer)
{
    return init(applicationId, hippoZk, carbonZk, backupRoot, leaderChecker);
}

bool CarbonDriverWrapperImpl::init(const std::string &applicationId,
                                   const std::string &hippoZk,
                                   const std::string &carbonZk,
                                   const std::string &backupRoot,
                                   worker_framework::LeaderElector *leaderElector,
                                   const std::string &proxySpec, const std::string &kmonSpec,
                                   const DriverOptions& opts)
{
    if (!_carbonDriverPtr->init(applicationId, hippoZk, carbonZk, backupRoot,
                                  leaderElector, proxySpec, kmonSpec, opts)) {
        return false;
    }
    _driverCachePtr.reset(new CarbonDriverCache(_carbonDriverPtr->getProxy()));
    return true;
}

bool CarbonDriverWrapperImpl::start() {
    CARBON_LOG(INFO, "carbon driver wrapper start");
    return _carbonDriverPtr->start() && _driverCachePtr->start();
}

bool CarbonDriverWrapperImpl::stop() {
    if (_driverCachePtr) {
        _driverCachePtr->stop();
        _driverCachePtr.reset();
    }
    _carbonDriverPtr->stop();
    CARBON_LOG(INFO, "carbon driver wrapper stoped");
    return true;
}

void CarbonDriverWrapperImpl::updateGroupPlans(const GroupPlanMap &groupPlans,
        ErrorInfo *errorInfo)
{
    assert(_driverCachePtr);
    _driverCachePtr->updateGroupPlans(groupPlans, errorInfo);
}

void CarbonDriverWrapperImpl::getGroupStatus(
        const vector<string> &groupids,
        vector<GroupStatus> *allGroupStatus) const
{
    assert(_driverCachePtr);
    _driverCachePtr->getGroupStatus(groupids, allGroupStatus);
    _slotWorkerNodeManagerPtr->storeGroupStatusList(*allGroupStatus, groupids.empty());
}

void CarbonDriverWrapperImpl::getAllGroupStatus(vector<GroupStatus> *allGroupStatus) const {
    return getGroupStatus(vector<string>(), allGroupStatus);
}

bool CarbonDriverWrapperImpl::releaseSlots(
        const std::vector<SlotId> &slotIds, 
        uint64_t leaseMs, ErrorInfo *errorInfo) const {
    CARBON_LOG(INFO, "releaseSlots: %s", ToJsonString(slotIds).c_str()); 
    auto workerNodeStatusList = _slotWorkerNodeManagerPtr->retrieveWorkerNode(slotIds);
    std::vector<ReclaimWorkerNode> toReclaimWorkerNodeList;
    for (uint32_t i = 0; i < workerNodeStatusList.size(); ++i) {
        ReclaimWorkerNode workerNode(workerNodeStatusList[i].workerNodeId, workerNodeStatusList[i].slotInfo.slotId);
        toReclaimWorkerNodeList.push_back(workerNode);
    }
    return reclaimWorkerNodes(toReclaimWorkerNodeList, leaseMs, errorInfo);
}

bool CarbonDriverWrapperImpl::releaseIps(
        const std::vector<std::string> &ips, 
        uint64_t leaseMs, ErrorInfo *errorInfo) const {
    CARBON_LOG(INFO, "releaseIps: %s, leaseMs: %lu", ToJsonString(ips).c_str(), leaseMs);
    std::vector<ReclaimWorkerNode> toReclaimWorkerNodeList;
    for (auto &ip: ips) {
        auto workerNodeStatusList = _slotWorkerNodeManagerPtr->retrieveWorkerNodes(ip);
        for (uint32_t i = 0; i < workerNodeStatusList.size(); ++i) {
            ReclaimWorkerNode workerNode(workerNodeStatusList[i].workerNodeId, workerNodeStatusList[i].slotInfo.slotId);
            toReclaimWorkerNodeList.push_back(workerNode);
        }
    }
    return reclaimWorkerNodes(toReclaimWorkerNodeList, leaseMs, errorInfo);
}

bool CarbonDriverWrapperImpl::reclaimWorkerNodes(
    const vector<ReclaimWorkerNode> &workerNodes, 
    uint64_t leaseMs, ErrorInfo *errorInfo) const {
    CARBON_LOG(INFO, "releaseWorkerNodes: %s, leaseMs: %lu", ToJsonString(workerNodes).c_str(), leaseMs);
    if (workerNodes.empty()) {
        return true;
    }
    carbon::monitor::TagMap tags {{"service", "releaseWorkerNodes"}};
    REPORT_USED_TIME(METRIC_SERVICE_LATENCY, tags);
    ProxyClientPtr proxyPtr = _carbonDriverPtr->getProxy();
    if (proxyPtr != NULL) {
        if (leaseMs == 0) {
            return proxyPtr->reclaimWorkerNodes(workerNodes, NULL, errorInfo);
        }
        ProxyWorkerNodePreference pref(HIPPO_PREFERENCE_SCOPE_APP, HIPPO_PREFERENCE_TYPE_PROHIBIT, leaseMs / 1000);
        return proxyPtr->reclaimWorkerNodes(workerNodes, &pref, errorInfo);
    }
    return false;
}

::google::protobuf::Service* CarbonDriverWrapperImpl::getService() const {
    return _carbonMasterService;
}

::google::protobuf::Service* CarbonDriverWrapperImpl::getOpsService() const {
    return _carbonOpsService;
}

END_CARBON_NAMESPACE(master);
