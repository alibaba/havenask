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
#include "master/GroupPlanManager.h"

namespace carbon {
CarbonDriverWrapper* CarbonDriverWrapper::createCarbonDriverWrapper() {
    return new carbon::master::CarbonDriverWrapperImpl;
}
}

using namespace std;
USE_CARBON_NAMESPACE(service);
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, CarbonDriverWrapperImpl);

CarbonDriverWrapperImpl::CarbonDriverWrapperImpl() {
    _carbonDriverPtr.reset(new CarbonDriver());
    _carbonMasterService = new CarbonMasterServiceImpl(_carbonDriverPtr);
    _carbonOpsService = new CarbonOpsServiceImpl(_carbonDriverPtr);
}

CarbonDriverWrapperImpl::~CarbonDriverWrapperImpl() {
    delete _carbonMasterService;
    _carbonMasterService = NULL;
    delete _carbonOpsService;
    _carbonOpsService = NULL;
    _carbonDriverPtr.reset();
}

bool CarbonDriverWrapperImpl::init(const string &applicationId,
                                   const string &hippoZk,
                                   const string &carbonZk,
                                   const string &backupRoot,
                                   worker_framework::LeaderChecker *leaderChecker,
                                   bool isNewStart,
                                   uint32_t amonitorPort, bool withBuffer, bool k8sMode)
{
    return _carbonDriverPtr->init(applicationId, hippoZk,
                                  carbonZk, backupRoot,
                                  leaderChecker, isNewStart,
                                  amonitorPort, withBuffer, k8sMode);
}

bool CarbonDriverWrapperImpl::start() {
    return _carbonDriverPtr->start();
}

bool CarbonDriverWrapperImpl::stop() {
    _carbonDriverPtr->stop();
    return true;
}

void CarbonDriverWrapperImpl::updateGroupPlans(const GroupPlanMap &groupPlans,
        ErrorInfo *errorInfo)
{
    RouterPtr routerPtr = _carbonDriverPtr->getRouter();
    if (routerPtr != NULL) {
        routerPtr->setGroups(groupPlans, errorInfo);
    }
}

void CarbonDriverWrapperImpl::getGroupStatus(
        const vector<string> &groupids,
        vector<GroupStatus> *allGroupStatus) const
{
    RouterPtr routerPtr = _carbonDriverPtr->getRouter();
    if (routerPtr != NULL) {
        ErrorInfo errorInfo;
        routerPtr->getGroupStatusList(groupids, allGroupStatus, &errorInfo);
    }
}

void CarbonDriverWrapperImpl::getAllGroupStatus(vector<GroupStatus> *allGroupStatus) const {
    return getGroupStatus(vector<string>(), allGroupStatus);
}

::google::protobuf::Service* CarbonDriverWrapperImpl::getService() const {
    return _carbonMasterService;
}

::google::protobuf::Service* CarbonDriverWrapperImpl::getOpsService() const {
    return _carbonOpsService;
}

bool CarbonDriverWrapperImpl::releaseSlots(
        const vector<SlotId> &slotIds, uint64_t leaseMs, ErrorInfo *errorInfo) 
{
    GroupManagerPtr groupManagerPtr = _carbonDriverPtr->getGroupManager();
    if (groupManagerPtr == nullptr) {
        errorInfo->errorCode = EC_RELEASE_SLOTS_FAILED;
        errorInfo->errorMsg = "group manager not inited";
        return false;
    }

    vector<ReleaseSlotInfo> releaseSlotInfos;
    if (!groupManagerPtr->genReleaseSlotInfos(slotIds, leaseMs, &releaseSlotInfos)) {
        errorInfo->errorCode = EC_RELEASE_SLOTS_FAILED;
        errorInfo->errorMsg = "gen release slot infos failed";
        return false;
    }

    return groupManagerPtr->releaseSlots(releaseSlotInfos, errorInfo);
}

END_CARBON_NAMESPACE(master);
