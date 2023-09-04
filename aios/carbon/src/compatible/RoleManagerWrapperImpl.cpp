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
#include "compatible/RoleManagerWrapperImpl.h"
#include "compatible/TransferUtil.h"
#include "carbon/Log.h"
#include "master/CarbonDriverWrapperImpl.h"
#include "carbon/ErrorDefine.h"

using namespace std;
BEGIN_CARBON_NAMESPACE(compatible);

CARBON_LOG_SETUP(compatible, RoleManagerWrapperImpl);

RoleManagerWrapperImpl::RoleManagerWrapperImpl() {
    _carbonDriverWrapper = new master::CarbonDriverWrapperImpl();
}

RoleManagerWrapperImpl::~RoleManagerWrapperImpl() {
    delete _carbonDriverWrapper;
}

bool RoleManagerWrapperImpl::init(
        const string &appId, const string &hippoZkPath,
        const string &amZkPath, bool isNewStart,
        worker_framework::LeaderElector *leaderElector,
        uint32_t amonitorPort)
{
    bool k8sMode = hippoZkPath.empty();
    return _carbonDriverWrapper->init(appId, hippoZkPath,
            amZkPath, "", leaderElector, isNewStart, amonitorPort, false, k8sMode);
}

bool RoleManagerWrapperImpl::start() {
    return _carbonDriverWrapper->start();
}

bool RoleManagerWrapperImpl::stop() {
    return _carbonDriverWrapper->stop();
}

bool RoleManagerWrapperImpl::setGroups(const map<string, GroupDesc> &groups) {
    GroupPlanMap groupPlanMap;
    for (map<string, GroupDesc>::const_iterator it = groups.begin();
         it != groups.end(); it++)
    {
        GroupPlan groupPlan;
        groupPlan.groupId = it->first;
        TransferUtil::trasferGroupDescToGroupPlan(it->second, &groupPlan);
        groupPlanMap[it->first] = groupPlan;
    }

    ErrorInfo errorInfo;
    _carbonDriverWrapper->updateGroupPlans(groupPlanMap, &errorInfo);
    return errorInfo.errorCode == EC_NONE;
}

void RoleManagerWrapperImpl::getGroupStatuses(
        const std::vector<string> &groupids,
        std::map<std::string, GroupStatusWrapper> &groupStatuses)
{
    vector<GroupStatus> allGroupStatus;
    _carbonDriverWrapper->getGroupStatus(groupids, &allGroupStatus);
    for (size_t i = 0; i < allGroupStatus.size(); i++) {
        const GroupStatus &groupStatus = allGroupStatus[i];
        GroupStatusWrapper &groupStatusWrapper = groupStatuses[groupStatus.groupId];
        TransferUtil::transferGroupStatus(groupStatus, &groupStatusWrapper);
    }
}

void RoleManagerWrapperImpl::getGroupStatuses(
        std::map<std::string, GroupStatusWrapper> &groupStatuses)
{
    return getGroupStatuses(std::vector<groupid_t>(), groupStatuses);
}

::google::protobuf::Service* RoleManagerWrapperImpl::getService() const {
    return _carbonDriverWrapper->getService();
}

::google::protobuf::Service* RoleManagerWrapperImpl::getOpsService() const {
    return _carbonDriverWrapper->getOpsService();
}

END_CARBON_NAMESPACE(compatible);
