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
#include "master/DefaultGroupScheduler.h"
#include "master/HoldMatrix.h"
#include "master/Util.h"
#include "carbon/Log.h"
#include "common/Recorder.h"
#include <cmath>
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, DefaultGroupScheduler);

DefaultGroupScheduler::DefaultGroupScheduler() {
}

DefaultGroupScheduler::~DefaultGroupScheduler() {
}

void DefaultGroupScheduler::init(const string &configStr) {
    //nothing need init
}

void DefaultGroupScheduler::generateScheduleParams(
        const version_t &latestGroupVersion,
        const GroupPlan &targetPlan,
        const RoleMap &curRoles,
        map<roleid_t, ScheduleParams> *scheduleParamsMap) const
{
    map<roleid_t, PercentMap> roleVerAvlPercentMap;
    getRoleVerAvlPercent(curRoles, &roleVerAvlPercentMap);

    HoldMatrix holdMatrix(roleVerAvlPercentMap,
                          latestGroupVersion,
                          targetPlan.minHealthCapacity,
                          Util::calcGroupLatestVersionRatio(targetPlan));

    common::Recorder::logWhenDiff(targetPlan.groupId + ".availAbleMatrix",
                                holdMatrix.getAvailMatrixStr(),
                                _logger);
    map<roleid_t, PercentMap> roleVerHoldPercentMap =
        holdMatrix.getHoldPercent();

    int64_t timeStamp = TimeUtility::currentTime();
    fillScheduleParamsMap(targetPlan, curRoles,
                          roleVerHoldPercentMap, timeStamp,
                          scheduleParamsMap);
}

void DefaultGroupScheduler::getRoleVerAvlPercent(
        const RoleMap &curRoles,
        map<roleid_t, PercentMap> *roleVerAvlPercentMap) const
{
    for (RoleMap::const_iterator it = curRoles.begin();
         it != curRoles.end(); it++)
    {
        it->second->getVerAvailablePercent(&(*roleVerAvlPercentMap)[it->first]);
    }
}

void DefaultGroupScheduler::fillScheduleParamsMap(
        const GroupPlan &targetPlan, const RoleMap &curRoles,
        map<roleid_t, PercentMap> &roleVerHoldPercentMap,
        int64_t timeStamp,
        map<roleid_t, ScheduleParams> *scheduleParamsMap) const
{
    RoleMap::const_iterator it = curRoles.begin();
    for (; it != curRoles.end(); it++) {
        int32_t roleHoldCount = 0;
        int32_t roleTargetCount = it->second->getTargetCount();
        int32_t roleCurrentCount = it->second->getNodeCount();
        const roleid_t &roleId = it->first;
        const PercentMap &verHoldPercentMap = roleVerHoldPercentMap[roleId];
        ScheduleParams &roleScheduleParams = (*scheduleParamsMap)[roleId];
        roleScheduleParams.timeStamp = timeStamp;
        roleScheduleParams.minHealthCount = (int32_t)ceil(
                targetPlan.minHealthCapacity * roleTargetCount / 100.0);
        for (PercentMap::const_iterator pit = verHoldPercentMap.begin();
             pit != verHoldPercentMap.end(); pit++)
        {
            int32_t baseCount = it->second->getBaseCount(pit->first);
            assert(baseCount != -1);
            int32_t holdCount = (int32_t)ceil(pit->second * baseCount / 100.0);
            roleScheduleParams.holdingCountMap[pit->first] = holdCount;
            roleHoldCount += holdCount;
        }
        int32_t expectHoldCount = roleScheduleParams.minHealthCount;
        roleHoldCount = max(roleHoldCount, expectHoldCount);
        int32_t extraCount = (int32_t)ceil(targetPlan.extraRatio
                * roleTargetCount / 100.0);

        if (roleHoldCount < roleTargetCount) {
            roleScheduleParams.maxCount = roleTargetCount;
        } else if (roleHoldCount < roleCurrentCount) {
            roleScheduleParams.maxCount = min(roleCurrentCount, roleHoldCount + extraCount);
        } else {
            assert(roleHoldCount == max(roleCurrentCount, roleTargetCount));
            roleScheduleParams.maxCount = roleHoldCount + extraCount;
            CARBON_LOG(DEBUG, "All nodes of [%s] are held, "
                       "roleHoldCount:%d, extraCount is:%d",
                       it->second->getGUID().c_str(), roleHoldCount, extraCount);
        }
    }
}

END_CARBON_NAMESPACE(master);

