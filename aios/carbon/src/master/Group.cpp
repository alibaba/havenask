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
#include "master/Group.h"
#include "carbon/Log.h"
#include "common/Recorder.h"
#include "master/Util.h"
#include "master/GroupScheduler.h"
#include "master/SignatureGenerator.h"
#include "common/JsonUtil.h"
#include "monitor/MonitorUtil.h"
#include "master/GroupVersionGenerator.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace carbon::common;

BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, Group);

Group::Group(const groupid_t &groupId,
             const SerializerPtr &serializerPtr,
             const HippoAdapterPtr &hippoAdapterPtr,
             const HealthCheckerManagerPtr &healthCheckerManagerPtr,
             const ServiceSwitchManagerPtr &serviceSwitchManagerPtr)
    : _hasPlan(false)
    , _groupId(groupId)
{
    _serializerPtr = serializerPtr;
    _hippoAdapterPtr = hippoAdapterPtr;
    _healthCheckerManagerPtr = healthCheckerManagerPtr;
    _serviceSwitchManagerPtr = serviceSwitchManagerPtr;
    _roleCreatorPtr.reset(new RoleCreator());
    _bufferForLast.reset(new rapidjson::StringBuffer(0, BUFFER_DEFAULT_CAPCITY));
    _bufferForCur.reset(new rapidjson::StringBuffer(0, BUFFER_DEFAULT_CAPCITY));
}

Group::~Group() {
}

Group::Group(const Group &group) {
    *this = group;
}

Group& Group::operator = (const Group& rhs) {
    if (this == &rhs) {
        return *this;
    }

    // _groupId = _groupId; # 赋值时 _groupId 不要改
    _targetPlan = rhs._targetPlan;
    // copy roles
    _curRoles.clear();
    RoleMap::const_iterator it = rhs._curRoles.begin();
    for (; it != rhs._curRoles.end(); it++) {
        RolePtr rolePtr(new Role(*it->second));
        _curRoles[it->first] = rolePtr;
    }

    _roleCreatorPtr = rhs._roleCreatorPtr;
    _serializerPtr = rhs._serializerPtr;
    _hippoAdapterPtr = rhs._hippoAdapterPtr;
    _healthCheckerManagerPtr = rhs._healthCheckerManagerPtr;
    _serviceSwitchManagerPtr = rhs._serviceSwitchManagerPtr;
    _bufferForLast = rhs._bufferForLast;
    _bufferForCur = rhs._bufferForCur;
    return *this;
}

void Group::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("groupId", _groupId, _groupId);
    json.Jsonize("targetPlan", _targetPlan, _targetPlan);
    json.Jsonize("curRoles", _curRoles, _curRoles);
    json.Jsonize("curVersion", _curVersion, _curVersion);
}

void Group::setPlan(const version_t &version, const GroupPlan &groupPlan) {
    recordPlan(groupPlan);
    _targetPlan = groupPlan;
    _curVersion = version;
    _hasPlan = true;
}

void Group::stop() {
    _targetPlan.minHealthCapacity = 0;
    map<roleid_t, RolePlan>::iterator it = _targetPlan.rolePlans.begin();
    for (; it != _targetPlan.rolePlans.end(); it++) {
        it->second.global.count = 0;
    }
}

bool Group::isStopped() const {
    for (RoleMap::const_iterator it = _curRoles.begin();
         it != _curRoles.end(); ++it)
    {
        if (!it->second->isStopped()) {
            return false;
        }
    }
    return true;
}

int32_t Group::getGroupAvailablePercent() {
    map<roleid_t, PercentMap> roleVerAvlPercentMap;
    PercentMap sumPercentMap;
    for (auto roleKV : _curRoles) {
        roleKV.second->getVerAvailablePercent(&roleVerAvlPercentMap[roleKV.first]);
        int32_t roleAvailable = 0;
        for (auto percentKV : roleVerAvlPercentMap[roleKV.first]) {
            roleAvailable += percentKV.second;
            if (sumPercentMap.find(percentKV.first) == sumPercentMap.end()) {
                sumPercentMap[percentKV.first] = percentKV.second;
            } else {
                if (percentKV.second < sumPercentMap[percentKV.first]) {
                    sumPercentMap[percentKV.first] = percentKV.second;
                }
            }
        }
        REPORT_METRIC(_groupId + "." + roleKV.first,
                      METRIC_ROLE_AVAILABLE_PERCENT,
                      (double)roleAvailable);
    }
    int32_t groupAvailable = 0;
    for (auto percentKV : sumPercentMap) {
        groupAvailable += percentKV.second;
    }
    return groupAvailable;
}

void Group::schedule() {
    REPORT_USED_TIME(_groupId, METRIC_GROUP_SCHEDULE_TIME);
    Group backupGroup(*this);

    updateRoles();
    REPORT_METRIC(_groupId, METRIC_GROUP_AVAILABLE_PERCENT,
                  (double)getGroupAvailablePercent());
    scheduleRoles();
    removeStoppedRoles();

    if (persist(backupGroup)) {
        execute();
    }
}

void Group::execute() {
    RoleMap::iterator it = _curRoles.begin();
    for (; it != _curRoles.end(); it++) {
        it->second->execute();
    }
}

void Group::getResourceTags(set<tag_t> *tags) const {
    RoleMap::const_iterator it = _curRoles.begin();
    for (; it != _curRoles.end(); it++) {
        it->second->getResourceTags(tags);
    }
}

void Group::updateRoles() {
    const map<roleid_t, RolePlan> &rolePlans = _targetPlan.rolePlans;
    map<roleid_t, RolePlan>::const_iterator rolePlanIt = rolePlans.begin();
    for (rolePlanIt = rolePlans.begin(); rolePlanIt != rolePlans.end();
         rolePlanIt++)
    {
        const roleid_t &roleId = rolePlanIt->first;
        RolePtr rolePtr;
        map<roleid_t, RolePtr>::iterator roleIt = _curRoles.find(roleId);
        if (roleIt == _curRoles.end()) {
            rolePtr = createRole(roleId);
            _curRoles[roleId] = rolePtr;
        } else {
            rolePtr = roleIt->second;
        }
        rolePtr->setPlan(_curVersion, rolePlanIt->second);
    }

    for (map<roleid_t, RolePtr>::iterator it = _curRoles.begin();
         it != _curRoles.end(); it++)
    {
        it->second->update();

        if (rolePlans.empty()) {
            it->second->stop();
            continue;
        }

        const roleid_t &roleId = it->first;
        if (rolePlans.find(roleId) == rolePlans.end()) {
            it->second->setEmptyPlan(_curVersion);
        }
    }
}

void Group::scheduleRoles() {
    GroupSchedulerPtr groupSchedulerPtr =
        _groupSchedulerCreator.createGroupScheduler(
                _targetPlan.schedulerConfig);

    map<roleid_t, ScheduleParams> paramsMap;
    groupSchedulerPtr->generateScheduleParams(_curVersion,
            _targetPlan, _curRoles, &paramsMap);

    map<string, RolePtr>::iterator it =  _curRoles.begin();
    for (; it != _curRoles.end(); it++) {
        it->second->schedule(paramsMap[it->first]);
    }

    removeUselessVersions();
}

void Group::removeUselessVersions() {
    map<version_t, int32_t> verCountMap;
    for (RoleMap::iterator it = _curRoles.begin();
         it != _curRoles.end(); it++)
    {
        map<version_t, int32_t> roleVerCountMap =
            it->second->getOldVerCountMap();

        for (map<version_t, int32_t>::iterator vit = roleVerCountMap.begin();
             vit != roleVerCountMap.end(); vit++)
        {
            verCountMap[vit->first] += vit->second;
        }
    }

    CARBON_LOG(DEBUG, "version count map:[%s]",
               ToJsonString(verCountMap, true).c_str());

    for (map<version_t, int32_t>::iterator it = verCountMap.begin();
         it != verCountMap.end(); it++)
    {
        const version_t &version = it->first;
        if (it->second == 0) {
            CARBON_LOG(INFO, "all roles of version [%s] are stopped,"
                       "remove the version.", version.c_str());
            for (RoleMap::iterator roleIt = _curRoles.begin();
                 roleIt != _curRoles.end(); roleIt++)
            {
                roleIt->second->removeVersionPlan(version);
            }
        }
    }
}

void Group::removeStoppedRoles() {
    const map<roleid_t, RolePlan> &rolePlans = _targetPlan.rolePlans;
    for (map<roleid_t, RolePtr>::iterator it = _curRoles.begin();
         it != _curRoles.end();)
    {
        if (it->second->isStopped() &&
            rolePlans.find(it->first) == rolePlans.end()) {
            CARBON_LOG(DEBUG, "erase role [%s]",
                       it->second->getGUID().c_str());
            _curRoles.erase(it++);
        } else {
            it++;
        }
    }
}

bool Group::persist(const Group &backupGroup) {
    if (doPersist()) {
        return true;
    }

    REPORT_METRIC("", METRIC_GROUP_PERSIST_FAILED_TIMES, 1);
    *this = backupGroup;
    return false;
}

void Group::recordPlan(const GroupPlan &groupPlan) {
    string oriPlanStr;
    string newPlanStr;
    try {
        oriPlanStr = FastToJsonString(_targetPlan);
        newPlanStr = FastToJsonString(groupPlan);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "jsonize Group Plan failed, groupId:[%s], error:[%s].",
                   _groupId.c_str(), e.what());
        return;
    }
    if (oriPlanStr != newPlanStr) {
        common::Recorder::newTarget(_groupId, newPlanStr);
    }
}

bool Group::doPersist() {
    const char* newJsonCStr;
    try {
        REPORT_USED_TIME(_groupId, METRIC_GROUP_TO_STRING_TIME);
        _bufferForCur->Clear();
        newJsonCStr = FastToJsonString(*this, *_bufferForCur);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "jsonize Group failed, groupId:[%s], error:[%s].",
                   _groupId.c_str(), e.what());
        return false;
    }

    if (strcmp(_bufferForLast->GetString(), newJsonCStr) == 0) {
        return true;
    }
    if (!_serializerPtr->write(newJsonCStr)) {
        CARBON_LOG(ERROR, "write the group plan failed, groupId:%s",
                   _groupId.c_str());
        return false;
    }

    // group diff, update bufferForLast
    _bufferForCur.swap(_bufferForLast);
    return true;
}

void Group::fillGroupStatus(GroupStatus *status) const {
    status->groupId = _groupId;
    for (RoleMap::const_iterator it = _curRoles.begin();
         it != _curRoles.end(); it++)
    {
        const roleid_t &roleId = it->first;
        const RolePtr &rolePtr = it->second;
        RoleStatus &roleStatus = status->roles[roleId];
        rolePtr->fillRoleStatus(&roleStatus);
    }
}

bool Group::recover() {
    CARBON_LOG(INFO, "begin recover group:[%s].", _groupId.c_str());
    assert(_serializerPtr != NULL);;
    string str;
    if (!_serializerPtr->read(str)) {
        CARBON_LOG(ERROR, "recover group failed, "
                   "fail to read group serialize file");
        return false;
    }

    groupid_t expectGroupId = _groupId;
    try {
        FastFromJsonString(*this, str);
    } catch (const ExceptionBase &e) {
        CARBON_LOG(ERROR, "recover group [%s] from json failed. error:[%s].",
                   _groupId.c_str(), e.what());
        return false;
    }

    if (_curVersion == "") {
        GroupVersionGeneratorV0 g;
        _curVersion = g.genGroupPlanVersion(_targetPlan);
    }

    if (_groupId != expectGroupId) {
        CARBON_LOG(ERROR, "recovered groupId [%s] is invalid, "
                   "expected is [%s]", _groupId.c_str(),
                   expectGroupId.c_str());
        return false;
    }

    for (RoleMap::iterator it = _curRoles.begin();
         it != _curRoles.end(); it++)
    {
        RolePtr &rolePtr = it->second;
        if (!rolePtr->init(_hippoAdapterPtr, _healthCheckerManagerPtr,
                           _serviceSwitchManagerPtr, true))
        {
            CARBON_LOG(ERROR, "init role %s failed when recover group %s",
                       it->first.c_str(), _groupId.c_str());
            return false;
        }
        if (!rolePtr->recoverServiceAdapters()) {
            CARBON_LOG(ERROR, "recover role serviceAdapters %s failed "
                       "when recover group %s",
                       it->first.c_str(), _groupId.c_str());
            return false;
        }
    }

    return true;
}

RolePtr Group::createRole(const roleid_t &roleId) {
    RolePtr rolePtr = _roleCreatorPtr->create(roleId, _groupId);
    assert(rolePtr != NULL);
    rolePtr->init(_hippoAdapterPtr, _healthCheckerManagerPtr,
                  _serviceSwitchManagerPtr);
    return rolePtr;
}

void Group::releaseSlot(const ReleaseSlotInfo &releaseSlotInfo) {
    CARBON_LOG(INFO, "Group %s release slot %s", _groupId.c_str(), ToJsonString(releaseSlotInfo).c_str());
    auto iter = _curRoles.find(releaseSlotInfo.carbonUnitId.roleId);
    if (iter == _curRoles.end()) {
        return;
    }
    iter->second->releaseSlot(releaseSlotInfo);
}

END_CARBON_NAMESPACE(master);
