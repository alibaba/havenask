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
#include "master/GroupManager.h"
#include "common/Recorder.h"
#include "common/TimeTracker.h"
#include "carbon/Log.h"
#include "monitor/CarbonMonitorSingleton.h"
#include "autil/StringUtil.h"
#include "fslib/fslib.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, GroupManager);

#define __ADJUST_SCHEDULE_INTERVAL    "schedule_interval"
#define __ADJUST_HIPPO_ADAPTER_STATUS "hippo_adapter_status"
#define __ADJUST_HIPPO_ADAPTER_STATUS_STOP "stop"
#define __ADJUST_HIPPO_ADAPTER_STATUS_START "start"

GroupManager::GroupManager(const HippoAdapterPtr &hippoAdapterPtr,
                           const GroupPlanManagerPtr &groupPlanManagerPtr,
                           const SerializerCreatorPtr &serializerCreatorPtr,
                           ServiceAdapterExtCreatorPtr creator)
    : _runner(DEFAULT_RUNNER_THREAD_NUM * 6)
{
    _groupPlanManagerPtr = groupPlanManagerPtr;
    _serializerCreatorPtr = serializerCreatorPtr;
    _hippoAdapterPtr = hippoAdapterPtr;
    _healthCheckerManagerPtr.reset(new HealthCheckerManager());
    _serviceSwitchManagerPtr.reset(new ServiceSwitchManager(_serializerCreatorPtr, creator));
    _roleCreatorPtr.reset(new RoleCreator());
}

GroupManager::~GroupManager() {
    stop();
}

bool GroupManager::start() {
    _scheduleLoopThreadPtr = LoopThread::createLoopThread(
            std::bind(&GroupManager::schedule, this),
            GROUP_SCHEDULE_INTERVAL);
    if (_scheduleLoopThreadPtr == NULL) {
        CARBON_LOG(ERROR, "create group schedule loop thread failed.");
        return false;
    }

    if (!_healthCheckerManagerPtr->start()) {
        CARBON_LOG(ERROR, "start health checker manager failed.");
        return false;
    }

    if (!_serviceSwitchManagerPtr->start()) {
        CARBON_LOG(ERROR, "start service switch manager failed.");
        return false;
    }
    
    if (!_runner.start()) {
        CARBON_LOG(ERROR, "start runner for GroupManager.");
        return false;
    }
    return true;
}

void GroupManager::stop() {
    CARBON_LOG(INFO, "stop GroupManager.");
    _scheduleLoopThreadPtr.reset();
    _healthCheckerManagerPtr.reset();
    _serviceSwitchManagerPtr.reset();
}

bool GroupManager::recover() {
    string serializerDir = _serializerCreatorPtr->getSerializerDir();
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(serializerDir);
    if (ec != fslib::EC_TRUE && ec != fslib::EC_FALSE) {
        CARBON_LOG(ERROR, "check group serializer dir exist failed, "
                   "error:[%s].",
                   fslib::fs::FileSystem::getErrorString(ec).c_str());
        return false;
    }

    if (ec == fslib::EC_FALSE) {
        CARBON_LOG(INFO, "group serializer dir not exist, not need recover.");
        return true;
    }

    vector<string> fileList;
    ec = fslib::fs::FileSystem::listDir(serializerDir, fileList);
    if (ec != fslib::EC_OK) {
        CARBON_LOG(ERROR, "list group serializer dir failed, error:[%s].",
                   fslib::fs::FileSystem::getErrorString(ec).c_str());
        return false;
    }

    vector<string> groupIds;
    for (size_t i = 0; i < fileList.size(); i++) {
        const string &fileName = fileList[i];
        if (!StringUtil::startsWith(fileName,
                        GROUP_PLAN_SERIALIZE_FILE_PREFIX))
        {
            continue;
        }
        string groupId = fileName.substr(
                string(GROUP_PLAN_SERIALIZE_FILE_PREFIX).size());
        groupIds.push_back(groupId);
    }

    for (size_t i = 0; i < groupIds.size(); i++) {
        const groupid_t &groupId = groupIds[i];
        GroupPtr groupPtr = createGroup(groupId);
        assert(groupPtr != NULL);
        if (!groupPtr->recover()) {
            CARBON_LOG(ERROR, "recover group failed. groupId:[%s].",
                       groupId.c_str());
            return false;
        }
        _groups[groupId] = groupPtr;
    }

    getAllGroupStatus(&_groupStatusCache);
    return true;
}

bool GroupManager::releaseSlots(const std::vector<ReleaseSlotInfo> &releaseSlotInfos,
                                ErrorInfo *errorInfo)
{
    {
        ScopedReadLock lock(_groupStatusLock);
        vector<ReleaseSlotInfo> notFoundSlot;
        if (!checkSlotInfoExist(releaseSlotInfos, &notFoundSlot)) {
            string errorMsg = "some slot not exist. not found list:" + ToJsonString(notFoundSlot, true);
            errorInfo->errorCode = EC_NODE_NOT_FOUND;
            errorInfo->errorMsg = errorMsg;
            CARBON_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    {
        ScopedLock lock(_releaseSlotLock);
        for (auto releaseSlotInfo : releaseSlotInfos) {
            _toReleaseSlots[releaseSlotInfo.carbonUnitId.hippoSlotId] = releaseSlotInfo;
        }
    }
    return true;
}


bool GroupManager::genReleaseSlotInfos(
        const vector<SlotId> &slotIds,
        const uint64_t leaseMs,
        vector<ReleaseSlotInfo> *releaseSlotInfos)
{
    releaseSlotInfos->clear();
    set<SlotId> slotIdSet(slotIds.begin(), slotIds.end());
    ScopedReadLock lock(_groupStatusLock);
    for (const auto &groupKV : _groupStatusCache) {
        for (const auto &roleKV : groupKV.second.roles) {
            for (const auto &replicaKv : roleKV.second.nodes) {
                const auto &curSlotId =
                    replicaKv.curWorkerNodeStatus.slotInfo.slotId;
                const auto &backupSlotId =
                    replicaKv.backupWorkerNodeStatus.slotInfo.slotId;
                if (slotIdSet.find(curSlotId) != slotIdSet.end()) {
                    releaseSlotInfos->push_back(
                            {{groupKV.first, roleKV.first, curSlotId.id}, leaseMs});
                    CARBON_LOG(DEBUG, "add %s:%s:%d to release set",
                            groupKV.first.c_str(), roleKV.first.c_str(), curSlotId.id);
                }
                if (slotIdSet.find(backupSlotId) != slotIdSet.end()) {
                    releaseSlotInfos->push_back(
                            {{groupKV.first, roleKV.first, backupSlotId.id}, leaseMs});
                    CARBON_LOG(DEBUG, "add %s:%s:%d to release set",
                            groupKV.first.c_str(), roleKV.first.c_str(), curSlotId.id);
                }
            }
        }
    }
    return slotIdSet.size() == releaseSlotInfos->size();
}


bool GroupManager::checkSlotInfoExist(
        const vector<ReleaseSlotInfo> &releaseSlotInfos,
        vector<ReleaseSlotInfo> *notFoundSlot) const
{
    for (const auto &releaseSlotInfo : releaseSlotInfos) {
        if (releaseSlotInfo.carbonUnitId.hippoSlotId == -1) {
            notFoundSlot->push_back(releaseSlotInfo);
            return false;
        }
        const auto groupIter = _groupStatusCache.find(releaseSlotInfo.carbonUnitId.groupId);
        if (groupIter == _groupStatusCache.end()) {
            notFoundSlot->push_back(releaseSlotInfo);
            return false;
        }
        const auto &groupStatus = groupIter->second;
        const auto roleIter = groupStatus.roles.find(releaseSlotInfo.carbonUnitId.roleId);
        if (roleIter == groupStatus.roles.end()) {
            notFoundSlot->push_back(releaseSlotInfo);
            return false;
        }
        const auto &roleStatus = roleIter->second;
        bool slotFound = false;
        for (const auto &nodeStatus : roleStatus.nodes) {
            if (nodeStatus.curWorkerNodeStatus.slotStatus.slotId.id ==
                releaseSlotInfo.carbonUnitId.hippoSlotId)
            {
                slotFound = true;
            }
            if (nodeStatus.backupWorkerNodeStatus.slotStatus.slotId.id ==
                releaseSlotInfo.carbonUnitId.hippoSlotId)
            {
                slotFound = true;
            }
        }
        if (!slotFound) {
            notFoundSlot->push_back(releaseSlotInfo);
            return false;
        }
    }
    return true;
}

bool GroupManager::adjustRunningArgs(const string &field, const string &value) {
#ifndef NDEBUG
    if (field == __ADJUST_SCHEDULE_INTERVAL) {
        return __forTestAdjustScheduleInterval(value);
    } else if (field == __ADJUST_HIPPO_ADAPTER_STATUS) {
        return __forTestAdjustHippoAdapterStatus(value);
    } else {
        CARBON_LOG(ERROR, "invalid argument field:%s, value:%s",
                   field.c_str(), value.c_str());
        return false;
    }
#endif
    return false;
}


void GroupManager::schedule() {
    TIME_TRACKER();
    int64_t bt0 = TimeUtility::currentTime();
    map<groupid_t, GroupStatus> groupStatusCache;
    getAllGroupStatus(&groupStatusCache);
    {
        ScopedWriteLock lock(_groupStatusLock);
        TIME_TRACK0(diffAndRecordGroupStatus(groupStatusCache));
        _groupStatusCache.swap(groupStatusCache);
    }
    if (!_hippoAdapterPtr->isWorking()) {
        CARBON_LOG(ERROR, "hippo adapter is not working, not schedule.");
        return;
    }

    int64_t bt1 = TimeUtility::currentTime();
    
    _hippoAdapterPtr->preUpdate();
    VersionedGroupPlanMap groupPlanMap = _groupPlanManagerPtr->getGroupPlans();
    TIME_TRACK0(updateGroupPlans(groupPlanMap));
    stopDeletedGroups(groupPlanMap);
    executeOpsCmd();
    TIME_TRACK0(scheduleGroups());
    releaseTags();
    _hippoAdapterPtr->postUpdate();

    int64_t endTime = TimeUtility::currentTime();
    double usedTimeSecond = (double)(endTime - bt1) / 1000.0 / 1000.0;
    REPORT_METRIC("", METRIC_GROUP_TOTAL_SCHEDULE_TIME, usedTimeSecond * 1000.0);
    if (usedTimeSecond >= SCHEDULE_TOO_LONG_TIME) {
        CARBON_LOG(WARN, "GroupManager schedule too long, used %f seconds, total %f seconds, detail: %s",
                   usedTimeSecond, (double)(endTime - bt0) / 1000.0 / 1000.0, TIME_TRACK_STRING());
    }
}



void GroupManager::diffAndRecordGroupStatus(
        const map<groupid_t, GroupStatus> &groupStatusCache)
{
    vector<std::function<void()>> funcs;
    for (auto kv : groupStatusCache) {
        auto rIt = _groupStatusCache.find(kv.first);
        if (rIt == _groupStatusCache.end()) {
            funcs.push_back([=](){this->recordStatus(kv.second, GroupStatus());});
        } else {
            funcs.push_back([=](){this->recordStatus(kv.second, rIt->second);});
        }
    }
    _runner.Run(funcs);
}

void GroupManager::recordStatus(const GroupStatus &newGroupStatus,
                                const GroupStatus &oriGroupStatus) const
{
    string oriStatusStr;
    string newStatusStr;
    try {
        oriStatusStr = FastToJsonString(oriGroupStatus);
        newStatusStr = FastToJsonString(newGroupStatus);
    } catch (const autil::legacy::ExceptionBase &e) {
        CARBON_LOG(ERROR, "jsonize Group status failed, groupId:[%s], error:[%s].",
                   newGroupStatus.groupId.c_str(), e.what());
        return;
    }
    if (oriStatusStr != newStatusStr) {
        common::Recorder::newCurrent(newGroupStatus.groupId, newStatusStr);
    }
}

void GroupManager::releaseTags() {
    set<tag_t> toReleaseTags;
    genToReleaseTags(&toReleaseTags);
    set<tag_t>::const_iterator it = toReleaseTags.begin();
    for (; it != toReleaseTags.end(); it++) {
        _hippoAdapterPtr->releaseTag(*it);
    }
}

void GroupManager::genToReleaseTags(set<tag_t> *toReleaseTags) const {
    set<tag_t> curTags;
    getResourceTags(&curTags);
    set<tag_t> inUsetags;
    _hippoAdapterPtr->getAllTags(&inUsetags);
    set_difference(inUsetags.begin(), inUsetags.end(),
                   curTags.begin(), curTags.end(),
                   inserter(*toReleaseTags, toReleaseTags->end()));

    if (toReleaseTags->size() > 0) {
        CARBON_LOG(INFO, "curTags:[%s], inUseTags:[%s], toReleasedTags:[%s].",
                   autil::legacy::ToJsonString(curTags, true).c_str(),
                   autil::legacy::ToJsonString(inUsetags, true).c_str(),
                   autil::legacy::ToJsonString(*toReleaseTags, true).c_str());
    }
}

void GroupManager::getResourceTags(set<tag_t> *tags) const {
    map<groupid_t, GroupPtr>::const_iterator it = _groups.begin();
    for (; it != _groups.end(); it++) {
        it->second->getResourceTags(tags);
    }
}


void GroupManager::updateGroupPlans(const VersionedGroupPlanMap &groupPlanMap) {
    for (VersionedGroupPlanMap::const_iterator groupPlanIt = groupPlanMap.begin();
         groupPlanIt != groupPlanMap.end(); groupPlanIt++)
    {
        const groupid_t &groupId = groupPlanIt->first;
        if (_groups.find(groupId) == _groups.end()) {
            GroupPtr groupPtr = createGroup(groupId);
            if (groupPtr == NULL) {
                CARBON_LOG(ERROR, "create group [%s] failed.",
                        groupId.c_str());
                continue;
            }
            _groups[groupId] = groupPtr;
        }
        _groups[groupId]->setPlan(groupPlanIt->second.version, groupPlanIt->second.plan);
    }
}

GroupPtr GroupManager::createGroup(const groupid_t &groupId) {
    string groupSerializePath = GROUP_PLAN_SERIALIZE_FILE_PREFIX + groupId;
    SerializerPtr serializerPtr = _serializerCreatorPtr->create(
            groupSerializePath, groupId, true);
    if (serializerPtr == NULL) {
        CARBON_LOG(ERROR, "can not create serializer for group [%s]",
                   groupId.c_str());
        return GroupPtr();
    }
    
    GroupPtr groupPtr;
    groupPtr.reset(new Group(groupId, serializerPtr,
                             _hippoAdapterPtr, _healthCheckerManagerPtr,
                             _serviceSwitchManagerPtr));
    return groupPtr;
}


void GroupManager::stopDeletedGroups(const VersionedGroupPlanMap &groupPlanMap) {
    for (map<groupid_t, GroupPtr>::iterator it = _groups.begin();
         it != _groups.end();)
    {
        const groupid_t &groupId = it->first;
        if (groupPlanMap.find(groupId) != groupPlanMap.end()) {
            it++;
            continue;
        }
        
        CARBON_LOG(INFO, "stop group:[%s]", groupId.c_str());
        it->second->stop();
        if (it->second->isStopped()) {
            CARBON_LOG(INFO, "remove stopped group:[%s]", groupId.c_str());
            _groups.erase(it++);
        } else {
            ++it;
        }
    }
}

void GroupManager::executeOpsCmd() {
    ScopedLock lock(_releaseSlotLock);
    map<int32_t, ReleaseSlotInfo> toReleaseSlots;
    toReleaseSlots.swap(_toReleaseSlots);
    for (auto toReleaseSlotPair : toReleaseSlots) {
        const ReleaseSlotInfo &releaseSlotInfo = toReleaseSlotPair.second;
        auto iter = _groups.find(releaseSlotInfo.carbonUnitId.groupId);
        if (iter == _groups.end()) {
            continue;
        }
        iter->second->releaseSlot(releaseSlotInfo);
    }
}

void GroupManager::scheduleGroups() {
    vector<std::function<void()>> funcs;
    for (const auto &group : _groups) {
        funcs.push_back([group](){group.second->schedule();});
    }
    _runner.Run(funcs);
}

void GroupManager::getGroupStatus(const vector<groupid_t> &groupIds,
                                  vector<GroupStatus> *groupStatusVect) const
{
    groupStatusVect->clear();
    ScopedReadLock lock(_groupStatusLock);
    if (groupIds.size() == 0) {
        groupStatusVect->resize(_groupStatusCache.size());
        size_t i = 0;
        map<groupid_t, GroupStatus>::const_iterator it;
        for (it = _groupStatusCache.begin(); it != _groupStatusCache.end();
             it ++, i++)
        {
            (*groupStatusVect)[i] = it->second;
        }
    } else {
        for (size_t i = 0; i < groupIds.size(); i++) {
            const groupid_t &groupId = groupIds[i];
            map<groupid_t, GroupStatus>::const_iterator it =
                _groupStatusCache.find(groupId);
            if (it == _groupStatusCache.end()) {
                continue;
            }
            groupStatusVect->push_back(it->second);
        }
    }
}

void GroupManager::getAllGroupStatus(
        map<groupid_t, GroupStatus> *groupStatusMap) const
{
    groupStatusMap->clear();
    for (map<groupid_t, GroupPtr>::const_iterator it = _groups.begin();
         it != _groups.end(); it++)
    {
        GroupStatus &groupStatus = (*groupStatusMap)[it->first];
        it->second->fillGroupStatus(&groupStatus);
    }
}

bool GroupManager::__forTestAdjustHippoAdapterStatus(const std::string &value) {
    if (value == __ADJUST_HIPPO_ADAPTER_STATUS_STOP) {
        CARBON_LOG(INFO, "[for case only api] stop hippo adapter");
        return _hippoAdapterPtr->stop();
    } else if (value == __ADJUST_HIPPO_ADAPTER_STATUS_START) {
        CARBON_LOG(INFO, "[for case only api] start hippo adapter");
        return _hippoAdapterPtr->start();
    } else {
        CARBON_LOG(INFO, "[for case only api] invalid ");
    }
    return false;
}

bool GroupManager::__forTestAdjustScheduleInterval(const std::string &value) {
    int64_t interval = 0;
    if (!StringUtil::strToInt64(value.c_str(), interval)) {
        CARBON_LOG(ERROR, "[for case only api] convert interval "
                   "from string [%s] to int failed", value.c_str());
        return false;
    }
    interval = interval * 1000L * 1000L;
    if (_scheduleLoopThreadPtr == NULL) {
        CARBON_LOG(ERROR, "[for case only api] scheduleLoopThread not started");
        return false;
    }
    _scheduleLoopThreadPtr->stop();
    _scheduleLoopThreadPtr = LoopThread::createLoopThread(
            std::bind(&GroupManager::schedule, this),
            interval);
    if (_scheduleLoopThreadPtr == NULL) {
        CARBON_LOG(ERROR, "create group schedule loop thread failed.");
        return false;
    }
    return true;
}



END_CARBON_NAMESPACE(master);

