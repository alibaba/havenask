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
#include "swift/admin/WorkerInfo.h"

#include <algorithm>
#include <cstddef>
#include <stdint.h>

#include "autil/TimeUtility.h"
#include "swift/admin/TopicInfo.h"
#include "swift/admin/TopicTable.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageComparator.h"

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, WorkerInfo);
using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::common;

const uint32_t DEFAULT_MAX_RESOURCE = 10000;

WorkerInfo::WorkerInfo(const RoleInfo &roleInfo) {
    _workerStatus = ROLE_DS_UNKOWN;
    _maxResource = DEFAULT_MAX_RESOURCE;
    _freeResource = _maxResource;
    _roleInfo = roleInfo;
    _groupName = roleInfo.getGroupName();
    _unknownStartTime = -1;
    _lastReleaseTime = TimeUtility::currentTime();
    _startTimeSec = _lastReleaseTime / 1000000;
    _firstDead = false;
    _lastSessionId = 0;
}

WorkerInfo::~WorkerInfo() {}

void WorkerInfo::setCurrent(const protocol::HeartbeatInfo &current) {
    ScopedWriteLock lock(_lock);
    _current = current;
    uint32_t brokerStartTime = _current.sessionid() / 1000000;
    _startTimeSec = max(_startTimeSec, brokerStartTime);
}

uint32_t WorkerInfo::getFreeResource() const {
    ScopedReadLock lock(_lock);
    return _freeResource;
}

void WorkerInfo::adjustFreeResource(float ratio) {
    ScopedReadLock lock(_lock);
    _freeResource = (uint32_t)(_freeResource * ratio);
}

bool WorkerInfo::needChange(TopicTable &topicTable, bool enableFastRecover) {
    if (_lastSessionId != _snapshot.sessionid()) {
        return true;
    }
    if (_workerStatus == ROLE_DS_DEAD && _firstDead) {
        _firstDead = false;
        return true;
    }
    uint32_t taskCount = _targetTaskSet.size();
    if (_targetTaskSet.size() != (uint32_t)_snapshot.tasks_size()) {
        return true;
    }
    if (getTargetRoleAddress() != _snapshot.address()) {
        return true;
    }
    for (uint32_t i = 0; i < taskCount; ++i) {
        const TaskStatus &taskStatus = _snapshot.tasks(i);
        assert(taskStatus.has_taskinfo());
        const TaskInfo &taskInfo = taskStatus.taskinfo();
        assert(taskInfo.has_partitionid());
        assert(taskInfo.partitionid().has_topicname() && taskInfo.partitionid().has_id());
        TaskPair tempPair(taskInfo.partitionid().topicname(), taskInfo.partitionid().id());
        TaskSet::iterator it = _targetTaskSet.find(tempPair);
        if (it == _targetTaskSet.end()) {
            return true;
        } else if (taskInfo.partitionid().has_version()) {
            TopicInfoPtr topicInfo = topicTable.findTopic(it->first);
            if (topicInfo == NULL || taskInfo.partitionid().version() != topicInfo->getModifyTime()) {
                return true;
            }
            if (enableFastRecover &&
                taskInfo.partitionid().inlineversion() != topicInfo->getInlineVersion(taskInfo.partitionid().id())) {
                return true;
            }
        }
    }
    return false;
}

void WorkerInfo::updateLastSessionId() { _lastSessionId = _snapshot.sessionid(); }

void WorkerInfo::prepareDecision(int64_t currentTime, int64_t unknownTimeout) {
    ScopedWriteLock lock(_lock);
    _snapshot = _current;
    _currentTaskStatus.clear();
    int32_t count = _snapshot.tasks_size();
    for (int i = 0; i < count; ++i) {
        const TaskStatus &taskStatus = _snapshot.tasks(i);
        const PartitionId &pid = taskStatus.taskinfo().partitionid();
        _currentTaskStatus[make_pair(pid.topicname(), pid.id())] = StatusInfo(
            taskStatus.status(), taskStatus.sessionid(), taskStatus.taskinfo().partitionid().inlineversion());
    }

    _targetTaskSet.clear();
    _targetPartitionCount.clear();
    _freeResource = _maxResource;
    checkStatus(currentTime, unknownTimeout);
}

bool WorkerInfo::hasTaskInCurrent(const string &topicName, uint32_t partition, StatusInfo &statusInfo) {
    map<TaskPair, WorkerInfo::StatusInfo>::const_iterator it = _currentTaskStatus.find(make_pair(topicName, partition));
    if (it != _currentTaskStatus.end()) {
        statusInfo = it->second;
        return true;
    } else {
        return false;
    }
}

void WorkerInfo::clearCurrentTask() { _currentTaskStatus.clear(); }

bool WorkerInfo::addTask2target(
    const string &topicName, uint32_t partition, uint32_t resource, uint32_t partitionLimit, const string &groupName) {
    if (groupName != _groupName) {
        return false;
    }
    if (resource == 0) {
        AUTIL_LOG(ERROR, "Invalid resource parameter:[%ud]", resource);
        return false;
    }
    if (!topicName.empty() && canLoad(topicName, resource, partitionLimit)) {
        pair<TaskSet::iterator, bool> ret = _targetTaskSet.insert(TaskPair(topicName, partition));
        if (ret.second) {
            _freeResource -= resource;
            _targetPartitionCount[topicName]++;
            return true;
        } else {
            AUTIL_LOG(ERROR,
                      "Topic has already been assigned to this broker:"
                      "[%s]-[%ud]",
                      topicName.c_str(),
                      partition);
            return false;
        }
    }
    return false;
}

string WorkerInfo::getCurrentRoleAddress() const {
    ScopedReadLock lock(_lock);
    if (!_current.has_address()) {
        return "";
    } else {
        return _current.address();
    }
}

string WorkerInfo::getTargetRoleAddress() const {
    ScopedReadLock lock(_roleInfoLock);
    return _roleInfo.getRoleAddress();
}

void WorkerInfo::updateRoleIp(const string &ip) {
    ScopedWriteLock lock(_roleInfoLock);
    _roleInfo.ip = ip;
}

void WorkerInfo::updateRolePort(uint16_t port) {
    ScopedWriteLock lock(_roleInfoLock);
    _roleInfo.port = port;
}

RoleInfo WorkerInfo::getRoleInfo() const {
    ScopedReadLock lock(_roleInfoLock);
    return _roleInfo;
}

const WorkerInfo::TaskSet &WorkerInfo::getTargetTask() const { return _targetTaskSet; }

const protocol::HeartbeatInfo &WorkerInfo::getCurrentHeartbeat() const {
    ScopedReadLock lock(_lock);
    return _current;
}

WorkerInfo::TaskSet WorkerInfo::getCurrentTask() const {
    TaskSet taskSet;
    ScopedReadLock lock(_lock);
    const HeartbeatInfo *hbinfo = _snapshot.tasks_size() > 0 ? &_snapshot : &_current;
    int32_t count = hbinfo->tasks_size();
    for (int i = 0; i < count; ++i) {
        const TaskStatus &taskStatus = hbinfo->tasks(i);
        const PartitionId &pid = taskStatus.taskinfo().partitionid();
        taskSet.insert(make_pair(pid.topicname(), pid.id()));
    }
    return taskSet;
}

WorkerInfo::TaskMap WorkerInfo::getCurrentTaskMap() const {
    TaskMap taskMap;
    ScopedReadLock lock(_lock);
    int32_t count = _snapshot.tasks_size();
    for (int i = 0; i < count; ++i) {
        const TaskStatus &taskStatus = _snapshot.tasks(i);
        const PartitionId &pid = taskStatus.taskinfo().partitionid();
        taskMap[make_pair(pid.topicname(), pid.id())] = _snapshot.address();
    }
    return taskMap;
}

int64_t WorkerInfo::getCurrentSessionId() const { return _snapshot.sessionid(); }

DecisionStatus WorkerInfo::getStatus() {
    ScopedReadLock lock(_lock);
    return _workerStatus;
}

bool WorkerInfo::isDead() {
    ScopedReadLock lock(_lock);
    return _workerStatus == ROLE_DS_DEAD;
}

void WorkerInfo::checkStatus(int64_t currentTime, int64_t unknownTimeout) {
    if (_snapshot.alive()) {
        _workerStatus = ROLE_DS_ALIVE;
        _unknownStartTime = -1;
    } else {
        if (_workerStatus == ROLE_DS_ALIVE) {
            _workerStatus = ROLE_DS_UNKOWN;
            _unknownStartTime = currentTime;
        } else if (_workerStatus == ROLE_DS_UNKOWN) {
            if (currentTime - _unknownStartTime > unknownTimeout) {
                _workerStatus = ROLE_DS_DEAD;
                _firstDead = true;
            }
        }
    }
}

int64_t WorkerInfo::getUnknownTime() {
    if (_unknownStartTime <= 0) {
        return 0;
    } else {
        return TimeUtility::currentTime() - _unknownStartTime;
    }
}

void WorkerInfo::setMaxResource(uint32_t resource) { _maxResource = resource; }

uint32_t WorkerInfo::getMaxResource() const { return _maxResource; }

void WorkerInfo::fillWorkerStatus(protocol::WorkerStatus &status) {
    ScopedReadLock lock(_lock);
    *(status.mutable_current()) = _current;
    for (TaskSet::iterator it = _targetTaskSet.begin(); it != _targetTaskSet.end(); ++it) {
        PartitionId *partitionId = status.add_target();
        partitionId->set_topicname(it->first);
        partitionId->set_id(it->second);
        partitionId->set_topicgroup(_groupName);
    }
    status.set_decisionstatus(_workerStatus);
    status.set_freeresource(_freeResource);
    status.set_maxresource(_maxResource);
}

bool WorkerInfo::canLoad(const string &topicName, uint32_t resource, uint32_t partitionLimit) {
    if (resource > _freeResource) {
        return false;
    }
    if (_targetPartitionCount[topicName] >= partitionLimit) {
        return false;
    }
    return true;
}

void WorkerInfo::setLeaderAddress(const string &address) { _leaderAddress = address; }

const string &WorkerInfo::getLeaderAddress() const { return _leaderAddress; }

BrokerVersionInfo WorkerInfo::getBrokerVersionInfo() {
    ScopedReadLock lock(_lock);
    return _current.versioninfo();
}

bool WorkerInfo::addBrokerInMetric(const BrokerInStatus &metric) {
    ScopedWriteLock lock(_brokerInMetricsLock);
    _brokerInMetrics.push_back(metric);
    return true;
}

bool WorkerInfo::isDeadInBrokerStatus(uint32_t timeoutSec) const {
    if (ROLE_TYPE_BROKER != getRoleInfo().roleType) {
        return false;
    }
    uint32_t currentTime = TimeUtility::currentTimeInSeconds();
    uint32_t lastReportTime = _startTimeSec;
    ScopedReadLock lock(_brokerInMetricsLock);
    if (0 != _brokerInMetrics.size()) {
        lastReportTime = max(_brokerInMetrics.back().updateTime, _startTimeSec);
    }
    return currentTime - lastReportTime > timeoutSec;
}

BrokerInStatus WorkerInfo::getLastBrokerInStatus() const {
    ScopedReadLock lock(_brokerInMetricsLock);
    return _brokerInMetrics.back();
}

bool WorkerInfo::isZkDfsTimeout(int64_t zfsTimeout, int64_t commitDelayThresholdInSec) const {
    ScopedReadLock lock(_brokerInMetricsLock);
    if (0 != _brokerInMetrics.size()) {
        return _brokerInMetrics.back().zfsTimeout > zfsTimeout ||
               _brokerInMetrics.back().commitDelay > commitDelayThresholdInSec * 1000 * 1000;
    } else {
        return false;
    }
}

} // namespace admin
} // namespace swift
