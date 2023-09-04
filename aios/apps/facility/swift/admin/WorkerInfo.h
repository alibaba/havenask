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
#pragma once

#include <assert.h>
#include <map>
#include <memory>
#include <set>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/admin/RoleInfo.h"
#include "swift/common/Common.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/util/CircularVector.h"

namespace swift {
namespace admin {

struct BrokerInStatus {
    uint32_t updateTime = 0;
    double cpu = 0;
    double mem = 0;
    uint64_t writeRate1min = 0;
    uint64_t writeRate5min = 0;
    uint64_t readRate1min = 0;
    uint64_t readRate5min = 0;
    uint64_t writeRequest1min = 0;
    uint64_t writeRequest5min = 0;
    uint64_t readRequest1min = 0;
    uint64_t readRequest5min = 0;
    int64_t commitDelay = -1;
    int64_t zfsTimeout = -1;
};

class TopicTable;

class WorkerInfo {
public:
    typedef std::pair<std::string, uint32_t> TaskPair;
    typedef std::set<TaskPair> TaskSet;
    typedef std::map<TaskPair, std::string> TaskMap;
    struct StatusInfo {
        StatusInfo() : status(protocol::PARTITION_STATUS_WAITING), sessionId(-1) {}
        StatusInfo(protocol::PartitionStatus st, int64_t sid, protocol::InlineVersion iv)
            : status(st), sessionId(sid), inlineVersion(iv) {}
        protocol::PartitionStatus status;
        int64_t sessionId;
        protocol::InlineVersion inlineVersion;
    };

public:
    WorkerInfo(const RoleInfo &roleInfo);
    ~WorkerInfo();

private:
    WorkerInfo(const WorkerInfo &);
    WorkerInfo &operator=(const WorkerInfo &);

public:
    // these interfaces should only be called in decision function
    void prepareDecision(int64_t currentTime, int64_t unknownTimeout);
    // should be only called after prepareDecision
    bool needChange(TopicTable &topicTable, bool enableFastRecover = false);
    bool isDead();
    protocol::DecisionStatus getStatus();
    bool hasTaskInCurrent(const std::string &topicName, uint32_t partition, StatusInfo &statusInfo);
    bool addTask2target(const std::string &topicName,
                        uint32_t partition,
                        uint32_t resource,
                        uint32_t partitionLimit,
                        const std::string &groupName = "");
    void setCurrent(const protocol::HeartbeatInfo &current);
    void setMaxResource(uint32_t resource);
    uint32_t getMaxResource() const;
    const TaskSet &getTargetTask() const;
    const protocol::HeartbeatInfo &getCurrentHeartbeat() const;
    TaskSet getCurrentTask() const;
    TaskMap getCurrentTaskMap() const;
    int64_t getCurrentSessionId() const;

    std::string getCurrentRoleAddress() const;
    std::string getTargetRoleAddress() const;
    void updateRoleIp(const std::string &ip);
    void updateRolePort(uint16_t port);
    RoleInfo getRoleInfo() const;

    uint32_t getFreeResource() const;
    void adjustFreeResource(float ratio);
    void fillWorkerStatus(protocol::WorkerStatus &status);
    const std::map<TaskPair, StatusInfo> &getCurrentTaskStatus() { return _currentTaskStatus; }
    void setLeaderAddress(const std::string &address);
    const std::string &getLeaderAddress() const;
    void clearCurrentTask();

    protocol::BrokerVersionInfo getBrokerVersionInfo();
    int64_t getUnknownTime();
    void setLastReleaseTime(int64_t time) { _lastReleaseTime = time; }
    int64_t getLastReleaseTime() { return _lastReleaseTime; }
    void updateLastSessionId();
    bool addBrokerInMetric(const BrokerInStatus &metric);
    bool isDeadInBrokerStatus(uint32_t timeoutSec) const;
    bool isZkDfsTimeout(int64_t zfsTimeout, int64_t dfsTimeout) const;
    BrokerInStatus getLastBrokerInStatus() const;

private:
    bool canLoad(const std::string &topicName, uint32_t resource, uint32_t partitionLimit);
    void checkStatus(int64_t currentTime, int64_t unknownTimeOut);

private:
    protocol::HeartbeatInfo _current;
    protocol::HeartbeatInfo _snapshot;
    TaskSet _targetTaskSet;
    std::map<TaskPair, StatusInfo> _currentTaskStatus;
    std::map<std::string, uint32_t> _targetPartitionCount;
    mutable autil::ReadWriteLock _lock;
    mutable autil::ReadWriteLock _roleInfoLock;
    protocol::DecisionStatus _workerStatus;
    uint32_t _freeResource;
    uint32_t _maxResource;
    RoleInfo _roleInfo;
    int64_t _unknownStartTime;
    int64_t _lastReleaseTime;
    std::string _leaderAddress;
    std::string _groupName;
    bool _firstDead;
    uint32_t _startTimeSec;
    int64_t _lastSessionId;
    mutable autil::ReadWriteLock _brokerInMetricsLock;
    util::CircularVector<BrokerInStatus> _brokerInMetrics;

private:
    friend class WorkerInfoTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(WorkerInfo);

typedef std::map<std::string, WorkerInfoPtr> WorkerMap;

inline bool operator<(const WorkerInfoPtr &lhs, const WorkerInfoPtr &rhs) {
    assert(lhs && rhs);
    if (lhs->getFreeResource() == rhs->getFreeResource()) {
        return lhs->getRoleInfo().roleName < rhs->getRoleInfo().roleName;
    } else {
        return lhs->getFreeResource() < rhs->getFreeResource();
    }
}

inline bool operator>(const WorkerInfoPtr &lhs, const WorkerInfoPtr &rhs) { return rhs < lhs; }

} // namespace admin
} // namespace swift
