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

#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/admin/AdminZkDataAccessor.h"
#include "swift/admin/WorkerInfo.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace protocol {
class HeartbeatInfo;
class RoleVersionInfos;
} // namespace protocol

namespace monitor {
struct SysControlMetricsCollector;

namespace protocol {} // namespace protocol

} // namespace monitor
} // namespace swift

namespace swift {
namespace admin {

class WorkerTable {
public:
    WorkerTable();
    ~WorkerTable();

private:
    WorkerTable(const WorkerTable &);
    WorkerTable &operator=(const WorkerTable &);

public:
    bool updateWorker(const protocol::HeartbeatInfo &heartbeatInfo);
    void updateBrokerVersionInfos(const protocol::RoleVersionInfos &roleInfos);
    void updateBrokerRoles(std::vector<std::string> &roleNames);
    void updateAdminRoles();
    WorkerMap getAdminWorkers() const;
    WorkerMap getBrokerWorkers() const;
    uint64_t getBrokerCount() const;
    void clear();
    void prepareDecision();
    void setDataAccessor(AdminZkDataAccessorPtr accessor);
    void adjustWorkerResource(const std::map<std::string, float> &resMap);

    protocol::BrokerVersionInfo getBrokerVersionInfo(const std::string &roleName);

public:
    void collectMetrics(monitor::SysControlMetricsCollector &collector, uint32_t adminCount);
    void clearCurrentTask(const std::string &groupName);
    bool addBrokerInMetric(const std::string &roleName, const BrokerInStatus &metric);
    size_t findErrorBrokers(std::vector<std::string> &zombieWorkers,
                            std::vector<std::string> &timeoutWorkers,
                            uint32_t deadBrokerTimeoutSec,
                            int64_t zfsTimeout,
                            int64_t dfsTimeout);
    bool getBrokerInStatus(const std::string &roleName, std::vector<std::pair<std::string, BrokerInStatus>> &status);
    void setBrokerUnknownTimeout(const int64_t &timeout);

private:
    bool updateWorker(const protocol::HeartbeatInfo &heartbeatInfo, WorkerMap &workerMap);

private:
    mutable autil::ThreadMutex _mutex;
    WorkerMap _brokerWorkerMap;
    WorkerMap _adminWorkerMap;
    int64_t _unknownTimeout;
    AdminZkDataAccessorPtr _dataAccessor;
    mutable autil::ReadWriteLock _brokerVersionLock;
    std::map<std::string, protocol::BrokerVersionInfo> _brokerVersionMap;

private:
    friend class WorkerTableTest;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace admin
} // namespace swift
