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
#include "swift/admin/WorkerTable.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>

#include "autil/TimeUtility.h"
#include "swift/admin/RoleInfo.h"
#include "swift/common/PathDefine.h"
#include "swift/config/ConfigDefine.h"
#include "swift/heartbeat/ZkHeartbeatWrapper.h"
#include "swift/monitor/AdminMetricsReporter.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/util/ProtoUtil.h"
#include "swift/util/TargetRecorder.h"

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, WorkerTable);
using namespace swift::protocol;
using namespace swift::heartbeat;
using namespace swift::util;
using namespace std;
using namespace autil;

WorkerTable::WorkerTable() : _unknownTimeout(config::DEFAULT_BROKER_UNKNOWN_TIMEOUT) {}

WorkerTable::~WorkerTable() {}

bool WorkerTable::updateWorker(const protocol::HeartbeatInfo &heartbeatInfo) {
    ScopedLock lock(_mutex);
    if (heartbeatInfo.role() == ROLE_TYPE_ADMIN) {
        return updateWorker(heartbeatInfo, _adminWorkerMap);
    } else if (heartbeatInfo.role() == ROLE_TYPE_BROKER) {
        return updateWorker(heartbeatInfo, _brokerWorkerMap);
    } else {
        AUTIL_LOG(ERROR, "receive strange heartbeat %s", heartbeatInfo.ShortDebugString().c_str());
        return false;
    }
}

bool WorkerTable::updateWorker(const protocol::HeartbeatInfo &heartbeatInfo, WorkerMap &workerMap) {
    if (!_dataAccessor) {
        AUTIL_LOG(ERROR, "data accessor is null");
        return false;
    }
    AUTIL_LOG(INFO, "%s", ProtoUtil::getHeartbeatStr(heartbeatInfo).c_str());
    const string &roleAddress = heartbeatInfo.address();
    string roleName, address, ip;
    uint16_t port;
    if (!common::PathDefine::parseRoleAddress(roleAddress, roleName, address)) {
        AUTIL_LOG(WARN, "receive bad heartbeat");
    }
    if (!common::PathDefine::parseAddress(address, ip, port)) {
        AUTIL_LOG(WARN, "receive bad heartbeat");
    }
    if (ip.empty() || port == 0) {
        AUTIL_LOG(WARN, "receive bad heartbeat");
        return false;
    }
    if (heartbeatInfo.role() == ROLE_TYPE_ADMIN) {
        roleName = roleAddress;
    }

    WorkerMap::iterator workerIt = workerMap.find(roleName);
    if (workerIt != workerMap.end()) {
        if (ROLE_TYPE_ADMIN != heartbeatInfo.role()) {
            string leaderAddress = workerIt->second->getLeaderAddress();
            if (address != leaderAddress) {
                bool isSuc = _dataAccessor->readLeaderAddress(roleName, leaderAddress);
                if (isSuc && !leaderAddress.empty()) {
                    workerIt->second->setLeaderAddress(leaderAddress);
                    if (address != leaderAddress) {
                        AUTIL_LOG(WARN,
                                  "receive old dead heartbeat from [%s], current leader is [%s] ignore it!",
                                  address.c_str(),
                                  leaderAddress.c_str());
                        return false;
                    }
                } else {
                    AUTIL_LOG(WARN, "read leader info failed!");
                    const RoleInfo &roleInfo = workerIt->second->getRoleInfo();
                    if (roleInfo.getAddress().empty()) {
                        AUTIL_LOG(WARN, "first receive heartbeat from [%s].", address.c_str());
                    } else if (ip != roleInfo.ip) {
                        AUTIL_LOG(
                            WARN, "heartbeat from %s, last heartbeat from  [%s]", address.c_str(), roleInfo.ip.c_str());
                    }
                    if (!heartbeatInfo.alive() && roleInfo.port != 0 &&
                        ((roleInfo.ip != ip && roleInfo.port == port) || roleInfo.port != port)) {
                        AUTIL_LOG(WARN, "receive old dead heartbeat from [%s], ignore it!", address.c_str());
                        return false;
                    }
                }
            }
        }
        workerIt->second->updateRolePort(port);
        workerIt->second->updateRoleIp(ip);
        workerIt->second->setCurrent(heartbeatInfo);
    } else {
        AUTIL_LOG(INFO, "create workerInfo[%s] from heartbeat", roleName.c_str());
        string leaderAddress;
        bool isSuc = _dataAccessor->readLeaderAddress(roleName, leaderAddress);
        if (!isSuc || leaderAddress.empty()) {
            isSuc = false;
            leaderAddress = address;
        }
        AUTIL_LOG(INFO, "read role [%s] leader address is [%s]", roleName.c_str(), leaderAddress.c_str());
        if (leaderAddress == address) {
            RoleInfo roleInfo(roleName, ip, port, heartbeatInfo.role());
            WorkerInfoPtr workerInfoPtr(new WorkerInfo(roleInfo));
            workerInfoPtr->setCurrent(heartbeatInfo);
            workerMap[roleName] = workerInfoPtr;
            if (isSuc) {
                workerInfoPtr->setLeaderAddress(leaderAddress);
            }
        } else {
            if (common::PathDefine::parseAddress(leaderAddress, ip, port)) {
                RoleInfo roleInfo(roleName, ip, port, heartbeatInfo.role());
                WorkerInfoPtr workerInfoPtr(new WorkerInfo(roleInfo));
                workerMap[roleName] = workerInfoPtr;
                workerInfoPtr->setLeaderAddress(leaderAddress);
                // admin follower first commit heartbeat need set current info
                if (heartbeatInfo.role() == ROLE_TYPE_ADMIN) {
                    workerInfoPtr->setCurrent(heartbeatInfo);
                }
            }
            AUTIL_LOG(WARN,
                      "receive strange heart beat, current role [%s] "
                      "leader address is [%s]",
                      roleName.c_str(),
                      leaderAddress.c_str());
        }
    }
    return true;
}

void WorkerTable::updateBrokerRoles(vector<string> &roleNames) {
    ScopedLock lock(_mutex);
    WorkerMap newMap;
    for (size_t i = 0; i < roleNames.size(); i++) {
        WorkerMap::iterator workerIt = _brokerWorkerMap.find(roleNames[i]);
        if (workerIt != _brokerWorkerMap.end()) {
            newMap[workerIt->first] = workerIt->second;
        } else {
            RoleInfo roleInfo;
            roleInfo.roleName = roleNames[i];
            roleInfo.roleType = protocol::ROLE_TYPE_BROKER;
            WorkerInfoPtr workerInfoPtr(new WorkerInfo(roleInfo));
            newMap[roleNames[i]] = workerInfoPtr;
            AUTIL_LOG(INFO, "create broker[%s] from admin", roleNames[i].c_str());
        }
    }
    _brokerWorkerMap = newMap;
}

void WorkerTable::updateAdminRoles() {
    ScopedLock lock(_mutex);
    WorkerMap newMap;
    WorkerMap::iterator workerIt = _adminWorkerMap.begin();
    for (; workerIt != _adminWorkerMap.end(); workerIt++) {
        if (workerIt->second->isDead()) {
            AUTIL_LOG(WARN, "admin[%s] is dead, remove from admin worker table", workerIt->first.c_str());
        } else {
            newMap[workerIt->first] = workerIt->second;
        }
    }
    _adminWorkerMap = newMap;
}

WorkerMap WorkerTable::getAdminWorkers() const {
    ScopedLock lock(_mutex);
    return _adminWorkerMap;
}

WorkerMap WorkerTable::getBrokerWorkers() const {
    ScopedLock lock(_mutex);
    return _brokerWorkerMap;
}

uint64_t WorkerTable::getBrokerCount() const {
    ScopedLock lock(_mutex);
    return _brokerWorkerMap.size();
}

void WorkerTable::prepareDecision() {
    ScopedLock lock(_mutex);
    for (WorkerMap::iterator it = _brokerWorkerMap.begin(); it != _brokerWorkerMap.end(); ++it) {
        it->second->prepareDecision(TimeUtility::currentTime(), _unknownTimeout);
    }
    // admin worker do not participate decision, but need update status
    for (WorkerMap::iterator it = _adminWorkerMap.begin(); it != _adminWorkerMap.end(); ++it) {
        it->second->prepareDecision(TimeUtility::currentTime(), _unknownTimeout);
    }
}

void WorkerTable::clear() {
    {
        ScopedLock lock(_mutex);
        _brokerWorkerMap.clear();
        _adminWorkerMap.clear();
    }
    {
        ScopedWriteLock lock(_brokerVersionLock);
        _brokerVersionMap.clear();
    }
}

void WorkerTable::clearCurrentTask(const string &groupName) {
    ScopedLock lock(_mutex);
    for (WorkerMap::iterator it = _brokerWorkerMap.begin(); it != _brokerWorkerMap.end(); ++it) {
        const RoleInfo &roleInfo = it->second->getRoleInfo();
        string workerGroupName = roleInfo.getGroupName();
        if (groupName.empty() || groupName == workerGroupName) {
            it->second->clearCurrentTask();
            AUTIL_LOG(INFO, "clear current task for [%s]", roleInfo.getRoleAddress().c_str());
        }
    }
}

void WorkerTable::collectMetrics(monitor::SysControlMetricsCollector &collector, uint32_t adminCount) {
    uint32_t totalBrokerCount = 0;
    uint32_t totalAliveBrokerCount = 0;
    uint32_t totalDeadBrokerCount = 0;
    uint32_t totalUnknownBrokerCount = 0;

    uint32_t totalAdminCount = adminCount;
    uint32_t totalAliveAdminCount = 0;
    uint32_t totalDeadAdminCount = 0;
    uint32_t totalUnknownAdminCount = 0;
    uint32_t freeResource = 0;
    uint32_t totalResource = 0;
    {
        ScopedLock lock(_mutex);
        DecisionStatus status;
        totalBrokerCount = _brokerWorkerMap.size();
        for (WorkerMap::iterator it = _brokerWorkerMap.begin(); it != _brokerWorkerMap.end(); ++it) {
            status = it->second->getStatus();
            if (status == ROLE_DS_ALIVE) {
                totalAliveBrokerCount++;
                totalResource += it->second->getMaxResource();
                freeResource += it->second->getFreeResource();
            } else if (status == ROLE_DS_UNKOWN) {
                totalUnknownBrokerCount++;
            } else if (status == ROLE_DS_DEAD) {
                totalDeadBrokerCount++;
            }
        }
        for (WorkerMap::iterator it = _adminWorkerMap.begin(); it != _adminWorkerMap.end(); ++it) {
            status = it->second->getStatus();
            if (status == ROLE_DS_ALIVE) {
                totalAliveAdminCount++;
            } else if (status == ROLE_DS_UNKOWN) {
                totalUnknownAdminCount++;
            } else if (status == ROLE_DS_DEAD) {
                totalDeadAdminCount++;
            }
        }
    }
    collector.workerCount = totalBrokerCount;
    collector.aliveWorkerCount = totalAliveBrokerCount;
    float percent = 0;
    if (totalBrokerCount != 0) {
        percent = (float)totalAliveBrokerCount / totalBrokerCount;
    }
    collector.aliveWorkerPercent = percent * 100;
    collector.deadWorkerCount = totalDeadBrokerCount;
    collector.unknownWorkerCount = totalUnknownBrokerCount;

    collector.adminCount = totalAdminCount;
    collector.aliveAdminCount = totalAliveAdminCount;
    percent = 0;
    if (totalAdminCount != 0) {
        percent = (float)totalAliveAdminCount / totalAdminCount;
    }
    collector.aliveAdminPercent = percent * 100;
    collector.deadAdminCount = totalDeadAdminCount;
    collector.unknownAdminCount = totalUnknownAdminCount;
    if (totalResource != 0) {
        collector.resourceUsedPercent = 1 - (double)freeResource / totalResource;
    } else {
        collector.resourceUsedPercent = 0;
    }
}

void WorkerTable::setDataAccessor(AdminZkDataAccessorPtr accessor) { _dataAccessor = accessor; }

void WorkerTable::adjustWorkerResource(const map<string, float> &resMap) {
    ScopedLock lock(_mutex);
    map<string, float>::const_iterator iter;
    WorkerMap::iterator workerIter;
    for (iter = resMap.begin(); iter != resMap.end(); iter++) {
        workerIter = _brokerWorkerMap.find(iter->first);
        if (workerIter != _brokerWorkerMap.end()) {
            AUTIL_LOG(INFO, "adjust free resource for role [%s], ratio [%f]", iter->first.c_str(), iter->second);
            workerIter->second->adjustFreeResource(iter->second);
        } else {
            AUTIL_LOG(WARN, "role [%s] not found", iter->first.c_str());
        }
    }
}

BrokerVersionInfo WorkerTable::getBrokerVersionInfo(const string &roleName) {
    {
        ScopedReadLock lock(_brokerVersionLock);
        map<string, BrokerVersionInfo>::const_iterator iter = _brokerVersionMap.find(roleName);
        if (iter != _brokerVersionMap.end()) {
            return iter->second;
        }
    }
    {
        ScopedLock lock(_mutex);
        WorkerMap::iterator workerIter = _brokerWorkerMap.find(roleName);
        if (workerIter == _brokerWorkerMap.end()) {
            BrokerVersionInfo brokerVersionInfo;
            return brokerVersionInfo;
        }
        return workerIter->second->getBrokerVersionInfo();
    }
}

void WorkerTable::updateBrokerVersionInfos(const RoleVersionInfos &roleInfos) {
    ScopedWriteLock lock(_brokerVersionLock);
    _brokerVersionMap.clear();
    for (int32_t i = 0; i < roleInfos.versioninfos_size(); i++) {
        const RoleVersionInfo &roleInfo = roleInfos.versioninfos(i);
        _brokerVersionMap[roleInfo.rolename()] = roleInfo.versioninfo();
    }
}

bool WorkerTable::addBrokerInMetric(const string &roleName, const BrokerInStatus &metric) {
    ScopedLock lock(_mutex);
    WorkerMap::iterator workerIt = _brokerWorkerMap.find(roleName);
    if (workerIt != _brokerWorkerMap.end()) {
        return workerIt->second->addBrokerInMetric(metric);
    } else {
        AUTIL_LOG(WARN, "worker info [%s] not exist", roleName.c_str());
        return false;
    }
}

size_t WorkerTable::findErrorBrokers(vector<string> &zombieWorkers,
                                     vector<string> &timeoutWorkers,
                                     uint32_t deadBrokerTimeoutSec,
                                     int64_t zfsTimeout,
                                     int64_t commitDelayThresholdInSec) {
    ScopedLock lock(_mutex);
    for (const auto &worker : _brokerWorkerMap) {
        if (worker.second->isDeadInBrokerStatus(deadBrokerTimeoutSec)) {
            const string &address = worker.second->getCurrentRoleAddress();
            if (!address.empty()) {
                zombieWorkers.emplace_back(address);
                timeoutWorkers.emplace_back(address);
            }
            continue;
        }
        if (worker.second->isZkDfsTimeout(zfsTimeout, commitDelayThresholdInSec)) {
            const string &address = worker.second->getCurrentRoleAddress();
            if (!address.empty()) {
                timeoutWorkers.emplace_back(address);
            }
        }
    }
    return timeoutWorkers.size();
}

bool WorkerTable::getBrokerInStatus(const string &roleName, vector<pair<string, BrokerInStatus>> &status) {
    if (roleName.empty()) {
        ScopedLock lock(_mutex);
        for (const auto &worker : _brokerWorkerMap) {
            const BrokerInStatus &ins = worker.second->getLastBrokerInStatus();
            status.emplace_back(worker.first, ins);
        }
    } else {
        ScopedLock lock(_mutex);
        WorkerMap::iterator iter = _brokerWorkerMap.find(roleName);
        if (iter == _brokerWorkerMap.end()) {
            return false;
        }
        const BrokerInStatus &ins = iter->second->getLastBrokerInStatus();
        status.emplace_back(iter->first, ins);
    }
    return true;
}

void WorkerTable::setBrokerUnknownTimeout(const int64_t &timeout) { _unknownTimeout = timeout; }

} // namespace admin
} // namespace swift
