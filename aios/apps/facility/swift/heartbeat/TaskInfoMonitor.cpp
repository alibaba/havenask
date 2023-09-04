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
#include "swift/heartbeat/TaskInfoMonitor.h"

#include <cstddef>
#include <functional>
#include <unistd.h>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/heartbeat/StatusUpdater.h"
#include "swift/heartbeat/ZkConnectionStatus.h"
#include "swift/protocol/Common.pb.h"
#include "swift/util/ProtoUtil.h"
#include "zookeeper/zookeeper.h"

namespace swift {
namespace heartbeat {
AUTIL_LOG_SETUP(swift, TaskInfoMonitor);

using namespace swift::protocol;
using namespace swift::util;
using namespace std;
using namespace autil;
using namespace cm_basic;

TaskInfoMonitor::TaskInfoMonitor() {
    _status = NULL;
    _isStopped = false;
    _sessionId = 0;
}

TaskInfoMonitor::~TaskInfoMonitor() {
    stop();
    _status = NULL;
}

bool TaskInfoMonitor::init(const std::string &address, int64_t sessionId, uint32_t timeout) {
    string host;
    string path;
    if (!extractParameter(address, host, path)) {
        AUTIL_LOG(ERROR, "invalid Dispatch TaskInfo address [%s]", address.c_str());
        return false;
    }
    _path = path;
    _zk.setParameter(host, timeout);
    _sessionId = sessionId;
    return true;
}

bool TaskInfoMonitor::extractParameter(const std::string &address, std::string &host, std::string &path) {
    const static string ZK_PREFIX = "zfs://";
    if (!autil::StringUtil::startsWith(address, ZK_PREFIX)) {
        AUTIL_LOG(ERROR, "Dispatch TaskInfo  address not startsWith [zfs://]");
        return false;
    }
    string trimPrefixAddress = address.substr(ZK_PREFIX.size());
    size_t pos1 = trimPrefixAddress.find("/");
    if (pos1 == string::npos) {
        AUTIL_LOG(ERROR, "find first '/' fail");
        return false;
    }
    host = trimPrefixAddress.substr(0, pos1);
    path = trimPrefixAddress.substr(pos1);
    if (!path.empty() && *path.rbegin() == '/') {
        path.resize(path.size() - 1);
    }
    return !(host.empty() || path.empty());
}

bool TaskInfoMonitor::start() {
    _isStopped = false;
    using namespace placeholders;
    if (_zk.isConnected()) {
        AUTIL_LOG(INFO, "zk is connected.");
        return true;
    }
    // TODO add process connect info before set
    _zk.setConnCallback(bind(&TaskInfoMonitor::disconnectNotify, this, _1, _2, _3));
    _zk.setDataCallback(bind(&TaskInfoMonitor::dataChange, this, _1, _2, _3));
    _zk.setCreateCallback(bind(&TaskInfoMonitor::dataChange, this, _1, _2, _3));
    _zk.setDeleteCallback(bind(&TaskInfoMonitor::dataChange, this, _1, _2, _3));
    if (!_zk.open()) {
        AUTIL_LOG(ERROR, "TaskInfoMonitor start failed. path[%s]", _path.c_str());
        _zk.setConnCallback(0);
        _zk.setCreateCallback(0);
        _zk.setDataCallback(0);
        _zk.setDeleteCallback(0);
        return false;
    }
    { // fill task at once, before connect notify
        autil::ScopedLock lock(_mutex);
        fillTaskInfo();
    }
    AUTIL_LOG(INFO, "Succesful to start TaskInfo Monitor");
    return true;
}

void TaskInfoMonitor::stop() {
    _isStopped = true;
    _zk.setConnCallback(0);
    _zk.setCreateCallback(0);
    _zk.setDataCallback(0);
    _zk.setDeleteCallback(0);
    usleep(50 * 1000);
    while (0 != _mutex.trylock()) {
        AUTIL_LOG(INFO, "mutex still holded in callback.");
        usleep(100 * 1000);
    }
    _zk.close();
    AUTIL_LOG(INFO, "TaskInfoMonitor stop");
    _mutex.unlock();
}

bool TaskInfoMonitor::getData(const std::string &path, DispatchInfo &msg) {
    std::string value;
    while (true) {
        ZOO_ERRORS ret = _zk.getData(path, value, true);
        if (ZOK == ret && !value.empty()) {
            return msg.ParseFromString(value);
        } else if (ZNONODE == ret) {
            AUTIL_LOG(WARN, "task path[%s] is not exist.", path.c_str());
            bool exist;
            // file not exist, set watcher.
            if (!_zk.check(_path, exist, true)) {
                // zookeeper error
                break;
            }
            if (!exist) {
                return true;
            }
        } else { // zookeeper error
            AUTIL_LOG(WARN, "task path[%s], value [%s].", path.c_str(), value.c_str());
            break;
        }
    }
    AUTIL_LOG(ERROR, "get task path[%s] failed.", path.c_str());
    return false;
}

void TaskInfoMonitor::disconnectNotify(ZkWrapper *zk, const string &path, ZkWrapper::ZkStatus status) {
    (void)zk;
    (void)path;
    AUTIL_LOG(INFO, "In DisconnectNotify");
    if (_isStopped) {
        AUTIL_LOG(WARN, "TaskInfoMonitor is stopped.");
        return;
    }
    autil::ScopedLock lock(_mutex);
    if (ZkWrapper::ZK_CONNECTED == status) {
        AUTIL_LOG(INFO, "zookeeper connected, fill task info.");
        fillTaskInfo();
    } else if (ZkWrapper::ZK_CONNECTING == status) {
        AUTIL_LOG(WARN, "zookeeper connecting.");
        _status->setZkMonitorStatus(false, TimeUtility::currentTime());
    } else {
        AUTIL_LOG(WARN, "zookeeper connection disconnected. status [%d]", int(status));
        _status->setZkMonitorStatus(false, TimeUtility::currentTime());
    }
}

void TaskInfoMonitor::dataChange(ZkWrapper *zk, const std::string &path, ZkWrapper::ZkStatus status) {
    (void)zk;
    (void)path;
    (void)status;
    AUTIL_LOG(INFO, "In dataChange");
    if (_isStopped) {
        AUTIL_LOG(WARN, "TaskInfoMonitor is stopped.");
        return;
    }
    {
        autil::ScopedLock lock(_mutex);
        fillTaskInfo();
    }
}

void TaskInfoMonitor::fillTaskInfo() {
    DispatchInfo msg;
    if (getData(_path, msg)) {
        _status->setZkMonitorStatus(true, TimeUtility::currentTime());
        AUTIL_LOG(INFO,
                  "Received Control Command On Path [%s], task [count: %d, %s]",
                  _path.c_str(),
                  msg.taskinfos_size(),
                  ProtoUtil::getDispatchStr(msg).c_str());
        if (msg.has_sessionid() && _sessionId != msg.sessionid()) {
            AUTIL_LOG(INFO, "ignore obsolete task, sessionId expect[%ld] actual[%ld]", _sessionId, msg.sessionid());
            return;
        }
        // TODO check address, throw exception
        _statusUpdater->updateTaskInfo(msg);
    } else {
        _status->setZkMonitorStatus(false, TimeUtility::currentTime());
        AUTIL_LOG(ERROR, "Get Data Failed when data change.path[%s]", _path.c_str());
    }
}

void TaskInfoMonitor::setStatusUpdater(StatusUpdater *statusUpdater) { _statusUpdater = statusUpdater; }

} // namespace heartbeat
} // namespace swift
