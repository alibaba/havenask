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
#include "swift/heartbeat/HeartbeatMonitor.h"

#include <cstddef>
#include <functional>
#include <set>
#include <unistd.h>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageComparator.h"
#include "zookeeper/zookeeper.h"

using namespace std;
using namespace autil;
using namespace cm_basic;
using namespace swift::protocol;
namespace swift {
namespace heartbeat {
AUTIL_LOG_SETUP(swift, HeartbeatMonitor);

HeartbeatMonitor::HeartbeatMonitor() : _isConnected(false) {}

HeartbeatMonitor::~HeartbeatMonitor() {}

void HeartbeatMonitor::setHandler(const HandlerFuncType &handler) {
    ScopedLock lock(_mutex);
    _handler = handler;
}

void HeartbeatMonitor::setStatusChangeHandler(const StatusChangeHandler &handler) {
    ScopedLock lock(_mutex);
    _statusChangeHandler = handler;
}

bool HeartbeatMonitor::setParameter(const string &address, uint32_t timeout) {
    string host;
    string path;

    if (!extractParameter(address, host, path)) {
        AUTIL_LOG(ERROR, "invalid heartBeat address [%s]", address.c_str());
        return false;
    }

    setParameter(host, path, timeout);
    return true;
}

bool HeartbeatMonitor::extractParameter(const string &address, string &host, string &path) {
    const static string ZK_PREFIX = "zfs://";
    if (!StringUtil::startsWith(address, ZK_PREFIX)) {
        AUTIL_LOG(ERROR, "heartBeat address not startsWith [zfs://]");
        return false;
    }

    string trimPrefixAddress = address.substr(ZK_PREFIX.size());
    size_t pos1 = trimPrefixAddress.find("/");
    if (pos1 == string::npos) {
        return false;
    }

    host = trimPrefixAddress.substr(0, pos1);
    path = trimPrefixAddress.substr(pos1);

    if (!path.empty() && *path.rbegin() == '/') {
        path.resize(path.size() - 1);
    }

    return !(host.empty() || path.empty());
}

void HeartbeatMonitor::setParameter(const string &host, const string &path, unsigned int timeout) {
    _zk.setParameter(host, timeout);
    _path = path;
}

bool HeartbeatMonitor::start(int64_t syncInterval) {
    using namespace placeholders;
    if (_zk.isConnected()) {
        AUTIL_LOG(DEBUG, "HeartbeatMonitor has not stop");
        return true;
    }
    _isConnected = false;
    _zk.setConnCallback(bind(&HeartbeatMonitor::disconnectNotify, this, _1, _2, _3));
    _zk.setChildCallback(bind(&HeartbeatMonitor::childChange, this, _1, _2, _3));
    _zk.setDataCallback(bind(&HeartbeatMonitor::dataChange, this, _1, _2, _3));
    _zk.setDeleteCallback(bind(&HeartbeatMonitor::existChange, this, _1, _2, _3));
    if (!_zk.open()) {
        AUTIL_LOG(ERROR, "HeartbeatMonitor start failed.path[%s]", _path.c_str());
        _zk.setConnCallback(0);
        _zk.setChildCallback(0);
        _zk.setDataCallback(0);
        _zk.setDeleteCallback(0);
        return false;
    }

    if (!_zk.createPath(_path)) {
        AUTIL_LOG(ERROR, "HeartbeatMonitor root path create failed. path[%s]", _path.c_str());
        stop();
        return false;
    }

    ScopedLock lock(_mutex);
    diffChildren(true);
    _syncHeartbeatThreadPtr = autil::LoopThread::createLoopThread(
        bind(&HeartbeatMonitor::syncHeartbeat, this), syncInterval, "sync_heartbeat");
    return true;
}

void HeartbeatMonitor::stop() {
    _syncHeartbeatThreadPtr.reset();
    _zk.setConnCallback(0);
    _zk.setChildCallback(0);
    _zk.setDataCallback(0);
    _zk.setDeleteCallback(0);
    usleep(50 * 1000);
    while (0 != _mutex.trylock()) {
        AUTIL_LOG(INFO, "mutex still holded in callback.");
        usleep(100 * 1000);
    }
    _zk.close();
    onStatusChange(ZkWrapper::ZK_BAD);
    _localWorkerImage.clear();
    _mutex.unlock();
}

void HeartbeatMonitor::dataChange(ZkWrapper *zk, const string &path, ZkWrapper::ZkStatus status) {
    (void)zk;
    // process modify
    AUTIL_LOG(INFO, "data change path [%s], status [%d]", path.c_str(), int(status));
    ScopedLock lock(_mutex);
    HeartbeatInfo msg;

    if (_zk.getHeartBeatInfo(path, msg, true)) {
        if (checkAndInsert(path, msg) && _handler) {
            _handler(msg);
        }
    } else {
        AUTIL_LOG(WARN, "Get Data Failed when data change.path[%s]", path.c_str());
        setNodeDisappear(path);
        return;
    }
}

void HeartbeatMonitor::existChange(ZkWrapper *zk, const string &path, ZkWrapper::ZkStatus status) {
    (void)zk;
    AUTIL_LOG(INFO, "exist change path [%s], status [%d]", path.c_str(), int(status));
    ScopedLock lock(_mutex);
    // invoked by zoo_getchild watcher
    if (path == _path) {
        return;
    }
    // invoked by zoo_get watcher
    bool exist = true;
    if (_zk.check(path, exist)) {
        if (exist) {
            // child node is deleted and then appeared
            // too fast that the two message is merged
            HeartbeatInfo msg;
            if (_zk.getHeartBeatInfo(path, msg, true)) {
                if (checkAndInsert(path, msg) && _handler) {
                    _handler(msg);
                }
                return;
            }
            AUTIL_LOG(WARN, "Get Data Failed when data change.path[%s]", path.c_str());
        }
        // child node is deleted
        setNodeDisappear(path);
    } else {
        AUTIL_LOG(ERROR, "Check Existence Failed when node delete.path[%s]", path.c_str());
        return;
    }
}

bool HeartbeatMonitor::checkAndInsert(const string &path, const HeartbeatInfo &msg) {
    if (_localWorkerImage.end() == _localWorkerImage.find(path)) {
        _localWorkerImage[path] = msg;
        return true;
    }
    HeartbeatInfo &localInfo = _localWorkerImage[path];
    if (msg.has_heartbeatid() && localInfo.has_heartbeatid() && msg.heartbeatid() < localInfo.heartbeatid()) {
        AUTIL_LOG(WARN,
                  "ignore obsolete heartbeat[%ld], current heartbeatId[%ld]",
                  msg.heartbeatid(),
                  localInfo.heartbeatid());
        return false;
    }
    if (localInfo != msg) {
        localInfo = msg;
        return true;
    }
    return false;
}

void HeartbeatMonitor::disconnectNotify(ZkWrapper *zk, const string &path, ZkWrapper::ZkStatus status) {
    (void)zk;
    AUTIL_LOG(INFO, "connect notify, path [%s], status [%d]", path.c_str(), int(status));
    ScopedLock lock(_mutex);
    if (ZkWrapper::ZK_CONNECTED == status) {
        if (_isConnected) {
            diffChildren(true);
        } else {
            onStatusChange(ZkWrapper::ZK_CONNECTED);
            _isConnected = true;
        }
    } else {
        _isConnected = false;
        onStatusChange(ZkWrapper::ZK_BAD);
    }
}

void HeartbeatMonitor::childChange(ZkWrapper *zk, const string &path, ZkWrapper::ZkStatus status) {
    (void)zk;
    AUTIL_LOG(INFO, "child change, path [%s], status [%d]", path.c_str(), int(status));
    ScopedLock lock(_mutex);
    diffChildren(false);
}

void HeartbeatMonitor::diffChildren(bool updateAll) {
    vector<string> children;

    if (ZOK != _zk.getChild(_path, children, true)) {
        AUTIL_LOG(ERROR, "Get Children failed when child changed.path[%s]", _path.c_str());
        return;
    }
    AUTIL_LOG(INFO, "Get children count [%d]", (int)children.size());

    set<string> nowChildren;
    for (vector<string>::iterator i = children.begin(); i != children.end(); ++i) {
        nowChildren.insert(_path + "/" + *i);
    }
    // node disappeared
    map<string, HeartbeatInfo>::iterator iter = _localWorkerImage.begin();
    for (; iter != _localWorkerImage.end();) {
        set<string>::iterator nowPos = nowChildren.find(iter->first);
        if (nowPos == nowChildren.end()) {
            HeartbeatInfo msg = iter->second;
            msg.set_alive(false);
            map<string, HeartbeatInfo>::iterator temp = iter++;
            _localWorkerImage.erase(temp);
            if (_handler) {
                _handler(msg);
            }
        } else {
            ++iter;
        }
    }
    // newly appeared
    for (set<string>::const_iterator iter = nowChildren.begin(); iter != nowChildren.end(); ++iter) {
        HeartbeatInfo msg;
        string nodePath = *iter;
        map<string, HeartbeatInfo>::iterator localiter = _localWorkerImage.find(nodePath);

        if (localiter == _localWorkerImage.end() || updateAll) {
            if (_zk.getHeartBeatInfo(nodePath, msg, true)) {
                if (checkAndInsert(nodePath, msg) && _handler) {
                    _handler(msg);
                }
            } else {
                AUTIL_LOG(WARN, "Get Data Failed while diffChildren. path[%s]", nodePath.c_str());
                setNodeDisappear(nodePath);
            }
        }
    }
}

void HeartbeatMonitor::onStatusChange(ZkWrapper::ZkStatus status) {
    AUTIL_LOG(INFO, "HeartbeatMonitor Status Change to %d.", status);
    if (_statusChangeHandler) {
        _statusChangeHandler(status);
    }
}

bool HeartbeatMonitor::isBad() {
    ScopedLock lock(_mutex);
    return _zk.isBad();
}

void HeartbeatMonitor::setNodeDisappear(const string &path) {
    AUTIL_LOG(INFO, "node disappear, path [%s].", path.c_str());
    map<string, HeartbeatInfo>::iterator iter = _localWorkerImage.find(path);
    if (iter != _localWorkerImage.end()) {
        HeartbeatInfo msg = iter->second;
        msg.set_alive(false);
        _localWorkerImage.erase(iter);
        if (_handler) {
            _handler(msg);
        }
    }
}

void HeartbeatMonitor::syncHeartbeat() {
    AUTIL_LOG(INFO, "begin sync heartbeat.");
    ScopedLock lock(_mutex);
    diffChildren(true);
    AUTIL_LOG(INFO, "end sync heartbeat.");
}

} // namespace heartbeat
} // namespace swift
