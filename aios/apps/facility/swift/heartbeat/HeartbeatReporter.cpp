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
#include "swift/heartbeat/HeartbeatReporter.h"

#include <cstddef>
#include <functional>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageComparator.h"
#include "swift/util/ProtoUtil.h"

using namespace std;
using namespace std::placeholders;
using namespace swift::protocol;
using namespace swift::util;
using namespace autil;

namespace swift {
namespace heartbeat {
AUTIL_LOG_SETUP(swift, HeartbeatReporter);

const int64_t FORCE_REPORT_INTERVAL_SEC = 300;
HeartbeatReporter::HeartbeatReporter() : _bChanged(true), _firstHeartbeat(true), _lastReportTime(0) {}

HeartbeatReporter::~HeartbeatReporter() {}

bool HeartbeatReporter::setParameter(const std::string &address, uint32_t timeout) {
    string host;
    string path;
    string name;
    if (!extractParameter(address, host, path, name)) {
        AUTIL_LOG(ERROR, "invalid heartBeat address [%s]", address.c_str());
        return false;
    }
    setParameter(host, path, name, timeout);
    return true;
}

bool HeartbeatReporter::extractParameter(const std::string &address,
                                         std::string &host,
                                         std::string &path,
                                         std::string &name) {
    const static string ZK_PREFIX = "zfs://";
    if (!autil::StringUtil::startsWith(address, ZK_PREFIX)) {
        AUTIL_LOG(ERROR, "heartBeat address not startsWith [zfs://]");
        return false;
    }
    string trimPrefixAddress = address.substr(ZK_PREFIX.size());
    size_t pos1 = trimPrefixAddress.find("/");
    size_t pos2 = trimPrefixAddress.rfind("/");
    if (pos1 == string::npos || pos2 == pos1) {
        return false;
    }
    host = trimPrefixAddress.substr(0, pos1);
    path = trimPrefixAddress.substr(pos1, pos2 - pos1);
    name = trimPrefixAddress.substr(pos2 + 1);
    return !(host.empty() || path.empty() || name.empty());
}

void HeartbeatReporter::setParameter(const string &host, const string &path, const string &name, unsigned int timeout) {
    AUTIL_LOG(INFO, "host [%s] path [%s] name [%s]", host.c_str(), path.c_str(), name.c_str());
    _firstHeartbeat = true;
    _zk.setParameter(host, timeout);
    _path = path;
    _name = name;
    // reconnect
    shutdown();
}

void HeartbeatReporter::shutdown() {
    _zk.setDeleteCallback(0);
    _zk.close();
    _bChanged = true;
    _localImage.Clear();
}

void HeartbeatReporter::existChange(cm_basic::ZkWrapper *zk,
                                    const std::string &path,
                                    cm_basic::ZkWrapper::ZkStatus status) {
    (void)zk;
    (void)path;
    (void)status;
    _bChanged = true;
    AUTIL_LOG(WARN, "heartbeat node disappeared!");
}

bool HeartbeatReporter::heartBeat(const HeartbeatInfo &msg, bool force) {
    AUTIL_LOG(DEBUG, "heartbeat");
    int64_t curTime = TimeUtility::currentTimeInSeconds();
    if (curTime - _lastReportTime > FORCE_REPORT_INTERVAL_SEC) {
        force = true;
    }
    if (!force && (_localImage == msg) && !_zk.isBad() && !_bChanged) {
        return true;
    }
    if (_zk.isConnecting()) {
        AUTIL_LOG(WARN, "heartbeat report delayed by connecting");
        return false;
    } else if (_zk.isBad()) {
        AUTIL_LOG(WARN, "heartbeat zk is bad reconnect.");
        _localImage.Clear();
        _bChanged = true;
        _zk.setDeleteCallback(std::bind(&HeartbeatReporter::existChange, this, _1, _2, _3));
        if (!_zk.open()) {
            AUTIL_LOG(WARN, "heartbeat report failed, can't open connection");
            _zk.setDeleteCallback(0);
            return false;
        } else {
            AUTIL_LOG(INFO, "heartbeat zk open success.");
        }
    }
    if (_firstHeartbeat) {
        bool exist = false;
        if (!_zk.check(_path + "/" + _name, exist, false)) {
            AUTIL_LOG(ERROR, "heart beat report check failed, can't check node");
            return false;
        }
        if (exist && (!_zk.remove(_path + "/" + _name))) {
            AUTIL_LOG(ERROR, "heart beat report failed, can't clear old environment");
            return false;
        }
        _firstHeartbeat = false;
    }
    _bChanged = false;
    if (!_zk.touch(_path + "/" + _name, msg)) {
        AUTIL_LOG(ERROR, "heart beat report failed, can't create node");
        _bChanged = true;
        return false;
    }
    bool exist = false;
    if (!_zk.check(_path + "/" + _name, exist, true)) {
        AUTIL_LOG(ERROR, "heart beat report check failed, can't check node");
        _bChanged = true;
        return false;
    }
    if (!exist) {
        AUTIL_LOG(WARN, "heart beat report, node disappear!");
        _bChanged = true;
        return false;
    }
    _localImage = msg;
    _lastReportTime = TimeUtility::currentTimeInSeconds();
    AUTIL_LOG(INFO, "send heartbeat [%s]", ProtoUtil::getHeartbeatStr(msg).c_str());
    return true;
}

bool HeartbeatReporter::isBad() const { return _zk.isBad(); }

} // namespace heartbeat
} // namespace swift
