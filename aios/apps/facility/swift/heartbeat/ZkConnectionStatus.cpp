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
#include "swift/heartbeat/ZkConnectionStatus.h"

namespace swift {
namespace heartbeat {
AUTIL_LOG_SETUP(swift, ZkConnectionStatus);

const int64_t ZkConnectionStatus::MAX_SERVING_TIME_AFTER_LOST = 500 * 1000;

ZkConnectionStatus::ZkConnectionStatus() {
    _heartbeatAlive = true;
    _zkMonitorAlive = true;
    _heartbeatLostTimestamp = -1;
    _zkMonitorLostTimestamp = -1;
}

ZkConnectionStatus::~ZkConnectionStatus() {}

void ZkConnectionStatus::setHeartbeatStatus(bool alive, int64_t timestamp) {
    autil::ScopedWriteLock lock(_lock);
    if (alive == _heartbeatAlive) {
        return;
    }
    if (alive) {
        _heartbeatAlive = true;
        _heartbeatLostTimestamp = -1;
    } else {
        _heartbeatAlive = false;
        _heartbeatLostTimestamp = timestamp;
    }
}

void ZkConnectionStatus::setZkMonitorStatus(bool alive, int64_t timestamp) {
    autil::ScopedWriteLock lock(_lock);
    if (alive == _zkMonitorAlive) {
        return;
    }
    if (alive) {
        _zkMonitorAlive = true;
        _zkMonitorLostTimestamp = -1;
    } else {
        _zkMonitorAlive = false;
        _zkMonitorLostTimestamp = timestamp;
    }
}

bool ZkConnectionStatus::isZkMonitorAlive() const {
    autil::ScopedWriteLock lock(_lock);
    return _zkMonitorAlive;
}

bool ZkConnectionStatus::isHeartbeatAlive() const {
    autil::ScopedWriteLock lock(_lock);
    return _heartbeatAlive;
}

bool ZkConnectionStatus::needRejectServing(int64_t timestamp) const {
    int64_t rejectTime;
    return rejectStatus(timestamp, rejectTime);
}

bool ZkConnectionStatus::rejectStatus(int64_t timestamp, int64_t &rejectTime) const {
    autil::ScopedReadLock lock(_lock);
    if (!_heartbeatAlive && timestamp - _heartbeatLostTimestamp > MAX_SERVING_TIME_AFTER_LOST) {
        AUTIL_LOG(
            WARN, "need reject serving, heartbeat lost time[%ldms]", (timestamp - _heartbeatLostTimestamp) / 1000);
        rejectTime = timestamp - _heartbeatLostTimestamp;
        return true;
    }
    if (!_zkMonitorAlive && timestamp - _zkMonitorLostTimestamp > MAX_SERVING_TIME_AFTER_LOST) {
        AUTIL_LOG(
            WARN, "need reject serving, zkMonitor lost time [%ldms]", (timestamp - _zkMonitorLostTimestamp) / 1000);
        rejectTime = timestamp - _zkMonitorLostTimestamp;
        return true;
    }
    return false;
}

} // namespace heartbeat
} // namespace swift
