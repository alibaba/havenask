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

#include <stdint.h>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/common/Common.h"

namespace swift {
namespace heartbeat {

class ZkConnectionStatus {
public:
    ZkConnectionStatus();
    ~ZkConnectionStatus();

public:
    void setHeartbeatStatus(bool alive, int64_t timestamp = autil::TimeUtility::currentTime());
    void setZkMonitorStatus(bool alive, int64_t timestamp = autil::TimeUtility::currentTime());

    bool needRejectServing(int64_t timestamp = autil::TimeUtility::currentTime()) const;
    bool rejectStatus(int64_t timestamp, int64_t &rejectTime) const;
    bool isZkMonitorAlive() const;
    bool isHeartbeatAlive() const;

private:
    ZkConnectionStatus(const ZkConnectionStatus &);
    ZkConnectionStatus &operator=(const ZkConnectionStatus &);

public:
private:
    bool _heartbeatAlive;
    bool _zkMonitorAlive;
    int64_t _heartbeatLostTimestamp;
    int64_t _zkMonitorLostTimestamp;
    mutable autil::ReadWriteLock _lock;
    static const int64_t MAX_SERVING_TIME_AFTER_LOST;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ZkConnectionStatus);

} // namespace heartbeat
} // namespace swift
