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
#include <string>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/heartbeat/ZkHeartbeatWrapper.h"
#include "swift/protocol/Heartbeat.pb.h"

namespace swift {
namespace heartbeat {

class HeartbeatReporter {
public:
    HeartbeatReporter();
    virtual ~HeartbeatReporter();

private:
    HeartbeatReporter(const HeartbeatReporter &);
    HeartbeatReporter &operator=(const HeartbeatReporter &);

public:
    bool setParameter(const std::string &address,
                      uint32_t timeout = 10000); // ms
    void
    setParameter(const std::string &host, const std::string &path, const std::string &name, uint32_t timeout = 10000);
    void shutdown();
    virtual bool heartBeat(const protocol::HeartbeatInfo &msg, bool force = false); // virtual for test
    bool isBad() const;

    void existChange(cm_basic::ZkWrapper *zk, const std::string &path, cm_basic::ZkWrapper::ZkStatus status);

private:
    bool extractParameter(const std::string &address, std::string &host, std::string &path, std::string &name);

private:
    volatile bool _bChanged;
    bool _firstHeartbeat;
    protocol::HeartbeatInfo _localImage;
    ZkHeartbeatWrapper _zk;
    std::string _path;
    std::string _name;
    int64_t _lastReportTime;

    friend class HeartbeatReporterTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(HeartbeatReporter);

} // namespace heartbeat
} // namespace swift
