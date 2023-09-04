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
#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace protocol {
class DispatchInfo;
} // namespace protocol
} // namespace swift

namespace swift {
namespace heartbeat {
class ZkConnectionStatus;
class StatusUpdater;

class TaskInfoMonitor {
public:
    TaskInfoMonitor();
    virtual ~TaskInfoMonitor();

private:
    TaskInfoMonitor(const TaskInfoMonitor &);
    TaskInfoMonitor &operator=(const TaskInfoMonitor &);

public:
    bool start();
    void stop();
    bool init(const std::string &address, int64_t sessionId = 0,
              uint32_t timeout = 10000); // ms
    void setZkConnectionStatus(ZkConnectionStatus *status) { _status = status; }
    void setStatusUpdater(StatusUpdater *statusUpdater);

public:
    bool isBad() { return _zk.isBad(); }

public:
    // zookeeper call back
    void dataChange(cm_basic::ZkWrapper *zk, const std::string &path, cm_basic::ZkWrapper::ZkStatus status);
    void disconnectNotify(cm_basic::ZkWrapper *zk, const std::string &path, cm_basic::ZkWrapper::ZkStatus status);

private:
    bool extractParameter(const std::string &address, std::string &host, std::string &path);
    // virtual for unit test
    virtual bool getData(const std::string &path, protocol::DispatchInfo &msg);
    void fillTaskInfo();

private:
    cm_basic::ZkWrapper _zk;
    autil::ThreadMutex _mutex;
    std::string _path;
    ZkConnectionStatus *_status;
    StatusUpdater *_statusUpdater;
    volatile bool _isStopped;
    int64_t _sessionId;

private:
    friend class TaskInfoMonitorTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(TaskInfoMonitor);

} // namespace heartbeat
} // namespace swift
