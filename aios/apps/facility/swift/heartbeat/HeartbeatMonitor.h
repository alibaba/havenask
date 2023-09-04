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

#include <functional>
#include <map>
#include <stdint.h>
#include <string>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "swift/common/Common.h"
#include "swift/config/ConfigDefine.h"
#include "swift/heartbeat/ZkHeartbeatWrapper.h"
#include "swift/protocol/Heartbeat.pb.h"

namespace swift {
namespace heartbeat {

class HeartbeatMonitor {
public:
    typedef std::function<void(const protocol::HeartbeatInfo &)> HandlerFuncType;
    typedef std::function<void(cm_basic::ZkWrapper::ZkStatus)> StatusChangeHandler;

public:
    HeartbeatMonitor();
    ~HeartbeatMonitor();

private:
    HeartbeatMonitor(const HeartbeatMonitor &);
    HeartbeatMonitor &operator=(const HeartbeatMonitor &);

public:
    bool setParameter(const std::string &address, uint32_t timeout = 10000); // ms
    void setParameter(const std::string &host, const std::string &path, unsigned int timeout = 10000);
    bool start(int64_t syncInterval = config::DEFAULT_SYNC_HEARTBEAT_INTERVAL);
    void stop();
    void setHandler(const HandlerFuncType &handler);
    void setStatusChangeHandler(const StatusChangeHandler &handler);
    bool isBad();

public:
    // used by callback only
    void dataChange(cm_basic::ZkWrapper *zk, const std::string &path, cm_basic::ZkWrapper::ZkStatus status);
    void childChange(cm_basic::ZkWrapper *zk, const std::string &path, cm_basic::ZkWrapper::ZkStatus status);
    void existChange(cm_basic::ZkWrapper *zk, const std::string &path, cm_basic::ZkWrapper::ZkStatus status);
    void disconnectNotify(cm_basic::ZkWrapper *zk, const std::string &path, cm_basic::ZkWrapper::ZkStatus status);

private:
    void diffChildren(bool updateAll);
    bool checkAndInsert(const std::string &path, const protocol::HeartbeatInfo &msg);
    bool extractParameter(const std::string &address, std::string &host, std::string &path);
    void onStatusChange(cm_basic::ZkWrapper::ZkStatus status);
    void setNodeDisappear(const std::string &path);
    void syncHeartbeat();

private:
    HandlerFuncType _handler;
    StatusChangeHandler _statusChangeHandler;
    std::map<std::string, protocol::HeartbeatInfo> _localWorkerImage;
    autil::ThreadMutex _mutex;
    ZkHeartbeatWrapper _zk;
    std::string _path;
    volatile bool _isConnected;
    autil::LoopThreadPtr _syncHeartbeatThreadPtr;

private:
    AUTIL_LOG_DECLARE();
    friend class HeartbeatMonitorTest;
};

SWIFT_TYPEDEF_PTR(HeartbeatMonitor);

} // namespace heartbeat
} // namespace swift
