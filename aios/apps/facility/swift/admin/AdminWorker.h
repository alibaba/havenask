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

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "swift/heartbeat/HeartbeatReporter.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "worker_framework/LeaderedWorkerBase.h"

namespace swift {
namespace heartbeat {
class HeartbeatServiceImpl;
} // namespace heartbeat
} // namespace swift

namespace swift {
namespace monitor {
class AdminMetricsReporter;
} // namespace monitor

namespace config {
class AdminConfig;
} // namespace config

namespace admin {
class AdminServiceImpl;
class SysController;

class AdminWorker : public worker_framework::LeaderedWorkerBase {
public:
    AdminWorker();
    ~AdminWorker();

private:
    AdminWorker(const AdminWorker &);
    AdminWorker &operator=(const AdminWorker &);

public:
    bool initMonitor() override;
    bool doInit() override;
    bool doStart() override;
    void doInitOptions() override;
    void doExtractOptions() override;
    void doStop() override;
    void doIdle() override;

protected:
    void becomeLeader() override;
    void noLongerLeader() override;

    // heartbeat
private:
    std::string getAccessKey();
    std::string getHeartbeatAddress();
    std::string getWorkerAddress();
    protocol::HeartbeatInfo getHeartbeatInfo();
    bool initHeartbeat();
    void heartbeat();
    void amonReport();
    bool initSysController();
    bool initAdminService();
    bool initHeartbeatService();
    void initEnvArgs();

private:
    static const int64_t AMON_REPORT_INTERVAL;

private:
    std::string _configPath;
    config::AdminConfig *_adminConfig = nullptr;
    SysController *_sysController = nullptr;
    AdminServiceImpl *_adminServiceImpl = nullptr;
    heartbeat::HeartbeatServiceImpl *_heartbeatServiceImpl = nullptr;
    heartbeat::HeartbeatReporter _heartbeatReporter;
    autil::LoopThreadPtr _amonitorThreadPtr;
    monitor::AdminMetricsReporter *_adminMetricsReporter = nullptr;
    autil::ThreadMutex _exitMutex;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace admin
} // namespace swift
