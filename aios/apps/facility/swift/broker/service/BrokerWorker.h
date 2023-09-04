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

#include <memory>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "swift/broker/service/BrokerHealthChecker.h"
#include "swift/common/Common.h"
#include "swift/heartbeat/HeartbeatReporter.h"
#include "swift/heartbeat/ZkConnectionStatus.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "worker_framework/LeaderedWorkerBase.h"

namespace autil {
namespace metric {
class Cpu;
class ProcessMemory;
} // namespace metric
} // namespace autil

namespace swift {
namespace util {
class IpMapper;
} // namespace util

namespace config {
class BrokerConfig;
} // namespace config
namespace monitor {
class BrokerMetricsReporter;
} // namespace monitor
namespace heartbeat {
class StatusUpdater;
class TaskInfoMonitor;
} // namespace heartbeat
namespace service {
class TopicPartitionSupervisor;
class TransporterImpl;
} // namespace service

namespace network {

class SwiftRpcChannelManager;

typedef std::shared_ptr<SwiftRpcChannelManager> SwiftRpcChannelManagerPtr;
class SwiftAdminAdapter;

typedef std::shared_ptr<SwiftAdminAdapter> SwiftAdminAdapterPtr;
} // namespace network
namespace monitor {
struct BrokerInStatusCollector;
} // namespace monitor

namespace service {

class BrokerWorker : public worker_framework::LeaderedWorkerBase {
public:
    BrokerWorker();
    ~BrokerWorker();

private:
    BrokerWorker(const BrokerWorker &);
    BrokerWorker &operator=(const BrokerWorker &);

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

private:
    void checkMonitor();
    void amonReport();
    bool loadConfig();
    void reportWorkerInMetric();
    void initFileSystem(const std::string &dfsRoot);
    bool initAdminClient();
    bool initIpMapper();
    // heartbeat
private:
    std::string getAccessKey();
    std::string getHeartbeatAddress();
    std::string getWorkerAddress();
    protocol::HeartbeatInfo getHeartbeatInfo();
    bool initHeartbeat();
    void heartbeat();
    void convertBrokerInMetric2Status(const protocol::BrokerInMetric &metric,
                                      monitor::BrokerInStatusCollector &collector);

private:
    static const int64_t CHECK_MONITOR_INTERVAL;
    static const int64_t AMON_REPORT_INTERVAL;
    static const int64_t STATUS_REPORT_INTERVAL;
    const static uint32_t LEADER_RETRY_TIME_INTERVAL;
    const static uint32_t LEADER_RETRY_COUNT;

    const static std::string CONFIG;
    const static std::string ROLE;
    static const std::string DEFAULT_LEADER_LEASE_TIME;
    static const std::string DEFAULT_LEADER_LOOP_INTERVAL;
    static const std::string PORT_FILE;

private:
    TransporterImpl *_transporterImpl;
    TopicPartitionSupervisor *_tpSupervisor;
    heartbeat::StatusUpdater *_statusUpdater;
    config::BrokerConfig *_brokerConfig;
    util::IpMapper *_ipMapper;
    heartbeat::TaskInfoMonitor *_taskInfoMonitor;
    autil::LoopThreadPtr _checkMonitorThreadPtr;
    heartbeat::ZkConnectionStatus _status;
    autil::LoopThreadPtr _amonitorThreadPtr;
    monitor::BrokerMetricsReporter *_metricsReporter;
    std::string _configPath;
    std::string _roleName;
    heartbeat::HeartbeatReporter _heartbeatReporter;
    protocol::BrokerVersionInfo _brokerVersionInfo;
    autil::ThreadMutex _exitMutex;
    std::string _initMonitorLog;
    int64_t _sessionId;
    autil::metric::Cpu *_cpu;
    autil::metric::ProcessMemory *_mem;
    autil::LoopThreadPtr _statusReportThreadPtr;
    network::SwiftRpcChannelManagerPtr _channelManager;
    network::SwiftAdminAdapterPtr _adminAdapter;
    BrokerHealthCheckerPtr _brokerHealthChecker;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(BrokerWorker);

} // namespace service
} // namespace swift
