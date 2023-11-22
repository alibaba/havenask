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

#include <atomic>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>

#include "build_service/common/ResourceContainer.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/worker/RpcWorkerHeartbeat.h"
#include "build_service/worker/WorkerHeartbeatExecutor.h"
#include "build_service/worker/WorkerStateHandler.h"
#include "kmonitor_adapter/Monitor.h"
#include "swift/client/SwiftClient.h"
#include "worker_framework/LeaderedWorkerBase.h"

namespace build_service { namespace worker {

class ServiceWorker : public worker_framework::LeaderedWorkerBase
{
public:
    ServiceWorker();
    ~ServiceWorker();

private:
    ServiceWorker(const ServiceWorker&);
    ServiceWorker& operator=(const ServiceWorker&);

protected:
    void doInitOptions() override;
    bool doInit() override;
    bool doStart() override;
    void doStop() override;
    void doIdle() override;
    bool needSlotIdInLeaderInfo() const override;

    void becomeLeader() override {};
    void noLongerLeader() override;

private:
    void initBeeper();
    virtual WorkerHeartbeatExecutor* createServiceImpl(const proto::PartitionId& pid, const std::string& workerZkRoot,
                                                       const std::string& appZkRoot,
                                                       const std::string& adminServiceName, const std::string& epochId);
    bool startMonitor(const proto::PartitionId& pid);
    void exit(const std::string& message);
    void fillAddr();
    void releaseLeader();

    void fillProcArgs(std::map<std::string, std::string>& argMap);
    template <typename T>
    void fillTargetArgument(const std::string& optName, const std::string& key,
                            std::map<std::string, std::string>& argMap);

    bool extractPartitionId(const std::string& roleName, const std::string& adminServiceName,
                            const std::string& serviceName, proto::PartitionId& pid) const;

private:
    int64_t _hippoDeclareTime;
    std::string _roleId;
    proto::PartitionId _partitionId;
    proto::LongAddress _addr;
    std::unique_ptr<WorkerHeartbeatExecutor> _workerHeartbeatExecutor;
    kmonitor_adapter::Monitor* _monitor;
    kmonitor::MetricsReporterPtr _reporter;
    indexlib::util::MetricProviderPtr _metricProvider;
    indexlib::util::MetricPtr _hippoStartProcInterval;
    indexlib::util::MetricPtr _waitToBeLeaderInterval;
    std::unique_ptr<RpcWorkerHeartbeat> _rpcHeartbeat;
    std::atomic<bool> _alreadyReleasedLeader;

private:
    static const std::string AMONITOR_AGENT_PORT;
    static const std::string ADMIN_SERVICE_NAME;
    static const std::string AMONITOR_SERVICE_NAME;
    static const std::string HEARTBEAT_TYPE;
    static const std::string RPC_HEARTBEAT;
    static const std::string LEADER_LEASE_TIMEOUT;
    static const std::string LEADER_SYNC_INTERVAL;
    static const std::string BEEPER_CONFIG_PATH;
    static const std::string HIPPO_ENV_APP_DECLARE_TIMESTAMP;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::worker
