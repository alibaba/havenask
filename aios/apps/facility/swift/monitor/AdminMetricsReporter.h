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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "swift/common/Common.h"
#include "swift/monitor/MetricsCommon.h"

namespace autil {
class Bitmap;
} // namespace autil

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

namespace swift {
namespace monitor {

enum BROKER_ERROR_TYPE {
    ERROR_TYPE_ZOMBIE = 0,
    ERROR_TYPE_TIMEOUT = 1,
};
struct RpcCollector {
    bool isTimeout = false;
    bool reject = false;
    int32_t latency = -1;
};

#define RPC_METRICS_TIMEOUT(method)                                                                                    \
    class method##Metrics : public kmonitor::MetricsGroup {                                                            \
    public:                                                                                                            \
        bool init(kmonitor::MetricsGroupManager *manager) override {                                                   \
            SWIFT_REGISTER_QPS_METRIC(method##Qps, ADMIN_GROUP_METRIC);                                                \
            SWIFT_REGISTER_QPS_METRIC(method##TimeoutQps, ADMIN_GROUP_METRIC);                                         \
            SWIFT_REGISTER_LATENCY_METRIC(method##Latency, ADMIN_GROUP_METRIC);                                        \
            SWIFT_REGISTER_QPS_METRIC(method##RejectQps, ADMIN_GROUP_METRIC);                                          \
            return true;                                                                                               \
        }                                                                                                              \
                                                                                                                       \
        void report(const kmonitor::MetricsTags *tags, RpcCollector *collector) {                                      \
            REPORT_MUTABLE_QPS(method##Qps);                                                                           \
            if (collector->isTimeout) {                                                                                \
                REPORT_MUTABLE_QPS(method##TimeoutQps);                                                                \
            }                                                                                                          \
            if (collector->reject) {                                                                                   \
                REPORT_MUTABLE_QPS(method##RejectQps);                                                                 \
            }                                                                                                          \
            SWIFT_REPORT_MUTABLE_METRIC(method##Latency, collector->latency);                                          \
        }                                                                                                              \
                                                                                                                       \
    private:                                                                                                           \
        kmonitor::MutableMetric *method##Qps = nullptr;                                                                \
        kmonitor::MutableMetric *method##TimeoutQps = nullptr;                                                         \
        kmonitor::MutableMetric *method##Latency = nullptr;                                                            \
        kmonitor::MutableMetric *method##RejectQps = nullptr;                                                          \
    };

RPC_METRICS_TIMEOUT(createTopic);
RPC_METRICS_TIMEOUT(createTopicBatch);
RPC_METRICS_TIMEOUT(modifyTopic);
RPC_METRICS_TIMEOUT(deleteTopic);
RPC_METRICS_TIMEOUT(deleteTopicBatch);
RPC_METRICS_TIMEOUT(getSysInfo);
RPC_METRICS_TIMEOUT(getTopicInfo);
RPC_METRICS_TIMEOUT(getAllTopicInfo);
RPC_METRICS_TIMEOUT(getAllTopicSimpleInfo);
RPC_METRICS_TIMEOUT(getAllTopicName);
RPC_METRICS_TIMEOUT(getPartitionInfo);
RPC_METRICS_TIMEOUT(getRoleAddress);
RPC_METRICS_TIMEOUT(getLeaderInfo);
RPC_METRICS_TIMEOUT(getAllWorkerStatus);
RPC_METRICS_TIMEOUT(getWorkerError);
RPC_METRICS_TIMEOUT(getPartitionError);
RPC_METRICS_TIMEOUT(updateBrokerConfig);
RPC_METRICS_TIMEOUT(rollbackBrokerConfig);
RPC_METRICS_TIMEOUT(transferPartition);
RPC_METRICS_TIMEOUT(changeSlot);
RPC_METRICS_TIMEOUT(registerSchema);
RPC_METRICS_TIMEOUT(getSchema);
RPC_METRICS_TIMEOUT(reportBrokerStatus);
RPC_METRICS_TIMEOUT(getBrokerStatus);
RPC_METRICS_TIMEOUT(getTopicRWTime);
RPC_METRICS_TIMEOUT(reportMissTopic);
RPC_METRICS_TIMEOUT(getLastDeletedNoUseTopic);
RPC_METRICS_TIMEOUT(getDeletedNoUseTopic);
RPC_METRICS_TIMEOUT(getDeletedNoUseTopicFiles);
RPC_METRICS_TIMEOUT(turnToMaster);
RPC_METRICS_TIMEOUT(turnToSlave);
RPC_METRICS_TIMEOUT(getMasterInfo);
RPC_METRICS_TIMEOUT(updateWriterVersion);
RPC_METRICS_TIMEOUT(topicAclManage);

struct SysControlMetricsCollector {
    uint32_t workerCount = 0;
    uint32_t aliveWorkerCount = 0;
    double aliveWorkerPercent = 0;
    double resourceUsedPercent = 0;
    uint32_t deadWorkerCount = 0;
    uint32_t unknownWorkerCount = 0;

    uint32_t adminCount = 0;
    uint32_t aliveAdminCount = 0;
    double aliveAdminPercent = 0;
    uint32_t deadAdminCount = 0;
    uint32_t unknownAdminCount = 0;

    uint32_t topicCount = 0;
    uint32_t partialRunningTopicCount = 0;
    uint32_t runningTopicCount = 0;
    double runningTopicPercent = 0;
    uint32_t waitingTopicCount = 0;

    uint32_t partitionCount = 0;
    uint32_t runningPartitionCount = 0;
    double runningPartitionPercent = 0;
    uint32_t waitingPartitionCount = 0;
    uint32_t loadingPartitionCount = 0;

    uint32_t isLeader = 0;
    uint32_t isMaster = 0;

    uint32_t enableMergeTopicCount = 0;
    uint32_t enableMergePartitionCount = 0;
};

class SysControlMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, SysControlMetricsCollector *collector);

private:
    kmonitor::MutableMetric *workerCount = nullptr;
    kmonitor::MutableMetric *aliveWorkerCount = nullptr;
    kmonitor::MutableMetric *aliveWorkerPercent = nullptr;
    kmonitor::MutableMetric *resourceUsedPercent = nullptr;
    kmonitor::MutableMetric *deadWorkerCount = nullptr;
    kmonitor::MutableMetric *unknownWorkerCount = nullptr;

    kmonitor::MutableMetric *adminCount = nullptr;
    kmonitor::MutableMetric *aliveAdminCount = nullptr;
    kmonitor::MutableMetric *aliveAdminPercent = nullptr;
    kmonitor::MutableMetric *deadAdminCount = nullptr;
    kmonitor::MutableMetric *unknownAdminCount = nullptr;

    kmonitor::MutableMetric *topicCount = nullptr;
    kmonitor::MutableMetric *partialRunningTopicCount = nullptr;
    kmonitor::MutableMetric *runningTopicCount = nullptr;
    kmonitor::MutableMetric *runningTopicPercent = nullptr;
    kmonitor::MutableMetric *waitingTopicCount = nullptr;

    kmonitor::MutableMetric *partitionCount = nullptr;
    kmonitor::MutableMetric *runningPartitionCount = nullptr;
    kmonitor::MutableMetric *runningPartitionPercent = nullptr;
    kmonitor::MutableMetric *waitingPartitionCount = nullptr;
    kmonitor::MutableMetric *loadingPartitionCount = nullptr;

    kmonitor::MutableMetric *isLeader = nullptr;
    kmonitor::MutableMetric *isMaster = nullptr;

    kmonitor::MutableMetric *enableMergeTopicCount = nullptr;
    kmonitor::MutableMetric *enableMergePartitionCount = nullptr;
};

class AdminMetricsReporter {
public:
    AdminMetricsReporter();
    ~AdminMetricsReporter();

private:
    AdminMetricsReporter(const AdminMetricsReporter &);
    AdminMetricsReporter &operator=(const AdminMetricsReporter &);

public:
    void reportScheduleTime(int64_t time);
    void reportWorkerCount(int count);
    void incLoadTopicFailed();
    void incLoadSchemaFailed();
    void incTopicNotExistQps(const std::string &topicName);
    void incDealErrorBrokersQps(const std::string &address, BROKER_ERROR_TYPE type);
    void reportAdminQueueSize(size_t queueLen);

    void reportcreateTopicMetrics(RpcCollector &collector);
    void reportcreateTopicBatchMetrics(RpcCollector &collector);
    void reportmodifyTopicMetrics(RpcCollector &collector);
    void reportdeleteTopicMetrics(RpcCollector &collector);
    void reportdeleteTopicBatchMetrics(RpcCollector &collector);
    void reportgetSysInfoMetrics(RpcCollector &collector);
    void reportgetTopicInfoMetrics(RpcCollector &collector);
    void reportgetAllTopicInfoMetrics(RpcCollector &collector);
    void reportgetAllTopicSimpleInfoMetrics(RpcCollector &collector);
    void reportgetAllTopicNameMetrics(RpcCollector &collector);
    void reportgetPartitionInfoMetrics(RpcCollector &collector);
    void reportgetRoleAddressMetrics(RpcCollector &collector);
    void reportgetLeaderInfoMetrics(RpcCollector &collector);
    void reportgetAllWorkerStatusMetrics(RpcCollector &collector);
    void reportgetWorkerErrorMetrics(RpcCollector &collector);
    void reportgetPartitionErrorMetrics(RpcCollector &collector);
    void reportupdateBrokerConfigMetrics(RpcCollector &collector);
    void reportrollbackBrokerConfigMetrics(RpcCollector &collector);
    void reporttransferPartitionMetrics(RpcCollector &collector);
    void reportchangeSlotMetrics(RpcCollector &collector);
    void reportregisterSchemaMetrics(RpcCollector &collector);
    void reportgetSchemaMetrics(RpcCollector &collector);
    void reportreportBrokerStatusMetrics(RpcCollector &collector);
    void reportgetBrokerStatusMetrics(RpcCollector &collector);
    void reportgetTopicRWTimeMetrics(RpcCollector &collector);
    void reportgetLastDeletedNoUseTopicMetrics(RpcCollector &collector);
    void reportgetDeletedNoUseTopicMetrics(RpcCollector &collector);
    void reportgetDeletedNoUseTopicFilesMetrics(RpcCollector &collector);
    void reportreportMissTopicMetrics(RpcCollector &collector);
    void reportSysControlMetrics(SysControlMetricsCollector &collector);
    void reportErrorBrokers(const std::vector<std::string> &zombieWorkers,
                            const std::vector<std::string> &timeoutWorkers);
    void reportPartitionResource(const std::vector<std::pair<std::string, uint32_t>> &resourceVec);
    void reportturnToMasterMetrics(RpcCollector &collector);
    void reportturnToSlaveMetrics(RpcCollector &collector);
    void reportgetMasterInfoMetrics(RpcCollector &collector);
    void reportMergeMetrics(const std::unordered_map<std::string, autil::Bitmap> &topicPartMap);
    void reportupdateWriterVersionMetrics(RpcCollector &collector);
    void reporttopicAclManageMetrics(RpcCollector &collector);

private:
    kmonitor::MetricsReporter *_metricsReporter = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(AdminMetricsReporter);

} // namespace monitor
} // namespace swift
