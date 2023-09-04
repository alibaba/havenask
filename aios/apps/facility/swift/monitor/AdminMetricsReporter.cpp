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
#include "swift/monitor/AdminMetricsReporter.h"

#include <iosfwd>
#include <map>

#include "autil/CommonMacros.h"
#include "autil/bitmap.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"

using namespace kmonitor;
using namespace std;

namespace swift {
namespace monitor {
AUTIL_LOG_SETUP(swift, AdminMetricsReporter);

AdminMetricsReporter::AdminMetricsReporter() { _metricsReporter = new MetricsReporter("swift", "", {}); }

AdminMetricsReporter::~AdminMetricsReporter() { DELETE_AND_SET_NULL(_metricsReporter); }

bool SysControlMetrics::init(kmonitor::MetricsGroupManager *manager) {
    SWIFT_REGISTER_GAUGE_METRIC(workerCount, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(aliveWorkerCount, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(aliveWorkerPercent, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(resourceUsedPercent, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(deadWorkerCount, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(unknownWorkerCount, ADMIN_GROUP_METRIC);

    SWIFT_REGISTER_GAUGE_METRIC(adminCount, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(aliveAdminCount, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(aliveAdminPercent, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(deadAdminCount, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(unknownAdminCount, ADMIN_GROUP_METRIC);

    SWIFT_REGISTER_STATUS_METRIC(topicCount, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(runningTopicCount, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(runningTopicPercent, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(partialRunningTopicCount, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(waitingTopicCount, ADMIN_GROUP_METRIC);

    SWIFT_REGISTER_STATUS_METRIC(partitionCount, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(runningPartitionCount, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(runningPartitionPercent, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(waitingPartitionCount, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(loadingPartitionCount, ADMIN_GROUP_METRIC);

    SWIFT_REGISTER_STATUS_METRIC(isLeader, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(isMaster, ADMIN_GROUP_METRIC);

    SWIFT_REGISTER_STATUS_METRIC(enableMergeTopicCount, ADMIN_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(enableMergePartitionCount, ADMIN_GROUP_METRIC);
    return true;
}

void SysControlMetrics::report(const MetricsTags *tags, SysControlMetricsCollector *collector) {
    REPORT_MUTABLE_METRIC(workerCount, collector->workerCount);
    REPORT_MUTABLE_METRIC(aliveWorkerCount, collector->aliveWorkerCount);
    REPORT_MUTABLE_METRIC(aliveWorkerPercent, collector->aliveWorkerPercent);
    REPORT_MUTABLE_METRIC(resourceUsedPercent, collector->resourceUsedPercent);
    REPORT_MUTABLE_METRIC(deadWorkerCount, collector->deadWorkerCount);
    REPORT_MUTABLE_METRIC(unknownWorkerCount, collector->unknownWorkerCount);

    REPORT_MUTABLE_METRIC(adminCount, collector->adminCount);
    REPORT_MUTABLE_METRIC(aliveAdminCount, collector->aliveAdminCount);
    REPORT_MUTABLE_METRIC(aliveAdminPercent, collector->aliveAdminPercent);
    REPORT_MUTABLE_METRIC(deadAdminCount, collector->deadAdminCount);
    REPORT_MUTABLE_METRIC(unknownAdminCount, collector->unknownAdminCount);

    REPORT_MUTABLE_METRIC(topicCount, collector->topicCount);
    REPORT_MUTABLE_METRIC(partialRunningTopicCount, collector->partialRunningTopicCount);
    REPORT_MUTABLE_METRIC(runningTopicCount, collector->runningTopicCount);
    REPORT_MUTABLE_METRIC(runningTopicPercent, collector->runningTopicPercent);
    REPORT_MUTABLE_METRIC(waitingTopicCount, collector->waitingTopicCount);

    REPORT_MUTABLE_METRIC(partitionCount, collector->partitionCount);
    REPORT_MUTABLE_METRIC(runningPartitionCount, collector->runningPartitionCount);
    REPORT_MUTABLE_METRIC(runningPartitionPercent, collector->runningPartitionPercent);
    REPORT_MUTABLE_METRIC(waitingPartitionCount, collector->waitingPartitionCount);
    REPORT_MUTABLE_METRIC(loadingPartitionCount, collector->loadingPartitionCount);

    REPORT_MUTABLE_METRIC(isLeader, collector->isLeader);
    REPORT_MUTABLE_METRIC(isMaster, collector->isMaster);

    REPORT_MUTABLE_METRIC(enableMergeTopicCount, collector->enableMergeTopicCount);
    REPORT_MUTABLE_METRIC(enableMergePartitionCount, collector->enableMergePartitionCount);
}

void AdminMetricsReporter::reportcreateTopicMetrics(RpcCollector &collector) {
    _metricsReporter->report<createTopicMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportcreateTopicBatchMetrics(RpcCollector &collector) {
    _metricsReporter->report<createTopicBatchMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportmodifyTopicMetrics(RpcCollector &collector) {
    _metricsReporter->report<modifyTopicMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportdeleteTopicMetrics(RpcCollector &collector) {
    _metricsReporter->report<deleteTopicMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportdeleteTopicBatchMetrics(RpcCollector &collector) {
    _metricsReporter->report<deleteTopicBatchMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetSysInfoMetrics(RpcCollector &collector) {
    _metricsReporter->report<getSysInfoMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetTopicInfoMetrics(RpcCollector &collector) {
    _metricsReporter->report<getTopicInfoMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetAllTopicInfoMetrics(RpcCollector &collector) {
    _metricsReporter->report<getAllTopicInfoMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetAllTopicSimpleInfoMetrics(RpcCollector &collector) {
    _metricsReporter->report<getAllTopicSimpleInfoMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetAllTopicNameMetrics(RpcCollector &collector) {
    _metricsReporter->report<getAllTopicNameMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetPartitionInfoMetrics(RpcCollector &collector) {
    _metricsReporter->report<getPartitionInfoMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetRoleAddressMetrics(RpcCollector &collector) {
    _metricsReporter->report<getRoleAddressMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetLeaderInfoMetrics(RpcCollector &collector) {
    _metricsReporter->report<getLeaderInfoMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetAllWorkerStatusMetrics(RpcCollector &collector) {
    _metricsReporter->report<getAllWorkerStatusMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetWorkerErrorMetrics(RpcCollector &collector) {
    _metricsReporter->report<getWorkerErrorMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetPartitionErrorMetrics(RpcCollector &collector) {
    _metricsReporter->report<getPartitionErrorMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportupdateBrokerConfigMetrics(RpcCollector &collector) {
    _metricsReporter->report<updateBrokerConfigMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportrollbackBrokerConfigMetrics(RpcCollector &collector) {
    _metricsReporter->report<rollbackBrokerConfigMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reporttransferPartitionMetrics(RpcCollector &collector) {
    _metricsReporter->report<transferPartitionMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportchangeSlotMetrics(RpcCollector &collector) {
    _metricsReporter->report<changeSlotMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportregisterSchemaMetrics(RpcCollector &collector) {
    _metricsReporter->report<registerSchemaMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetSchemaMetrics(RpcCollector &collector) {
    _metricsReporter->report<getSchemaMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportreportBrokerStatusMetrics(RpcCollector &collector) {
    _metricsReporter->report<reportBrokerStatusMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetBrokerStatusMetrics(RpcCollector &collector) {
    _metricsReporter->report<getBrokerStatusMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetTopicRWTimeMetrics(RpcCollector &collector) {
    _metricsReporter->report<getTopicRWTimeMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetLastDeletedNoUseTopicMetrics(RpcCollector &collector) {
    _metricsReporter->report<getLastDeletedNoUseTopicMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetDeletedNoUseTopicMetrics(RpcCollector &collector) {
    _metricsReporter->report<getDeletedNoUseTopicMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetDeletedNoUseTopicFilesMetrics(RpcCollector &collector) {
    _metricsReporter->report<getDeletedNoUseTopicFilesMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportSysControlMetrics(SysControlMetricsCollector &collector) {
    _metricsReporter->report<SysControlMetrics, SysControlMetricsCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportreportMissTopicMetrics(RpcCollector &collector) {
    _metricsReporter->report<reportMissTopicMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportScheduleTime(int64_t time) {
    static const string name = ADMIN_GROUP_METRIC + "scheduleTime";
    REPORT_USER_MUTABLE_METRIC(_metricsReporter, name, time);
}

void AdminMetricsReporter::reportWorkerCount(int count) {
    static const string name = ADMIN_GROUP_METRIC + "workerCount";
    REPORT_USER_MUTABLE_METRIC(_metricsReporter, name, count);
}

void AdminMetricsReporter::incLoadTopicFailed() {
    static const string name = ADMIN_GROUP_METRIC + "loadTopicFailed";
    REPORT_USER_MUTABLE_QPS(_metricsReporter, name);
}

void AdminMetricsReporter::incLoadSchemaFailed() {
    static const string name = ADMIN_GROUP_METRIC + "loadSchemaFailed";
    REPORT_USER_MUTABLE_QPS(_metricsReporter, name);
}

void AdminMetricsReporter::incTopicNotExistQps(const string &topicName) {
    static const string name = ADMIN_GROUP_METRIC + "topicNotExistQps";
    MetricsTags tags("topic", topicName);
    REPORT_USER_MUTABLE_QPS_TAGS(_metricsReporter, name, &tags);
}
void AdminMetricsReporter::incDealErrorBrokersQps(const string &address, BROKER_ERROR_TYPE type) {
    static const string name = ADMIN_GROUP_METRIC + "dealErrorBrokersQps";
    MetricsTags tags("address", address);
    switch (type) {
    case ERROR_TYPE_ZOMBIE:
        tags.AddTag("type", "zombie");
        break;
    case ERROR_TYPE_TIMEOUT:
        tags.AddTag("type", "timeout");
        break;
    default:
        tags.AddTag("type", "unknown");
    }
    REPORT_USER_MUTABLE_QPS_TAGS(_metricsReporter, name, &tags);
}

void AdminMetricsReporter::reportAdminQueueSize(size_t queueLen) {
    static const string name = ADMIN_GROUP_METRIC + "adminQueueSize";
    REPORT_USER_MUTABLE_METRIC(_metricsReporter, name, queueLen);
}

void AdminMetricsReporter::reportErrorBrokers(const vector<string> &zombieWorkers,
                                              const vector<string> &timeoutWorkers) {
    static const string zombieName = ADMIN_GROUP_METRIC + "deadBroker";
    static const string timeoutName = ADMIN_GROUP_METRIC + "timeoutBroker";
    map<string, string> tagsMap;
    for (const auto &address : zombieWorkers) {
        tagsMap["address"] = address;
        MetricsTags tags(tagsMap);
        REPORT_USER_MUTABLE_QPS_TAGS(_metricsReporter, zombieName, &tags);
    }
    for (const auto &address : timeoutWorkers) {
        tagsMap["address"] = address;
        MetricsTags tags(tagsMap);
        REPORT_USER_MUTABLE_QPS_TAGS(_metricsReporter, timeoutName, &tags);
    }
}

void AdminMetricsReporter::reportPartitionResource(const vector<pair<string, uint32_t>> &resourceVec) {
    static const string name = ADMIN_GROUP_METRIC + "topicResource";
    for (const auto &item : resourceVec) {
        MetricsTags tags("topic", item.first);
        REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, name, item.second, &tags);
    }
}

void AdminMetricsReporter::reportturnToMasterMetrics(RpcCollector &collector) {
    _metricsReporter->report<turnToMasterMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportturnToSlaveMetrics(RpcCollector &collector) {
    _metricsReporter->report<turnToSlaveMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportgetMasterInfoMetrics(RpcCollector &collector) {
    _metricsReporter->report<getMasterInfoMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reportMergeMetrics(const std::unordered_map<std::string, autil::Bitmap> &topicPartMap) {
    static const string totalTopicCountName = ADMIN_GROUP_METRIC + "totalMergeTopicCount";
    static const string mergedTopicPercentName = ADMIN_GROUP_METRIC + "mergedTopicPercent";
    static const string totalPartCountName = ADMIN_GROUP_METRIC + "totalMergePartitionCount";
    static const string mergingPartCountName = ADMIN_GROUP_METRIC + "mergingPartitionCount";
    static const string mergedPartCountName = ADMIN_GROUP_METRIC + "mergedPartitionCount";
    static const string mergedPartPercentName = ADMIN_GROUP_METRIC + "mergedPartitionPercent";
    std::map<std::string, std::string> tagsMap;
    size_t totalTopicCount = topicPartMap.size();
    size_t mergedTopic = 0;
    for (auto it = topicPartMap.begin(); it != topicPartMap.end(); ++it) {
        MetricsTags tags("topic", it->first);
        REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, totalTopicCountName, 1, &tags);
        uint32_t totalPartCount = it->second.GetItemCount();
        REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, totalPartCountName, totalPartCount, &tags);
        uint32_t mergingCount = it->second.GetUnsetCount();
        REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, mergingPartCountName, mergingCount, &tags);
        uint32_t mergedCount = it->second.GetSetCount();
        REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, mergedPartCountName, mergedCount, &tags);
        float mergedPartitionPercent = 0;
        if (totalPartCount != 0) {
            mergedPartitionPercent = (float)mergedCount / totalPartCount;
        }
        if (mergedCount == totalPartCount) {
            ++mergedTopic;
        }
        REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, mergedPartPercentName, mergedPartitionPercent, &tags);
    }
    float mergedTopicPercent = 0;
    if (totalTopicCount != 0) {
        mergedTopicPercent = (float)mergedTopic / totalTopicCount;
    }
    REPORT_USER_MUTABLE_METRIC(_metricsReporter, mergedTopicPercentName, mergedTopicPercent);
}

void AdminMetricsReporter::reportupdateWriterVersionMetrics(RpcCollector &collector) {
    _metricsReporter->report<updateWriterVersionMetrics, RpcCollector>(nullptr, &collector);
}

void AdminMetricsReporter::reporttopicAclManageMetrics(RpcCollector &collector) {
    _metricsReporter->report<topicAclManageMetrics, RpcCollector>(nullptr, &collector);
}

} // namespace monitor
} // namespace swift
