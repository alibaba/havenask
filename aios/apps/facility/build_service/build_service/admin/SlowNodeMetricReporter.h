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
#ifndef ISEARCH_BS_SLOWNODEMETRICREPORTER_H
#define ISEARCH_BS_SLOWNODEMETRICREPORTER_H

#include "build_service/common_define.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"
#include "kmonitor_adapter/Monitor.h"

namespace build_service { namespace admin {

class SlowNodeMetricReporter
{
public:
    enum DetectType {
        DT_DEAD,
        DT_RESTART,
        DT_SLOW,
        DT_RECLAIM,
    };

    enum HandleType {
        HT_BACKUP,
        HT_MIGRATE,
    };

    struct MetricRecord {
        int64_t slowNodeMigrateTimes = 0;
        int64_t deadNodeMigrateTimes = 0;
        int64_t restartNodeMigrateTimes = 0;
        int64_t backupNodeCreateTimes = 0;
        int64_t effectiveBackupNodeCount = 0;
    };

    SlowNodeMetricReporter();
    ~SlowNodeMetricReporter();

    SlowNodeMetricReporter(const SlowNodeMetricReporter&) = delete;
    SlowNodeMetricReporter& operator=(const SlowNodeMetricReporter&) = delete;
    SlowNodeMetricReporter(SlowNodeMetricReporter&&) = delete;
    SlowNodeMetricReporter& operator=(SlowNodeMetricReporter&&) = delete;

    void Init(const proto::BuildId& buildId, kmonitor_adapter::Monitor* monitor);
    void ReportMetrics(const MetricRecord& metricRecord, const kmonitor::MetricsTags& tags);

    void IncreaseDetectQps(DetectType type, const kmonitor::MetricsTags& tags);
    void IncreaseHandleQps(HandleType type, const kmonitor::MetricsTags& tags);

    bool EnableDetailMetric() const;

private:
    void IncreaseQps(const kmonitor_adapter::MetricPtr& metric, const kmonitor::MetricsTags& tags);

private:
    kmonitor::MetricsTags _tags;
    kmonitor_adapter::MetricPtr _handleNodeTimesMetric;
    kmonitor_adapter::MetricPtr _migrateNodeTimesMetric;
    kmonitor_adapter::MetricPtr _backupNodeCreateTimesMetric;
    kmonitor_adapter::MetricPtr _slowNodeMigrateTimesMetric;
    kmonitor_adapter::MetricPtr _deadNodeMigrateTimesMetric;
    kmonitor_adapter::MetricPtr _restartNodeMigrateTimesMetric;
    kmonitor_adapter::MetricPtr _effectiveBackupNodeCountMetric;

    kmonitor_adapter::MetricPtr _deadNodeDetectQps;
    kmonitor_adapter::MetricPtr _restartNodeDetectQps;
    kmonitor_adapter::MetricPtr _slowNodeDetectQps;
    kmonitor_adapter::MetricPtr _reclaimNodeDetectQps;

    kmonitor_adapter::MetricPtr _handleCreateBackupNodeQps;
    kmonitor_adapter::MetricPtr _handleMigrateNodeQps;

    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SlowNodeMetricReporter);

}} // namespace build_service::admin

#endif // ISEARCH_BS_SLOWNODEMETRICREPORTER_H
