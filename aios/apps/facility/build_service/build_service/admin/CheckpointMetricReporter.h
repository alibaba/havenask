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
#ifndef ISEARCH_BS_CHECKPOINTMETRICREPORTER_H
#define ISEARCH_BS_CHECKPOINTMETRICREPORTER_H

#include "build_service/admin/AppPlanMaker.h"
#include "build_service/admin/MetricEventLogger.h"
#include "build_service/common_define.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class CheckpointMetricReporter
{
public:
    CheckpointMetricReporter();
    ~CheckpointMetricReporter();

private:
    CheckpointMetricReporter(const CheckpointMetricReporter&);
    CheckpointMetricReporter& operator=(const CheckpointMetricReporter&);

public:
    void init(const proto::BuildId& buildId, kmonitor_adapter::Monitor* globalMonitor);

    void reportIndexVersion(int64_t value, const kmonitor::MetricsTags& tags);
    void reportIndexSchemaVersion(int64_t value, const kmonitor::MetricsTags& tags);
    void reportIndexSize(int64_t value, const kmonitor::MetricsTags& tags);
    void reportIndexRemainSize(int64_t value, const kmonitor::MetricsTags& tags);
    void reportSuezVersionZkConnection(int64_t value, const kmonitor::MetricsTags& tags);
    void calculateAndReportIndexTimestampFreshness(int64_t indexTs, const kmonitor::MetricsTags& tags);
    void calculateAndReportProcessorFreshness(int64_t processTs, const kmonitor::MetricsTags& tags);
    void reportSingleBuilderFreshness(int64_t builderTs, const kmonitor::MetricsTags& tags);
    void reportSingleProcessorFreshness(int64_t builderTs, const kmonitor::MetricsTags& tags);

    void reportIndexTaskRemainOperationCount(int64_t count, const kmonitor::MetricsTags& tags);

    void fillCheckpointInfo(std::map<std::string, std::vector<std::pair<std::string, int64_t>>>& infoMap);
    void clearAllLogInfo();

private:
    void initCheckpointLoggers();
    void updateLog(const std::string& id, int64_t value, const kmonitor::MetricsTags& tags);

private:
    kmonitor_adapter::Monitor* _monitor = nullptr;
    kmonitor::MetricsTags _tags;
    kmonitor_adapter::MetricPtr _indexVersionMetric;
    kmonitor_adapter::MetricPtr _indexSchemaVersionMetric;
    kmonitor_adapter::MetricPtr _indexSizeMetric;
    kmonitor_adapter::MetricPtr _indexRemainSizeMetric;
    kmonitor_adapter::MetricPtr _indexFreshnessMetric;
    kmonitor_adapter::MetricPtr _singleBuilderFreshnessMetric;
    kmonitor_adapter::MetricPtr _processorFreshnessMetric;
    kmonitor_adapter::MetricPtr _singleProcessorFreshnessMetric;
    kmonitor_adapter::MetricPtr _suezVersionZkConnectionMetric;
    kmonitor_adapter::MetricPtr _indexTaskRemainOpCountMetric;

    std::unordered_map<std::string, std::shared_ptr<MetricEventLogger>> _checkpointLoggerMap;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CheckpointMetricReporter);

}} // namespace build_service::admin

#endif // ISEARCH_BS_CHECKPOINTMETRICREPORTER_H
