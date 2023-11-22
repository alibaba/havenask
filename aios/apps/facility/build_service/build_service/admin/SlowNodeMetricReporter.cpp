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
#include "build_service/admin/SlowNodeMetricReporter.h"

#include <assert.h>
#include <iosfwd>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "build_service/util/Monitor.h"
#include "kmonitor/client/MetricLevel.h"

using namespace std;
using namespace autil;

#define METRIC_TAG_GENERATION "generation"
#define METRIC_TAG_APPNAME "appName"
#define METRIC_TAG_DATATABLE "dataTable"

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, SlowNodeMetricReporter);

SlowNodeMetricReporter::SlowNodeMetricReporter() {}
SlowNodeMetricReporter::~SlowNodeMetricReporter() {}

void SlowNodeMetricReporter::Init(const proto::BuildId& buildId, kmonitor_adapter::Monitor* monitor)
{
    if (monitor == nullptr) {
        BS_LOG(ERROR, "init SlowNodeMetricReporter failed");
        return;
    }
    std::string generationId = StringUtil::toString(buildId.generationid());
    std::string appName = buildId.appname();
    std::string dataTable = buildId.datatable();

    _tags.AddTag(METRIC_TAG_GENERATION, generationId);
    _tags.AddTag(METRIC_TAG_APPNAME, appName);
    _tags.AddTag(METRIC_TAG_DATATABLE, dataTable);

    _handleNodeTimesMetric = monitor->registerGaugeMetric("slowNode.handleNodeTimes", kmonitor::FATAL);
    _migrateNodeTimesMetric = monitor->registerGaugeMetric("slowNode.migrateNodeTimes", kmonitor::FATAL);
    _backupNodeCreateTimesMetric = monitor->registerGaugeMetric("slowNode.backupNodeCreateTimes", kmonitor::FATAL);
    _slowNodeMigrateTimesMetric = monitor->registerGaugeMetric("slowNode.slowNodeMigrateTimes", kmonitor::FATAL);
    _deadNodeMigrateTimesMetric = monitor->registerGaugeMetric("slowNode.deadNodeMigrateTimes", kmonitor::FATAL);
    _restartNodeMigrateTimesMetric = monitor->registerGaugeMetric("slowNode.restartNodeMigrateTimes", kmonitor::FATAL);
    _effectiveBackupNodeCountMetric =
        monitor->registerGaugeMetric("slowNode.effectiveBackupNodeCount", kmonitor::FATAL);

    if (autil::EnvUtil::getEnv("bs_enable_slow_node_detail_metric", true)) {
        _deadNodeDetectQps = monitor->registerQPSMetric("slowNode.deadNodeDetectQps", kmonitor::FATAL);
        _restartNodeDetectQps = monitor->registerQPSMetric("slowNode.restartNodeDetectQps", kmonitor::FATAL);
        _slowNodeDetectQps = monitor->registerQPSMetric("slowNode.slowNodeDetectQps", kmonitor::FATAL);
        _reclaimNodeDetectQps = monitor->registerQPSMetric("slowNode.reclaimNodeDetectQps", kmonitor::FATAL);
        _handleCreateBackupNodeQps = monitor->registerQPSMetric("slowNode.handleCreateBackupNodeQps", kmonitor::FATAL);
        _handleMigrateNodeQps = monitor->registerQPSMetric("slowNode.handleMigrateNodeQps", kmonitor::FATAL);
    }
    BS_LOG(INFO, "init SlowNodeMetricReporter success");
}

void SlowNodeMetricReporter::ReportMetrics(const MetricRecord& metricRecord, const kmonitor::MetricsTags& tags)
{
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    int64_t migrateNodeTimes =
        metricRecord.slowNodeMigrateTimes + metricRecord.deadNodeMigrateTimes + metricRecord.restartNodeMigrateTimes;
    int64_t handleNodeTimes = migrateNodeTimes + metricRecord.backupNodeCreateTimes;

    REPORT_KMONITOR_METRIC2(_handleNodeTimesMetric, newTags, handleNodeTimes);
    REPORT_KMONITOR_METRIC2(_migrateNodeTimesMetric, newTags, migrateNodeTimes);
    REPORT_KMONITOR_METRIC2(_backupNodeCreateTimesMetric, newTags, metricRecord.backupNodeCreateTimes);
    REPORT_KMONITOR_METRIC2(_slowNodeMigrateTimesMetric, newTags, metricRecord.slowNodeMigrateTimes);
    REPORT_KMONITOR_METRIC2(_deadNodeMigrateTimesMetric, newTags, metricRecord.deadNodeMigrateTimes);
    REPORT_KMONITOR_METRIC2(_restartNodeMigrateTimesMetric, newTags, metricRecord.restartNodeMigrateTimes);
    REPORT_KMONITOR_METRIC2(_effectiveBackupNodeCountMetric, newTags, metricRecord.effectiveBackupNodeCount);
}

void SlowNodeMetricReporter::IncreaseDetectQps(DetectType type, const kmonitor::MetricsTags& tags)
{
    switch (type) {
    case DT_DEAD:
        return IncreaseQps(_deadNodeDetectQps, tags);
    case DT_RESTART:
        return IncreaseQps(_restartNodeDetectQps, tags);
    case DT_SLOW:
        return IncreaseQps(_slowNodeDetectQps, tags);
    case DT_RECLAIM:
        return IncreaseQps(_reclaimNodeDetectQps, tags);
    default:
        assert(false);
    }
}

void SlowNodeMetricReporter::IncreaseHandleQps(HandleType type, const kmonitor::MetricsTags& tags)
{
    switch (type) {
    case HT_BACKUP:
        return IncreaseQps(_handleCreateBackupNodeQps, tags);
    case HT_MIGRATE:
        return IncreaseQps(_handleMigrateNodeQps, tags);
    default:
        assert(false);
    }
}

void SlowNodeMetricReporter::IncreaseQps(const kmonitor_adapter::MetricPtr& metric, const kmonitor::MetricsTags& tags)
{
    if (!metric) {
        return;
    }

    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    metric->report(newTags, 1);
}

bool SlowNodeMetricReporter::EnableDetailMetric() const { return _deadNodeDetectQps != nullptr; }

}} // namespace build_service::admin
