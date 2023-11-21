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
#include "build_service/admin/CheckpointMetricReporter.h"

#include <cstddef>
#include <cstdint>
#include <limits>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/util/Monitor.h"
#include "kmonitor/client/MetricLevel.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, CheckpointMetricReporter);

#define METRIC_TAG_GENERATION "generation"
#define METRIC_TAG_APPNAME "appName"
#define METRIC_TAG_DATATABLE "dataTable"

CheckpointMetricReporter::CheckpointMetricReporter() {}

CheckpointMetricReporter::~CheckpointMetricReporter() {}

void CheckpointMetricReporter::init(const proto::BuildId& buildId, kmonitor_adapter::Monitor* monitor)
{
    initCheckpointLoggers();
    if (monitor == NULL) {
        BS_LOG(INFO, "init CheckpointMetricReporter failed");
        return;
    }

    BS_LOG(INFO, "init CheckpointMetricReporter");
    string generationId = StringUtil::toString(buildId.generationid());
    string appName = buildId.appname();
    string dataTable = buildId.datatable();

    _tags.AddTag(METRIC_TAG_GENERATION, generationId);
    _tags.AddTag(METRIC_TAG_APPNAME, appName);
    _tags.AddTag(METRIC_TAG_DATATABLE, dataTable);

    _indexVersionMetric = monitor->registerGaugeMetric("checkpoint.indexVersion", kmonitor::FATAL);
    _indexSchemaVersionMetric = monitor->registerGaugeMetric("checkpoint.indexSchemaVersion", kmonitor::FATAL);
    _indexSizeMetric = monitor->registerGaugeMetric("checkpoint.indexSize", kmonitor::FATAL);
    _indexRemainSizeMetric = monitor->registerGaugeMetric("checkpoint.indexTotalRemainSize", kmonitor::FATAL);
    _suezVersionZkConnectionMetric =
        monitor->registerGaugeMetric("checkpoint.suezVersionZkConnection", kmonitor::FATAL);
    _indexTaskRemainOpCountMetric = monitor->registerGaugeMetric("checkpoint.indexTaskRemainOpCount", kmonitor::FATAL);
    _indexFreshnessMetric = monitor->registerGaugeMetric("checkpoint.indexFreshness", kmonitor::FATAL);
    _processorFreshnessMetric = monitor->registerGaugeMetric("checkpoint.processorFreshness", kmonitor::FATAL);
    _singleProcessorFreshnessMetric =
        monitor->registerGaugeMetric("checkpoint.singleProcessorFreshness", kmonitor::FATAL);
    _singleBuilderFreshnessMetric = monitor->registerGaugeMetric("checkpoint.singleBuilderFreshness", kmonitor::FATAL);
}

void CheckpointMetricReporter::initCheckpointLoggers()
{
    if (!autil::EnvUtil::getEnv("bs_support_metric_logger", true)) {
        return;
    }
    _checkpointLoggerMap["indexVersion"] = make_shared<MetricEventLogger>();
    _checkpointLoggerMap["indexSchemaVersion"] = make_shared<MetricEventLogger>();
    _checkpointLoggerMap["indexSize"] = make_shared<MetricEventLogger>();
    _checkpointLoggerMap["indexTotalRemainSize"] = make_shared<MetricEventLogger>();
    _checkpointLoggerMap["indexFreshness"] = make_shared<MetricEventLogger>();
    _checkpointLoggerMap["processorFreshness"] = make_shared<MetricEventLogger>();
    _checkpointLoggerMap["singleProcessorFreshness"] = make_shared<MetricEventLogger>();
    _checkpointLoggerMap["singleBuilderFreshness"] = make_shared<MetricEventLogger>();
    _checkpointLoggerMap["indexTaskRemainOperationCount"] = make_shared<MetricEventLogger>();
}

void CheckpointMetricReporter::updateLog(const std::string& id, int64_t value, const kmonitor::MetricsTags& tags)
{
    if (_checkpointLoggerMap.empty()) {
        return;
    }

    auto iter = _checkpointLoggerMap.find(id);
    if (iter == _checkpointLoggerMap.end()) {
        return;
    }
    iter->second->update(value, tags);
}

void CheckpointMetricReporter::reportIndexVersion(int64_t value, const kmonitor::MetricsTags& tags)
{
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    REPORT_KMONITOR_METRIC2(_indexVersionMetric, newTags, value);
    updateLog("indexVersion", value, tags);
}

void CheckpointMetricReporter::reportIndexTaskRemainOperationCount(int64_t value, const kmonitor::MetricsTags& tags)
{
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    REPORT_KMONITOR_METRIC2(_indexTaskRemainOpCountMetric, newTags, value);
    updateLog("indexTaskRemainOperationCount", value, tags);
}

void CheckpointMetricReporter::reportIndexSchemaVersion(int64_t value, const kmonitor::MetricsTags& tags)
{
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    REPORT_KMONITOR_METRIC2(_indexSchemaVersionMetric, newTags, value);
    updateLog("indexSchemaVersion", value, tags);
}

void CheckpointMetricReporter::reportIndexSize(int64_t value, const kmonitor::MetricsTags& tags)
{
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    REPORT_KMONITOR_METRIC2(_indexSizeMetric, newTags, value);
    updateLog("indexSize", value, tags);
}

void CheckpointMetricReporter::reportIndexRemainSize(int64_t value, const kmonitor::MetricsTags& tags)
{
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    REPORT_KMONITOR_METRIC2(_indexRemainSizeMetric, newTags, value);
    updateLog("indexTotalRemainSize", value, tags);
}

void CheckpointMetricReporter::reportSuezVersionZkConnection(int64_t value, const kmonitor::MetricsTags& tags)
{
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    REPORT_KMONITOR_METRIC2(_suezVersionZkConnectionMetric, newTags, value);
}

void CheckpointMetricReporter::calculateAndReportIndexTimestampFreshness(int64_t value,
                                                                         const kmonitor::MetricsTags& tags)
{
    if (value == std::numeric_limits<int64_t>::max() || value < 0) {
        return;
    }
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    int64_t curTs = TimeUtility::currentTime();
    REPORT_KMONITOR_METRIC2(_indexFreshnessMetric, newTags, curTs - value);
    updateLog("indexFreshness", autil::TimeUtility::us2sec(curTs - value), tags);
}

void CheckpointMetricReporter::calculateAndReportProcessorFreshness(int64_t value, const kmonitor::MetricsTags& tags)
{
    if (value == std::numeric_limits<int64_t>::max() || value < 0) {
        return;
    }
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    int64_t curTs = TimeUtility::currentTime();
    REPORT_KMONITOR_METRIC2(_processorFreshnessMetric, newTags, curTs - value);
    updateLog("processorFreshness", autil::TimeUtility::us2sec(curTs - value), tags);
}

void CheckpointMetricReporter::reportSingleBuilderFreshness(int64_t value, const kmonitor::MetricsTags& tags)
{
    if (value == std::numeric_limits<int64_t>::max() || value < 0) {
        return;
    }
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    int64_t curTs = TimeUtility::currentTime();
    REPORT_KMONITOR_METRIC2(_singleBuilderFreshnessMetric, newTags, curTs - value);
    updateLog("singleBuilderFreshness", autil::TimeUtility::us2sec(curTs - value), tags);
}

void CheckpointMetricReporter::reportSingleProcessorFreshness(int64_t value, const kmonitor::MetricsTags& tags)
{
    if (value == std::numeric_limits<int64_t>::max() || value < 0) {
        return;
    }
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    int64_t curTs = TimeUtility::currentTime();
    REPORT_KMONITOR_METRIC2(_singleProcessorFreshnessMetric, newTags, curTs - value);
    updateLog("singleProcessorFreshness", autil::TimeUtility::us2sec(curTs - value), tags);
}

void CheckpointMetricReporter::fillCheckpointInfo(
    std::map<std::string, std::vector<std::pair<std::string, int64_t>>>& infoMap)
{
    for (auto iter = _checkpointLoggerMap.begin(); iter != _checkpointLoggerMap.end(); iter++) {
        iter->second->fillLogInfo(infoMap[iter->first]);
    }
}

void CheckpointMetricReporter::clearAllLogInfo()
{
    for (auto iter = _checkpointLoggerMap.begin(); iter != _checkpointLoggerMap.end(); iter++) {
        iter->second->clear();
    }
}

}} // namespace build_service::admin
