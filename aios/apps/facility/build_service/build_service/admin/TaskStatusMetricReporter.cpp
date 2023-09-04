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
#include "build_service/admin/TaskStatusMetricReporter.h"

#include "build_service/util/Monitor.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, TaskStatusMetricReporter);

#define METRIC_TAG_GENERATION "generation"
#define METRIC_TAG_APPNAME "appName"
#define METRIC_TAG_DATATABLE "dataTable"

TaskStatusMetricReporter::TaskStatusMetricReporter() {}

TaskStatusMetricReporter::~TaskStatusMetricReporter() {}

void TaskStatusMetricReporter::init(const proto::BuildId& buildId, kmonitor_adapter::Monitor* monitor)
{
    initLoggers();
    if (monitor == NULL) {
        BS_LOG(INFO, "init CheckpointMetricReporter failed");
        return;
    }

    BS_LOG(INFO, "init TaskStatusMetricReporter");
    string generationId = StringUtil::toString(buildId.generationid());
    string appName = buildId.appname();
    string dataTable = buildId.datatable();

    _tags.AddTag(METRIC_TAG_GENERATION, generationId);
    _tags.AddTag(METRIC_TAG_APPNAME, appName);
    _tags.AddTag(METRIC_TAG_DATATABLE, dataTable);

    _totalTaskNodeCountMetric = monitor->registerGaugeMetric("task.totalNodeCount", kmonitor::FATAL);
    _runningTaskNodeCountMetric = monitor->registerGaugeMetric("task.runningNodeCount", kmonitor::FATAL);
    _taskStatusMetric = monitor->registerGaugeMetric("task.taskStatus", kmonitor::FATAL);
    _taskRunningTimeMetric = monitor->registerGaugeMetric("task.taskRunningTimeInMicroSec", kmonitor::FATAL);
}

void TaskStatusMetricReporter::reportTaskNodeCount(int64_t totalCount, int64_t runningCount,
                                                   const kmonitor::MetricsTags& tags)
{
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    REPORT_KMONITOR_METRIC2(_totalTaskNodeCountMetric, newTags, totalCount);
    REPORT_KMONITOR_METRIC2(_runningTaskNodeCountMetric, newTags, runningCount);
    updateLog("totalNodeCount", totalCount, tags);
    updateLog("runningNodeCount", runningCount, tags);
}

void TaskStatusMetricReporter::reportTaskStatus(int64_t status, const kmonitor::MetricsTags& tags)
{
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    REPORT_KMONITOR_METRIC2(_taskStatusMetric, newTags, status);
}

void TaskStatusMetricReporter::reportTaskRunningTime(int64_t time, const kmonitor::MetricsTags& tags)
{
    kmonitor::MetricsTags newTags(_tags.GetTagsMap());
    tags.MergeTags(&newTags);
    REPORT_KMONITOR_METRIC2(_taskRunningTimeMetric, newTags, time);
    updateLog("taskRunningTime", autil::TimeUtility::us2sec(time), tags);
}

void TaskStatusMetricReporter::initLoggers()
{
    if (!autil::EnvUtil::getEnv("bs_support_metric_logger", true)) {
        return;
    }
    _loggerMap["totalNodeCount"] = make_shared<MetricEventLogger>();
    _loggerMap["runningNodeCount"] = make_shared<MetricEventLogger>();
    _loggerMap["taskRunningTime"] = make_shared<MetricEventLogger>();
}

void TaskStatusMetricReporter::updateLog(const std::string& id, int64_t value, const kmonitor::MetricsTags& tags)
{
    if (_loggerMap.empty()) {
        return;
    }
    auto iter = _loggerMap.find(id);
    if (iter == _loggerMap.end()) {
        return;
    }
    iter->second->update(value, tags);
}

void TaskStatusMetricReporter::clearAllLogInfo()
{
    for (auto iter = _loggerMap.begin(); iter != _loggerMap.end(); iter++) {
        iter->second->clear();
    }
}

void TaskStatusMetricReporter::fillTaskLogInfo(std::vector<TaskLogItem>& logInfo)
{
    if (_loggerMap.empty()) {
        return;
    }

    auto totalNodeCountLogger = _loggerMap["totalNodeCount"];
    auto runningNodeCountLogger = _loggerMap["runningNodeCount"];
    auto taskRunningTimeLogger = _loggerMap["taskRunningTime"];
    if (!taskRunningTimeLogger || !runningNodeCountLogger || !taskRunningTimeLogger) {
        return;
    }

    map<string, TaskLogItem> logMap;
    vector<pair<string, int64_t>> logValues;
    totalNodeCountLogger->fillLogInfo(logValues);
    for (auto& item : logValues) {
        auto& logItem = logMap[item.first];
        logItem.id = item.first;
        logItem.totalNodeCount = item.second;
    }
    logValues.clear();
    runningNodeCountLogger->fillLogInfo(logValues);
    for (auto& item : logValues) {
        auto& logItem = logMap[item.first];
        if (logItem.id.empty()) {
            logItem.id = item.first;
        }
        logItem.runningNodeCount = item.second;
    }
    logValues.clear();
    taskRunningTimeLogger->fillLogInfo(logValues);
    for (auto& item : logValues) {
        auto& logItem = logMap[item.first];
        if (logItem.id.empty()) {
            logItem.id = item.first;
        }
        logItem.taskRunningTime = item.second;
    }
    for (auto iter = logMap.begin(); iter != logMap.end(); iter++) {
        logInfo.push_back(iter->second);
    }
}

}} // namespace build_service::admin
