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
#include "indexlib/framework/index_task/IndexTaskMetrics.h"

#include "indexlib/framework/index_task/IndexTaskHistory.h"
#include "indexlib/util/metrics/KmonitorTagvNormalizer.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, IndexTaskMetrics);

IndexTaskMetrics::IndexTaskMetrics(const std::string& tabletName,
                                   const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter)
    : _tabletName(indexlib::util::KmonitorTagvNormalizer::GetInstance()->Normalize(tabletName))
    , _metricsReporter(metricsReporter)
    , _lastTaskFinishedTimestamp(autil::TimeUtility::currentTimeInSeconds())
{
}

IndexTaskMetrics::~IndexTaskMetrics() {}

void IndexTaskMetrics::SetCurrentTask(const std::string& taskType, const std::string& taskName)
{
    std::lock_guard<std::mutex> guard(_mutex);
    _currentTaskType = taskType;
    _currentTaskName = taskName;
    _currentTaskTriggerTimestamp = autil::TimeUtility::currentTimeInSeconds();
}

void IndexTaskMetrics::FinishCurrentTask()
{
    std::lock_guard<std::mutex> guard(_mutex);
    _currentTaskTriggerTimestamp = -1;
    _lastTaskFinishedTimestamp = autil::TimeUtility::currentTimeInSeconds();
    _currentTaskType.clear();
    _currentTaskName.clear();
}

void IndexTaskMetrics::UpdateTaskHistory(const framework::IndexTaskHistory& his)
{
    auto typeNames = his.GetAllTaskTypes();
    std::lock_guard<std::mutex> guard(_mutex);
    for (auto& name : typeNames) {
        auto taskLogs = his.GetTaskLogs(name);
        _latestTaskTimestamp[name] = taskLogs[0]->GetTriggerTimestampInSecond();
    }
}

void IndexTaskMetrics::ReportMetrics()
{
    INDEXLIB_FM_REPORT_METRIC(mergeBaseVersionId);
    INDEXLIB_FM_REPORT_METRIC(mergeTargetVersionId);
    INDEXLIB_FM_REPORT_METRIC(mergeCommittedVersionId);
    INDEXLIB_FM_REPORT_METRIC(runningMergeBaseVersionId);
    INDEXLIB_FM_REPORT_METRIC(runningMergeLeftOps);
    INDEXLIB_FM_REPORT_METRIC(runningMergeTotalOps);

    std::string runningTaskType;
    std::string runningTaskName;
    int64_t runningTaskTriggerTimestamp;
    std::map<std::string, int64_t> historyTaskInfoMap;
    {
        std::lock_guard<std::mutex> guard(_mutex);
        runningTaskType = _currentTaskType;
        runningTaskName = _currentTaskName;
        runningTaskTriggerTimestamp = _currentTaskTriggerTimestamp;
        historyTaskInfoMap = _latestTaskTimestamp;
    }

    int64_t currentTime = autil::TimeUtility::currentTimeInSeconds();
    {
        kmonitor::MetricsTags tags;
        if (!runningTaskType.empty()) {
            tags.AddTag("taskType", runningTaskType);
        }
        if (!runningTaskName.empty()) {
            tags.AddTag("taskName", runningTaskName);
        }
        if (!_tabletName.empty()) {
            tags.AddTag("tabletName", _tabletName);
        }
        if (runningTaskTriggerTimestamp > 0) {
            INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&tags, runningTaskTriggerTimeFreshness,
                                                          currentTime - runningTaskTriggerTimestamp);
            INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&tags, notTriggerRunningTaskTimeFreshness, 0);
        } else {
            INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&tags, notTriggerRunningTaskTimeFreshness,
                                                          currentTime - _lastTaskFinishedTimestamp);
        }
    }
    for (auto& taskInfo : historyTaskInfoMap) {
        kmonitor::MetricsTags tags;
        tags.AddTag("taskTypeId",
                    indexlib::util::KmonitorTagvNormalizer::GetInstance()->Normalize(taskInfo.first, '-'));
        // tags.AddTag("tabletName", _tabletName);
        INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&tags, finishedTaskTriggerTimeFreshness,
                                                      currentTime - taskInfo.second);
    }
}

void IndexTaskMetrics::RegisterMetrics()
{
#define REGISTER_TABLET_MERGE_METRIC(metricName, metricType)                                                           \
    _##metricName = 0L;                                                                                                \
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, metricName, "merge/" #metricName, metricType)

    REGISTER_TABLET_MERGE_METRIC(mergeBaseVersionId, kmonitor::GAUGE);
    REGISTER_TABLET_MERGE_METRIC(mergeTargetVersionId, kmonitor::GAUGE);
    REGISTER_TABLET_MERGE_METRIC(mergeCommittedVersionId, kmonitor::GAUGE);
    REGISTER_TABLET_MERGE_METRIC(runningMergeBaseVersionId, kmonitor::GAUGE);
    REGISTER_TABLET_MERGE_METRIC(runningMergeLeftOps, kmonitor::GAUGE);
    REGISTER_TABLET_MERGE_METRIC(runningMergeTotalOps, kmonitor::GAUGE);

#undef REGISTER_TABLET_MERGE_METRIC

#define REGISTER_TABLET_TASK_METRIC(metricName, metricType)                                                            \
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, metricName, "task/" #metricName, metricType)

    REGISTER_TABLET_TASK_METRIC(finishedTaskTriggerTimeFreshness, kmonitor::GAUGE);
    REGISTER_TABLET_TASK_METRIC(runningTaskTriggerTimeFreshness, kmonitor::GAUGE);
    REGISTER_TABLET_TASK_METRIC(notTriggerRunningTaskTimeFreshness, kmonitor::GAUGE);

#undef REGISTER_TABLET_TASK_METRIC
}

void IndexTaskMetrics::FillMetricsInfo(std::map<std::string, std::string>& infoMap)
{
    infoMap["mergeBaseVersionId"] = autil::StringUtil::toString(_mergeBaseVersionId);
    infoMap["mergeTargetVersionId"] = autil::StringUtil::toString(_mergeTargetVersionId);
    infoMap["mergeCommittedVersionId"] = autil::StringUtil::toString(_mergeCommittedVersionId);
    infoMap["runningMergeBaseVersionId"] = autil::StringUtil::toString(_runningMergeBaseVersionId);
    infoMap["runningMergeLeftOps"] = autil::StringUtil::toString(_runningMergeLeftOps);
    infoMap["runningMergeTotalOps"] = autil::StringUtil::toString(_runningMergeTotalOps);

    std::lock_guard<std::mutex> guard(_mutex);
    if (_currentTaskTriggerTimestamp > 0) {
        infoMap["runningTaskType"] = _currentTaskType;
        infoMap["runningTaskName"] = _currentTaskName;
        infoMap["runningTaskTriggerTime"] = autil::StringUtil::toString(_currentTaskTriggerTimestamp);
        infoMap["lastTaskFinishedTimestamp"] = autil::StringUtil::toString(_lastTaskFinishedTimestamp);
    }
}

} // namespace indexlibv2::framework
