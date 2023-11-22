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

#include <mutex>

#include "autil/Log.h"
#include "indexlib/framework/IMetrics.h"
#include "indexlib/framework/MetricsWrapper.h"

namespace kmonitor {
class MetricsReporter;
}

namespace indexlibv2::framework {
class IndexTaskHistory;

class IndexTaskMetrics : public framework::IMetrics
{
public:
    IndexTaskMetrics(const std::string& tabletName, const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter);

    ~IndexTaskMetrics();

public:
    void SetCurrentTask(const std::string& taskType, const std::string& taskName);
    void FinishCurrentTask();
    void UpdateTaskHistory(const framework::IndexTaskHistory& his);

    void ReportMetrics() override;
    void RegisterMetrics() override;

    void FillMetricsInfo(std::map<std::string, std::string>& infoMap);

private:
    mutable std::mutex _mutex;
    std::string _tabletName;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    std::string _currentTaskType;
    std::string _currentTaskName;
    int64_t _currentTaskTriggerTimestamp = -1;
    int64_t _lastTaskFinishedTimestamp = -1;
    std::map<std::string, int64_t> _latestTaskTimestamp; /* key : task type */

    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int32_t, mergeBaseVersionId);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int32_t, mergeTargetVersionId);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int32_t, mergeCommittedVersionId);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, mergeCommittedVersionDelay);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int32_t, runningMergeBaseVersionId);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, runningMergeLeftOps);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, runningMergeTotalOps);

    INDEXLIB_FM_DECLARE_METRIC(finishedTaskTriggerTimeFreshness);
    INDEXLIB_FM_DECLARE_METRIC(runningTaskTriggerTimeFreshness);
    INDEXLIB_FM_DECLARE_METRIC(notTriggerRunningTaskTimeFreshness);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
