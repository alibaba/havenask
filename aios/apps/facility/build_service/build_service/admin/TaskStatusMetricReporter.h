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
#ifndef ISEARCH_BS_TASKSTATUSMETRICREPORTER_H
#define ISEARCH_BS_TASKSTATUSMETRICREPORTER_H

#include "build_service/admin/MetricEventLogger.h"
#include "build_service/common_define.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"
#include "kmonitor_adapter/Monitor.h"

namespace build_service { namespace admin {

class TaskStatusMetricReporter
{
public:
    struct TaskLogItem {
        std::string id;
        int64_t totalNodeCount = -1;
        int64_t runningNodeCount = -1;
        int64_t taskRunningTime = -1;
    };

public:
    TaskStatusMetricReporter();
    ~TaskStatusMetricReporter();

private:
    TaskStatusMetricReporter(const TaskStatusMetricReporter&);
    TaskStatusMetricReporter& operator=(const TaskStatusMetricReporter&);

public:
    void init(const proto::BuildId& buildId, kmonitor_adapter::Monitor* globalMonitor);

    void reportTaskNodeCount(int64_t totalCount, int64_t runningCount, const kmonitor::MetricsTags& tags);

    void reportTaskStatus(int64_t status, const kmonitor::MetricsTags& tags);

    void reportTaskRunningTime(int64_t time, const kmonitor::MetricsTags& tags);

    void fillTaskLogInfo(std::vector<TaskLogItem>& logInfo);
    void clearAllLogInfo();

private:
    void initLoggers();
    void updateLog(const std::string& id, int64_t value, const kmonitor::MetricsTags& tags);

private:
    kmonitor::MetricsTags _tags;
    kmonitor_adapter::MetricPtr _totalTaskNodeCountMetric;
    kmonitor_adapter::MetricPtr _runningTaskNodeCountMetric;
    kmonitor_adapter::MetricPtr _taskStatusMetric;
    kmonitor_adapter::MetricPtr _taskRunningTimeMetric;

    std::unordered_map<std::string, std::shared_ptr<MetricEventLogger>> _loggerMap;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskStatusMetricReporter);

}} // namespace build_service::admin

#endif // ISEARCH_BS_TASKSTATUSMETRICREPORTER_H
