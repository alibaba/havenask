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

#include <cstdint>
#include "kmonitor/client/MetricsReporter.h"

namespace kmonitor {
class MutableMetric;
class MetricsTags;
class MetricsGroupManager;
}

namespace navi {

struct NaviStat;

struct RunGraphMetricsCollector {
    int64_t newSessionLatency = 0;
    int64_t sessionBeginTime = 0;
    int64_t initScheduleTime = 0;
    int64_t fillResultTime = 0;
    int64_t totalQueueLatency = 0;
    int64_t totalComputeLatency = 0;
    bool hasError = false;
    bool hasLack = false;
};

class RunGraphMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, RunGraphMetricsCollector *collector);
private:
    kmonitor::MutableMetric *_latency = nullptr;
    kmonitor::MutableMetric *_sessionScheduleWaitLatency = nullptr;
    kmonitor::MutableMetric *_naviRunLatency = nullptr;
    kmonitor::MutableMetric *_totalQueueLatency = nullptr;
    kmonitor::MutableMetric *_totalComputeLatency = nullptr;
    kmonitor::MutableMetric *_newSessionLatency = nullptr;
    kmonitor::MutableMetric *_naviRunSuccessQps = nullptr;
    kmonitor::MutableMetric *_naviRunFailedQps = nullptr;
    kmonitor::MutableMetric *_naviRunQps = nullptr;
    kmonitor::MutableMetric *_naviRpcLackQps = nullptr;
};

class NaviStatMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, const NaviStat *stat);
private:
    kmonitor::MutableMetric *_serviceStatus = nullptr;
};

struct TaskQueueStat;

class TaskQueueStatMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, const TaskQueueStat *stat);
private:
    kmonitor::MutableMetric *_activeThreadCount = nullptr;
    kmonitor::MutableMetric *_activeThreadQueueCount = nullptr;
    kmonitor::MutableMetric *_idleThreadQueueCount = nullptr;
    kmonitor::MutableMetric *_processingCount = nullptr;
    kmonitor::MutableMetric *_processingCountRatio = nullptr;
    kmonitor::MutableMetric *_queueCount = nullptr;
    kmonitor::MutableMetric *_queueCountRatio = nullptr;
};


}
