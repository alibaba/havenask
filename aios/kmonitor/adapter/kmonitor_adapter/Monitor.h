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

#include <map>
#include <memory>
#include <vector>

#include "Metric.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/core/MutableMetric.h"

namespace kmonitor_adapter {

class ScopeLatencyReporter {
public:
    ScopeLatencyReporter(MetricPtr &metric) : _beginTime(autil::TimeUtility::currentTime()), _metric(metric) {}
    ~ScopeLatencyReporter() {
        if (_metric) {
            int64_t endTime = autil::TimeUtility::currentTime();
            _metric->report((endTime - _beginTime) / 1000.0);
        }
    }

private:
    int64_t _beginTime;
    MetricPtr &_metric;
};

class Monitor {
public:
    Monitor(std::string serviceName, autil::RecursiveThreadMutex *metricMutex, kmonitor::KMonitor *kmonitor);
    ~Monitor();

    void init(std::string service);

    typedef std::vector<std::pair<std::string, std::string>> KVVec;
    MetricPtr registerRawMetric(const std::string &name, kmonitor::MetricLevel level, const KVVec &tags = KVVec());
    MetricPtr registerQPSMetric(const std::string &name, kmonitor::MetricLevel level, const KVVec &tags = KVVec());
    MetricPtr registerStatusMetric(const std::string &name, kmonitor::MetricLevel level, const KVVec &tags = KVVec());
    MetricPtr registerCounterMetric(const std::string &name, kmonitor::MetricLevel level, const KVVec &tags = KVVec());
    MetricPtr registerGaugeMetric(const std::string &name,
                                  kmonitor::MetricLevel level,
                                  const KVVec &tags = KVVec(),
                                  bool more_stats = false);
    MetricPtr
    registerGaugePercentileMetric(const std::string &name, kmonitor::MetricLevel level, const KVVec &tags = KVVec());
    // max, p99(global + local)
    MetricPtr registerLatencyMetric(const std::string &name, kmonitor::MetricLevel level, const KVVec &tags = KVVec());
    void unregisterMetric(const std::string &name);

    MetricPtr registerMetric(const std::string &name,
                             kmonitor::MetricType metricType,
                             kmonitor::MetricLevel level,
                             const KVVec &tags = KVVec());

private:
    autil::RecursiveThreadMutex *_metricMutex;
    kmonitor::KMonitor *_kmonitor = nullptr;
};

typedef std::unique_ptr<Monitor> MonitorPtr;

} // namespace kmonitor_adapter
