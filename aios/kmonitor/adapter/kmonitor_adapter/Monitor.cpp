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
#include "kmonitor_adapter/Monitor.h"

#include <sstream>

#include "kmonitor/client/KMonitor.h"
#include "kmonitor/client/KMonitorFactory.h"
#include "kmonitor/client/KMonitorWorker.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/core/MetricsConfig.h"
#include "kmonitor/client/core/MetricsTags.h"

using namespace std;
using namespace kmonitor;

namespace kmonitor_adapter {
AUTIL_DECLARE_AND_SETUP_LOGGER(monitor, Monitor);

Monitor::Monitor(string serviceName, autil::RecursiveThreadMutex *metricMutex, kmonitor::KMonitor *kmonitor)
    : _metricMutex(metricMutex), _kmonitor(kmonitor) {
    _kmonitor->SetServiceName(serviceName);
}

Monitor::~Monitor() {}

MetricPtr Monitor::registerRawMetric(const string &name, MetricLevel level, const KVVec &tags) {
    return registerMetric(name, RAW, level, tags);
}

MetricPtr Monitor::registerQPSMetric(const string &name, MetricLevel level, const KVVec &tags) {
    return registerMetric(name, QPS, level, tags);
}

MetricPtr Monitor::registerCounterMetric(const string &name, MetricLevel level, const KVVec &tags) {
    return registerMetric(name, COUNTER, level, tags);
}

MetricPtr Monitor::registerStatusMetric(const string &name, MetricLevel level, const KVVec &tags) {
    return registerMetric(name, kmonitor::MetricType::STATUS, level, tags);
}

MetricPtr Monitor::registerGaugeMetric(const string &name, MetricLevel level, const KVVec &tags, bool more_stats) {
    return registerMetric(name, GAUGE, level, tags);
}

MetricPtr Monitor::registerGaugePercentileMetric(const string &name, MetricLevel level, const KVVec &tags) {
    return registerMetric(name, SUMMARY, level, tags);
}

MetricPtr Monitor::registerLatencyMetric(const string &name, MetricLevel level, const KVVec &tags) {
    return registerMetric(name, SUMMARY, level, tags);
}

MetricPtr Monitor::registerMetric(const string &name, MetricType metricType, MetricLevel level, const KVVec &tags) {
    // autil::ScopedLock lock(*_metricMutex);
    MetricPtr metric(new Metric(_kmonitor->RegisterMetric(name, metricType, level)));
    metric->addTags(tags);
    return metric;
}

} // namespace kmonitor_adapter
