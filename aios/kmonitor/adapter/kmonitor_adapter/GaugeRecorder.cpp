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
#include "kmonitor_adapter/GaugeRecorder.h"

#include <cassert>
#include <unistd.h>

#include "autil/Log.h"
#include "kmonitor_adapter/MonitorFactory.h"

namespace kmonitor_adapter {

GaugeRecorder::GaugeRecorder(const std::string &name, MetricPtr metric)
    : _name(name), _metric(std::move(metric)), _mergedSum(0), _mergedNum(0) {
    registerRecorder();
}

GaugeRecorder::GaugeRecorder(Monitor *monitor,
                             const std::string &metricName,
                             kmonitor::MetricLevel level,
                             const Metric::KVVec &tags)
    : _name(metricName), _mergedSum(0), _mergedNum(0) {
    assert(monitor);
    _metric = monitor->registerGaugeMetric(metricName, level, tags);
    assert(_metric);
    registerRecorder();
}

GaugeRecorder::~GaugeRecorder() { unregisterRecoder(); }

} // namespace kmonitor_adapter
