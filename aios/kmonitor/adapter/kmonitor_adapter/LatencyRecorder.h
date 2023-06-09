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

#include <atomic>

#include "kmonitor_adapter/GaugeRecorder.h"
#include "kmonitor_adapter/MonitorFactory.h"
#include "kmonitor_adapter/ThreadData.h"
#include "kmonitor_adapter/Time.h"

namespace kmonitor_adapter {

class LatencyRecorder : public GaugeRecorder
{
public:
    LatencyRecorder(const std::string& name, MetricPtr metric) : GaugeRecorder(name, std::move(metric)) {}
    LatencyRecorder(Monitor* monitor, const std::string& metricName, kmonitor::MetricLevel level,
                    const Metric::KVVec& tags = Metric::KVVec())
        : GaugeRecorder()
    {
        assert(monitor);
        GaugeRecorder::_name = metricName;
        GaugeRecorder::_metric = monitor->registerGaugePercentileMetric(metricName, level, tags);
        assert(GaugeRecorder::_metric);
        registerRecorder();
    }

    uint64_t processValue(uint64_t val) override { return time::getMicroSeconds(val); }
};

} // namespace kmonitor_adapter
