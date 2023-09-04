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

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "kmonitor/client/MetricsReporter.h"

namespace indexlib { namespace util {

// notice: no need to declare MutableMetrics first, just report it
class Metric
{
public:
    Metric(const kmonitor::MetricsReporterPtr& reporter, const std::string& metricName,
           kmonitor::MetricType type) noexcept;
    ~Metric() noexcept;

public:
    [[deprecated("arg 'needSummary' is useless")]] void Report(double value, bool needSummary) noexcept
    {
        Report(value);
    }
    void Report(double value) noexcept
    {
        _value = value;
        if (_reporter) {
            _reporter->report(value, _metricName, _type, nullptr);
        }
    }
    void Report(const kmonitor::MetricsTags* tags, double value) noexcept
    {
        _value = value;
        if (_reporter) {
            _reporter->report(value, _metricName, _type, tags);
        }
    }
    void Report(const kmonitor::MetricsTagsPtr& tags, double value) noexcept
    {
        _value = value;
        if (_reporter) {
            _reporter->report(value, _metricName, _type, tags.get());
        }
    }

    void IncreaseQps(double value = 1.0, const kmonitor::MetricsTags* tags = nullptr) noexcept
    {
        _value++;
        if (_reporter) {
            _reporter->report(value, _metricName, _type, tags);
        }
    }

    double TEST_GetValue() noexcept { return _value; }

public:
    void TEST_SetValue(double value) noexcept { _value = value; }

public:
    static void IncreaseQps(std::shared_ptr<Metric> metric) noexcept
    {
        if (metric != nullptr) {
            metric->IncreaseQps();
        }
    }
    static void IncreaseQps(std::shared_ptr<Metric> metric, uint32_t value) noexcept
    {
        if (metric != nullptr) {
            metric->IncreaseQps(value);
        }
    }
    static void ReportMetric(std::shared_ptr<Metric> metric, uint32_t count) noexcept
    {
        if (metric != nullptr) {
            metric->Report(count);
        }
    }

private:
    kmonitor::MetricsReporterPtr _reporter;
    std::string _metricName;
    kmonitor::MetricType _type;
    double _value = 0.0;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<Metric> MetricPtr;

class ScopeLatencyReporter
{
public:
    ScopeLatencyReporter(Metric* metric) noexcept : _metric(metric) {}
    ~ScopeLatencyReporter() noexcept
    {
        if (_metric != NULL) {
            _metric->Report(_timer.done_ms());
        }
    }
    const autil::ScopedTime2& GetTimer() const noexcept { return _timer; }

private:
    autil::ScopedTime2 _timer;
    Metric* _metric;
};
}} // namespace indexlib::util
