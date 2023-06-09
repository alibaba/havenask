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
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "autil/Log.h"
#include "indexlib/framework/IMetrics.h"
namespace indexlib { namespace util {
class Metric;
}} // namespace indexlib::util
namespace kmonitor {
class MetricsTags;
class MetricsReporter;
class MetricsTags;
} // namespace kmonitor

namespace indexlibv2::index {

class DeletionMapMetrics : public framework::IMetrics
{
public:
    DeletionMapMetrics(const std::string& metricsName, const std::shared_ptr<kmonitor::MetricsTags>& metricsTags,
                       const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter,
                       std::function<uint32_t()> function);
    ~DeletionMapMetrics();

public:
    void ReportMetrics() override;
    void RegisterMetrics() override;
    void Stop();
    void Start();

private:
    std::shared_ptr<kmonitor::MetricsTags> _metricsTags;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    std::function<uint32_t()> _getMetricValueFunc;
    std::shared_ptr<indexlib::util::Metric> _reportMetric;
    std::mutex _mutex;
    bool _stop;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
