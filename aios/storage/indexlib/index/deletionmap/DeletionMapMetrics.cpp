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
#include "indexlib/index/deletionmap/DeletionMapMetrics.h"

#include "indexlib/framework/MetricsWrapper.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DeletionMapMetrics);

DeletionMapMetrics::DeletionMapMetrics(const std::string& metricsName,
                                       const std::shared_ptr<kmonitor::MetricsTags>& metricsTags,
                                       const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter,
                                       std::function<uint32_t()> function)
    : _metricsTags(metricsTags)
    , _metricsReporter(metricsReporter)
    , _getMetricValueFunc(function)
    , _stop(true)
{
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, report, metricsName, kmonitor::GAUGE);
}

DeletionMapMetrics::~DeletionMapMetrics() {}

void DeletionMapMetrics::Start()
{
    std::lock_guard<std::mutex> guard(_mutex);
    _stop = false;
}

void DeletionMapMetrics::Stop()
{
    std::lock_guard<std::mutex> guard(_mutex);
    _stop = true;
}

void DeletionMapMetrics::ReportMetrics()
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (!_stop) {
        _reportMetric->Report(_metricsTags.get(), _getMetricValueFunc());
    }
}
void DeletionMapMetrics::RegisterMetrics() {}

} // namespace indexlibv2::index
