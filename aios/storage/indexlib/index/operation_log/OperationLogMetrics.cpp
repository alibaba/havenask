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
#include "indexlib/index/operation_log/OperationLogMetrics.h"

#include "indexlib/framework/MetricsWrapper.h"
#include "kmonitor/client/core/MetricsTags.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, OperationLogMetrics);
OperationLogMetrics::OperationLogMetrics(segmentid_t segmentid,
                                         const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter,
                                         std::function<size_t()> func)
    : _metricsReporter(metricsReporter)
    , _getOperationCountFunc(func)
    , _stop(false)
{
    _tags.reset(new kmonitor::MetricsTags("segmentid", std::to_string(segmentid)));
}

OperationLogMetrics::~OperationLogMetrics() {}

void OperationLogMetrics::Stop()
{
    std::lock_guard<std::mutex> guard(_mutex);
    _stop = true;
}

void OperationLogMetrics::ReportMetrics()
{
    std::lock_guard<std::mutex> guard(_mutex);
    if (!_stop) {
        _operationWriterTotalOpCountMetric->Report(_tags.get(), _getOperationCountFunc());
    }
}
void OperationLogMetrics::RegisterMetrics()
{
    std::lock_guard<std::mutex> guard(_mutex);
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, operationWriterTotalOpCount,
                                         "build/operationWriterTotalOpCount", kmonitor::GAUGE);
}

} // namespace indexlib::index
