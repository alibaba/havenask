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
#include "indexlib/framework/index_task/IndexOperationMetrics.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlibv2.framework, IndexOperationMetrics);

IndexOperationMetrics::IndexOperationMetrics(const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter,
                                             const kmonitor::MetricsTags& tags)
    : _metricsReporter(metricsReporter)
    , _tags(tags)
{
}

IndexOperationMetrics::~IndexOperationMetrics() {}

void IndexOperationMetrics::RegisterMetrics()
{
#define REGISTER_INDEX_OP_METRIC(metricName)                                                                           \
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, metricName, "index_task/" #metricName, kmonitor::GAUGE)

    REGISTER_INDEX_OP_METRIC(operationExecuteTime);
}

void IndexOperationMetrics::ReportMetrics()
{
    INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&_tags, operationExecuteTime, _timer.done_us());
}

int64_t IndexOperationMetrics::GetTimerDoneUs() { return _timer.done_us(); }

} // namespace indexlibv2::framework
