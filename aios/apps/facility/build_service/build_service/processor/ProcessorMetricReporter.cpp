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
#include "build_service/processor/ProcessorMetricReporter.h"

#include "build_service/util/Monitor.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace build_service { namespace processor {

BS_LOG_SETUP(processor, ProcessorMetricReporter);

bool ProcessorMetricReporter::declareMetrics(indexlib::util::MetricProviderPtr metricProvider)
{
    _processLatencyMetric = DECLARE_METRIC(metricProvider, "basic/processLatency", kmonitor::GAUGE, "ms");
    _chainProcessLatencyMetric = DECLARE_METRIC(metricProvider, "basic/chainProcessLatency", kmonitor::GAUGE, "ms");
    _prepareBatchProcessLatencyMetric =
        DECLARE_METRIC(metricProvider, "basic/prepareBatchProcessLatency", kmonitor::GAUGE, "ms");
    _endBatchProcessLatencyMetric =
        DECLARE_METRIC(metricProvider, "basic/endBatchProcessLatency", kmonitor::GAUGE, "ms");
    _processQpsMetric = DECLARE_METRIC(metricProvider, "basic/processQps", kmonitor::QPS, "count");
    _processErrorQpsMetric = DECLARE_METRIC(metricProvider, "debug/processErrorQps", kmonitor::QPS, "count");
    _totalDocCountMetric = DECLARE_METRIC(metricProvider, "statistics/totalDocCount", kmonitor::STATUS, "count");
    _addDocDeleteSubQpsMetric = DECLARE_METRIC(metricProvider, "debug/addDocDeleteSubQps", kmonitor::QPS, "count");
    _rewriteDeleteSubDocQpsMetric =
        DECLARE_METRIC(metricProvider, "debug/rewriteDeleteSubDocQps", kmonitor::QPS, "count");
    return true;
}

void ProcessorMetricReporter::increaseDocCount(uint32_t docCount)
{
    _totalDocCount += docCount;
    REPORT_METRIC(_totalDocCountMetric, _totalDocCount);
}

}} // namespace build_service::processor
