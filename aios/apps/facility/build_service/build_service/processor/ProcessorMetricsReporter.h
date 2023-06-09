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
#ifndef ISEARCH_BS_PROCESSORMETRICSREPORTER_H
#define ISEARCH_BS_PROCESSORMETRICSREPORTER_H

namespace indexlib { namespace util {
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

struct ProcessorMetricsReporter {
    bool declareMetrics(indexlib::util::MetricProviderPtr metricProvider);
    indexlib::util::MetricPtr processLatencyMetric;
    indexlib::util::MetricPtr chainProcessLetencyMetric;
    indexlib::util::MetricPtr freshnessMetric;
    indexlib::util::MetricPtr errorQpsMetric;
    indexlib::util::MetricPtr waitProcessDocCount;
};

#endif // ISEARCH_BS_PROCESSORMETRICSREPORTER_H
