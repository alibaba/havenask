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

#include <memory>
#include <stdint.h>

#include "build_service/util/Log.h"

namespace indexlib { namespace util {
class MetricProvider;
class Metric;

typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

namespace build_service { namespace processor {

class ProcessorMetricReporter
{
public:
    ProcessorMetricReporter() : _totalDocCount(0) {}

public:
    void increaseDocCount(uint32_t docCount = 1);

public:
    bool declareMetrics(indexlib::util::MetricProviderPtr metricProvider);
    indexlib::util::MetricPtr _processLatencyMetric;
    indexlib::util::MetricPtr _chainProcessLatencyMetric;
    indexlib::util::MetricPtr _prepareBatchProcessLatencyMetric;
    indexlib::util::MetricPtr _endBatchProcessLatencyMetric;
    indexlib::util::MetricPtr _processQpsMetric;
    indexlib::util::MetricPtr _processErrorQpsMetric;
    indexlib::util::MetricPtr _totalDocCountMetric;
    indexlib::util::MetricPtr _addDocDeleteSubQpsMetric;
    indexlib::util::MetricPtr _rewriteDeleteSubDocQpsMetric;

private:
    uint32_t _totalDocCount;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::processor
