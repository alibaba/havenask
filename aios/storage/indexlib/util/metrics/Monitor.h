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

#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

// using kmonitor::GAUGE;
// using kmonitor::QPS;
// using kmonitor::STATUS;

#define IE_DECLARE_METRIC(metric) ::indexlib::util::MetricPtr m##metric##Metric;

#define IE_DECLARE_REACHABLE_METRIC(metric)                                                                            \
    util::MetricPtr m##metric##Metric;                                                                                 \
                                                                                                                       \
public:                                                                                                                \
    const util::MetricPtr& Get##metric##Metric() const { return m##metric##Metric; }

#define IE_DECLARE_PARAM_METRIC(ParamType, metric)                                                                     \
public:                                                                                                                \
    ParamType m##metric = 0;                                                                                           \
    util::MetricPtr m##metric##Metric;                                                                                 \
                                                                                                                       \
public:                                                                                                                \
    void Increase##metric##Value(ParamType incValue) { m##metric += incValue; }                                        \
    void Decrease##metric##Value(ParamType decValue) { m##metric -= decValue; }                                        \
    ParamType Get##metric##Value() const { return m##metric; }                                                         \
    void Set##metric##Value(ParamType value) { m##metric = value; }

#define IE_INIT_METRIC_GROUP(metricProvider, metric, metricName, metricType, unit)                                     \
    do {                                                                                                               \
        if (!metricProvider) {                                                                                         \
            break;                                                                                                     \
        }                                                                                                              \
        std::string declareMetricName = std::string("indexlib/") + metricName;                                         \
        m##metric##Metric = metricProvider->DeclareMetric(declareMetricName, metricType);                              \
    } while (0)

#define IE_INIT_LOCAL_INPUT_METRIC(reporter, metric)                                                                   \
    do {                                                                                                               \
        reporter.Init((m##metric##Metric));                                                                            \
    } while (0)

#define IE_REPORT_METRIC(metric, count)                                                                                \
    do {                                                                                                               \
        if ((m##metric##Metric)) {                                                                                     \
            (m##metric##Metric)->Report((count));                                                                      \
        }                                                                                                              \
    } while (0)

#define IE_REPORT_METRIC_WITH_TAGS(metric, tags, count)                                                                \
    do {                                                                                                               \
        if ((m##metric##Metric)) {                                                                                     \
            (m##metric##Metric)->Report(tags, (count));                                                                \
        }                                                                                                              \
    } while (0)

#define IE_INCREASE_QPS(metric)                                                                                        \
    do {                                                                                                               \
        if ((m##metric##Metric)) {                                                                                     \
            (m##metric##Metric)->IncreaseQps();                                                                        \
        }                                                                                                              \
    } while (0)

#define IE_INCREASE_QPS_WITH_TAGS(metric, tags)                                                                        \
    do {                                                                                                               \
        if ((m##metric##Metric)) {                                                                                     \
            (m##metric##Metric)->IncreaseQps((double)1.0, tags);                                                       \
        }                                                                                                              \
    } while (0)
