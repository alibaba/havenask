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
#include <string>

#include "indexlib/util/metrics/IndexlibMetricControl.h"
#include "indexlib/util/metrics/Metric.h"
#include "kmonitor/client/MetricType.h"
#include "kmonitor/client/MetricsReporter.h"

namespace indexlibv2::framework {

#define INDEXLIB_FM_DECLARE_METRIC(metricName)                                                                         \
    std::shared_ptr<indexlib::util::Metric> _##metricName##Metric;                                                     \
                                                                                                                       \
public:                                                                                                                \
    const std::shared_ptr<indexlib::util::Metric>& Get##metricName##Metric() const { return _##metricName##Metric; }

#define INDEXLIB_FM_DECLARE_NORMAL_METRIC(MetricValueType, metricName)                                                 \
private:                                                                                                               \
    MetricValueType _##metricName = 0;                                                                                 \
    std::shared_ptr<indexlib::util::Metric> _##metricName##Metric;                                                     \
                                                                                                                       \
public:                                                                                                                \
    void Increase##metricName##Value(MetricValueType incValue) { _##metricName += incValue; }                          \
    void Decrease##metricName##Value(MetricValueType decValue) { _##metricName -= decValue; }                          \
    MetricValueType Get##metricName##Value() const { return _##metricName; }                                           \
    void Set##metricName##Value(MetricValueType value) { _##metricName = value; }

#define REGISTER_METRIC_WITH_INDEXLIB_PREFIX(metricsReporter, metricName, metricKey, metricType)                       \
    do {                                                                                                               \
        const std::string actualMetricKey = std::string("indexlib/") + metricKey;                                      \
        indexlib::util::IndexlibMetricControl::Status status =                                                         \
            indexlib::util::IndexlibMetricControl::GetInstance()->Get(actualMetricKey);                                \
        if (status.prohibit) {                                                                                         \
            AUTIL_LOG(INFO, "register metric[%s] failed, prohibit to register", actualMetricKey.c_str());              \
            _##metricName##Metric = nullptr;                                                                           \
            break;                                                                                                     \
        }                                                                                                              \
        _##metricName##Metric =                                                                                        \
            std::make_shared<indexlib::util::Metric>(metricsReporter, actualMetricKey, metricType);                    \
    } while (0)

#define INDEXLIB_FM_REPORT_METRIC(metricName)                                                                          \
    do {                                                                                                               \
        if ((_##metricName##Metric != nullptr)) {                                                                      \
            (_##metricName##Metric)->Report((_##metricName));                                                          \
        }                                                                                                              \
    } while (0)

#define INDEXLIB_FM_REPORT_METRIC_WITH_TAGS(tags, metricName)                                                          \
    do {                                                                                                               \
        if ((_##metricName##Metric != nullptr)) {                                                                      \
            (_##metricName##Metric)->Report(tags, (_##metricName));                                                    \
        }                                                                                                              \
    } while (0)

#define INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(tags, metricName, value)                                         \
    do {                                                                                                               \
        if ((_##metricName##Metric != nullptr)) {                                                                      \
            (_##metricName##Metric)->Report(tags, (value));                                                            \
        }                                                                                                              \
    } while (0)

#define INDEXLIB_FM_REPORT_METRIC_WITH_VALUE(metricName, value)                                                        \
    do {                                                                                                               \
        if ((_##metricName##Metric != nullptr)) {                                                                      \
            (_##metricName##Metric)->Report((value));                                                                  \
        }                                                                                                              \
    } while (0)

#define INDEXLIB_FM_INCREASE_QPS(metricName)                                                                           \
    do {                                                                                                               \
        if ((_##metricName##Metric) != nullptr) {                                                                      \
            (_##metricName##Metric)->IncreaseQps();                                                                    \
        }                                                                                                              \
    } while (0)

} // namespace indexlibv2::framework
