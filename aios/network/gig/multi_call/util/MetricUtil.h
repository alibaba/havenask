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
#ifndef MULTI_CALL_METRICUTIL_H
#define MULTI_CALL_METRICUTIL_H

#include <map>
#include <mutex>
#include <string>

#include "aios/network/gig/multi_call/common/common.h"
#include "kmonitor/client/KMonitor.h"
#include "kmonitor/client/core/MutableMetric.h"

namespace multi_call {

#define DECLARE_METRIC(metric)                                                                     \
public:                                                                                            \
    void report##metric(double value) {                                                            \
        if (metric) {                                                                              \
            metric->Update(value);                                                                 \
        }                                                                                          \
    }                                                                                              \
                                                                                                   \
private:                                                                                           \
    kmonitor::Metric *metric;

#define DEFINE_METRIC(kMonitor, metric, metricName, metricType, metricLevel, metricTags)           \
    ({                                                                                             \
        metric = NULL;                                                                             \
        std::string gigMetricName = std::string("gig.") + metricName;                              \
        static std::once_flag once_##metric;                                                       \
        std::call_once(once_##metric, [&]() {                                                      \
            if ((kMonitor)) {                                                                      \
                kMonitor->Register(gigMetricName, kmonitor::metricType, kmonitor::metricLevel);    \
            }                                                                                      \
        });                                                                                        \
        if ((kMonitor && metricTags)) {                                                            \
            metric = (kMonitor)->DeclareMetric(                                                    \
                gigMetricName, const_cast<kmonitor::MetricsTags *>(metricTags.get()));             \
            if (!metric) {                                                                         \
                AUTIL_LOG(ERROR, "declare metric[%s] failed", metricName);                         \
            }                                                                                      \
        }                                                                                          \
    })

#define DECLARE_MUTABLE_METRIC(metric)                                                             \
public:                                                                                            \
    void report##metric(double value, const kmonitor::MetricsTagsPtr &tags) {                      \
        if (metric) {                                                                              \
            metric->Report(tags, value);                                                           \
        }                                                                                          \
    }                                                                                              \
    void report##metric(double value, const std::map<std::string, std::string> &kv) {              \
        if (metric) {                                                                              \
            const auto tags = _kMonitor->GetMetricsTags(kv);                                       \
            metric->Report(tags, value);                                                           \
        }                                                                                          \
    }                                                                                              \
                                                                                                   \
private:                                                                                           \
    std::shared_ptr<kmonitor::MutableMetric> metric;

#define DEFINE_MUTABLE_METRIC(kMonitor, metric, metricName, metricType, metricLevel)               \
    {                                                                                              \
        metric = NULL;                                                                             \
        std::string gigMetricName = std::string("gig.") + metricName;                              \
        if (kMonitor) {                                                                            \
            metric.reset(kMonitor->RegisterMetric(gigMetricName, kmonitor::metricType,             \
                                                  kmonitor::metricLevel));                         \
        }                                                                                          \
    }

class MetricUtil
{
public:
    static std::string normalizeTag(const std::string &tag);
    static std::string normalizeEmpty(const std::string &tag);
};

} // namespace multi_call
#endif // MULTI_CALL_METRICUTIL_H
