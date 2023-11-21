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

#include "build_service/common_define.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

using kmonitor::GAUGE;
using kmonitor::QPS;
using kmonitor::STATUS;

#define DECLARE_METRIC(metricProvider, metricName, metricType, unit)                                                   \
    ({                                                                                                                 \
        indexlib::util::MetricPtr metric;                                                                              \
        if ((metricProvider)) {                                                                                        \
            metric = (metricProvider)->DeclareMetric(metricName, metricType);                                          \
            if (metric == nullptr) {                                                                                   \
                std::string errorMsg = std::string("register metric[") + metricName + "] failed";                      \
                BS_LOG(ERROR, "%s", errorMsg.c_str());                                                                 \
            }                                                                                                          \
        }                                                                                                              \
        metric;                                                                                                        \
    })

#define REPORT_METRIC(metric, count)                                                                                   \
    do {                                                                                                               \
        if ((metric)) {                                                                                                \
            (metric)->Report((count));                                                                                 \
        }                                                                                                              \
    } while (0)

#define REPORT_METRIC2(metric, tags, count)                                                                            \
    do {                                                                                                               \
        if ((metric)) {                                                                                                \
            (metric)->Report(tags, (count));                                                                           \
        }                                                                                                              \
    } while (0)

#define INCREASE_QPS(metric)                                                                                           \
    do {                                                                                                               \
        if ((metric)) {                                                                                                \
            (metric)->IncreaseQps();                                                                                   \
        }                                                                                                              \
    } while (0)

#define INCREASE_VALUE(metric, value)                                                                                  \
    do {                                                                                                               \
        if ((metric)) {                                                                                                \
            (metric)->IncreaseQps(value);                                                                              \
        }                                                                                                              \
    } while (0)

#define REPORT_KMONITOR_METRIC(metric, count)                                                                          \
    do {                                                                                                               \
        if ((metric)) {                                                                                                \
            (metric)->report((count));                                                                                 \
        }                                                                                                              \
    } while (0)

#define REPORT_KMONITOR_METRIC2(metric, tags, count)                                                                   \
    do {                                                                                                               \
        if ((metric)) {                                                                                                \
            (metric)->report(tags, count);                                                                             \
        }                                                                                                              \
    } while (0)
