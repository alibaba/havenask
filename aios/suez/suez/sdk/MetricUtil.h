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

#include "autil/Log.h"
#include "kmonitor/client/MetricLevel.h"

namespace suez {

static const kmonitor::MetricLevel kDefaultMetricLevel = kmonitor::NORMAL;

// register base

#define REGISTER_METRIC_BASE(target, name, metricType, level, tags)                                                    \
    do {                                                                                                               \
        if (nullptr == monitor) {                                                                                      \
            target = nullptr;                                                                                          \
            return;                                                                                                    \
        }                                                                                                              \
        std::string metricName = (name);                                                                               \
        monitor->Register(metricName, (metricType), (level));                                                          \
        target = monitor->DeclareMetric(metricName, tags);                                                             \
        if (nullptr == target) {                                                                                       \
            AUTIL_LOG(ERROR, "failed to register metric:[%s]", metricName.c_str());                                    \
        }                                                                                                              \
    } while (0)

#define REGISTER_QPS_METRIC_BASE(target, name, level, tags)                                                            \
    REGISTER_METRIC_BASE(target, name, kmonitor::QPS, level, tags)

#define REGISTER_GAUGE_METRIC_BASE(target, name, level, tags)                                                          \
    REGISTER_METRIC_BASE(target, name, kmonitor::GAUGE, level, tags)

#define REGISTER_SUMMARY_GAUGE_METRIC_BASE(target, name, level, tags)                                                  \
    REGISTER_METRIC_BASE(target, name, kmonitor::SUMMARY, level, tags)

// register shortcut

#define REGISTER_GAUGE_METRIC(target, name) REGISTER_GAUGE_METRIC_BASE(target, name, kDefaultMetricLevel, tags)

#define REGISTER_LENGTH_METRIC(target, name) REGISTER_METRIC(target, name, kmonitor::GAUGE, kDefaultMetricLevel, tags)

#define REGISTER_LATENCY_METRIC(target, name)                                                                          \
    REGISTER_SUMMARY_GAUGE_METRIC_BASE(target, name, kDefaultMetricLevel, tags)

#define REGISTER_QPS_METRIC(target, name) REGISTER_QPS_METRIC_BASE(target, name, kDefaultMetricLevel, tags)

// register without tags shortcut

#define REGISTER_GAUGE_METRIC_NOTAGS(target, name)                                                                     \
    REGISTER_GAUGE_METRIC_BASE(target, name, kDefaultMetricLevel, nullptr)

#define REGISTER_LENGTH_METRIC_NOTAGS(target, name)                                                                    \
    REGISTER_METRIC(target, name, kmonitor::GAUGE, kDefaultMetricLevel, nullptr)

#define REGISTER_LATENCY_METRIC_NOTAGS(target, name)                                                                   \
    REGISTER_SUMMARY_GAUGE_METRIC_BASE(target, name, kDefaultMetricLevel, nullptr)

#define REGISTER_QPS_METRIC_NOTAGS(target, name) REGISTER_QPS_METRIC_BASE(target, name, kDefaultMetricLevel, nullptr)

// report shortcut

#define REPORT_METRIC(metric, value)                                                                                   \
    if (metric) {                                                                                                      \
        metric->Update(value);                                                                                         \
    }

#define REPORT_QPS(metric) REPORT_METRIC(metric, 1)

} // namespace suez
