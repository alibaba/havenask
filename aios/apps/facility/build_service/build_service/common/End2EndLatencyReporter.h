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

#include <atomic>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/util/metrics/Metric.h"
#include "kmonitor/client/core/MetricsTags.h"

namespace indexlib { namespace util {
class MetricProvider;

typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
}} // namespace indexlib::util

namespace build_service { namespace common {

class End2EndLatencyReporter
{
public:
    static const kmonitor::MetricsTags TAGS_IS_RECOVERED;
    static const kmonitor::MetricsTags TAGS_NOT_RECOVERED;

public:
    End2EndLatencyReporter();
    ~End2EndLatencyReporter();

private:
    End2EndLatencyReporter(const End2EndLatencyReporter&);
    End2EndLatencyReporter& operator=(const End2EndLatencyReporter&);

public:
    void init(indexlib::util::MetricProviderPtr metricPreovider);
    void setIsServiceRecovered(bool isServiceRecovered) { _isServiceRecovered = isServiceRecovered; }
    void report(const std::string& source, int64_t latency);
    void reportMaxLatency(int64_t latency);

private:
    indexlib::util::MetricPtr getMetric(const std::string& source);

private:
    typedef std::map<std::string, indexlib::util::MetricPtr> MetricMap;
    indexlib::util::MetricProviderPtr _metricProvider;
    indexlib::util::MetricPtr _end2EndLatencyMetric;
    indexlib::util::MetricPtr _maxEnd2EndLatencyMetric;
    MetricMap _end2EndLatencyMetricMap;
    std::atomic<bool> _isServiceRecovered;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(End2EndLatencyReporter);

}} // namespace build_service::common
