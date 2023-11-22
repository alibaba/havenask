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
#include "build_service/common/End2EndLatencyReporter.h"

#include <cstddef>
#include <utility>

#include "alog/Logger.h"
#include "build_service/util/Monitor.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "kmonitor/client/MetricType.h"

using namespace std;

using namespace indexlib::util;

namespace build_service { namespace common {
BS_LOG_SETUP(common, End2EndLatencyReporter);

const kmonitor::MetricsTags End2EndLatencyReporter::TAGS_IS_RECOVERED = {"service_recovered", "true"};
const kmonitor::MetricsTags End2EndLatencyReporter::TAGS_NOT_RECOVERED = {"service_recovered", "false"};

End2EndLatencyReporter::End2EndLatencyReporter() : _isServiceRecovered(false) {}

End2EndLatencyReporter::~End2EndLatencyReporter() {}

void End2EndLatencyReporter::init(MetricProviderPtr metricProvider)
{
    _metricProvider = metricProvider;
    _end2EndLatencyMetric = DECLARE_METRIC(_metricProvider, "basic/end2EndLatency", kmonitor::GAUGE, "ms");
    _maxEnd2EndLatencyMetric = DECLARE_METRIC(_metricProvider, "basic/maxEnd2EndLatency", kmonitor::GAUGE, "ms");
}

void End2EndLatencyReporter::reportMaxLatency(int64_t latency)
{
    if (_isServiceRecovered.load()) {
        REPORT_METRIC2(_maxEnd2EndLatencyMetric, &TAGS_IS_RECOVERED, latency);
    } else {
        REPORT_METRIC2(_maxEnd2EndLatencyMetric, &TAGS_NOT_RECOVERED, latency);
    }
}

void End2EndLatencyReporter::report(const std::string& source, int64_t latency)
{
    if (_isServiceRecovered.load()) {
        REPORT_METRIC2(_end2EndLatencyMetric, &TAGS_IS_RECOVERED, latency);
    } else {
        REPORT_METRIC2(_end2EndLatencyMetric, &TAGS_NOT_RECOVERED, latency);
    }

    if (source.empty()) {
        return;
    }
    indexlib::util::MetricPtr metric = getMetric(source);

    if (_isServiceRecovered.load()) {
        REPORT_METRIC2(metric, &TAGS_IS_RECOVERED, latency);
    } else {
        REPORT_METRIC2(metric, &TAGS_NOT_RECOVERED, latency);
    }
}

indexlib::util::MetricPtr End2EndLatencyReporter::getMetric(const string& source)
{
    MetricMap::iterator iter = _end2EndLatencyMetricMap.find(source);
    if (iter != _end2EndLatencyMetricMap.end()) {
        return iter->second;
    }
    if (!_metricProvider) {
        return NULL;
    }
    indexlib::util::MetricPtr metric;
    string metricName = "basic/end2EndLatency_" + source;
    metric = _metricProvider->DeclareMetric(metricName, kmonitor::GAUGE);
    if (!metric) {
        BS_LOG(ERROR, "register metric [%s] failed.", metricName.c_str());
    }
    _end2EndLatencyMetricMap[source] = metric;
    return metric;
}

}} // namespace build_service::common
