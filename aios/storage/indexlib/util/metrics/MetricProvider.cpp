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
#include "indexlib/util/metrics/MetricProvider.h"

#include "indexlib/util/metrics/IndexlibMetricControl.h"

using namespace autil;
using namespace std;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, MetricProvider);

const string MetricProvider::INDEXLIB_DEFAULT_KMONITOR_SERVICE_NAME = "indexlib";

MetricProvider::MetricProvider(kmonitor::MetricsReporterPtr reporter) noexcept : _reporter(reporter) {}

MetricProvider::~MetricProvider() noexcept {}

MetricProviderPtr MetricProvider::Create(const kmonitor::MetricsReporterPtr& reporter) noexcept
{
    return util::MetricProviderPtr(new util::MetricProvider(reporter));
}

void MetricProvider::setMetricPrefix(const std::string& metricPrefix) noexcept
{
    ScopedLock lock(_lock);
    _metricPrefix = metricPrefix;
}

MetricPtr MetricProvider::DeclareMetric(const std::string& metricName, kmonitor::MetricType type) noexcept
{
    ScopedLock lock(_lock);
    string actualMetricName = _metricPrefix.empty() ? metricName : _metricPrefix + "/" + metricName;
    auto iter = _metricMap.find(actualMetricName);
    if (iter != _metricMap.end()) {
        return iter->second;
    }
    IndexlibMetricControl::Status status = IndexlibMetricControl::GetInstance()->Get(actualMetricName);

    if (status.prohibit) {
        AUTIL_LOG(INFO, "register metric[%s] failed, prohibit to register", actualMetricName.c_str());
        return util::MetricPtr();
    }
    MetricPtr metric(new Metric(_reporter, actualMetricName, type));
    _metricMap.insert(make_pair(actualMetricName, metric));
    return metric;
}
}} // namespace indexlib::util
