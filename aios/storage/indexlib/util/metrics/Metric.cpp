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
#include "indexlib/util/metrics/Metric.h"

#include "autil/StringUtil.h"
#include "indexlib/util/metrics/IndexlibMetricControl.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, Metric);

Metric::Metric(const kmonitor::MetricsReporterPtr& reporter, const std::string& metricName,
               kmonitor::MetricType type) noexcept
    : _reporter(reporter)
    , _type(type)
    , _value(std::numeric_limits<double>::min())
{
    _metricName = metricName;
    StringUtil::replaceAll(_metricName, "/", ".");
}

Metric::~Metric() noexcept
{
    if (_type == kmonitor::MetricType::STATUS && _reporter) {
        // when index partition is unload, should unregister status metric to invoid reporting this metric
        _reporter->unregister(_metricName);
    }
}
}} // namespace indexlib::util
