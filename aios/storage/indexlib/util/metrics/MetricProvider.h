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

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/util/metrics/Metric.h"
#include "kmonitor/client/MetricType.h"
#include "kmonitor/client/MetricsReporter.h"

namespace indexlib { namespace util {

class MetricProvider;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;

class MetricProvider
{
public:
    MetricProvider(kmonitor::MetricsReporterPtr reporter = nullptr) noexcept;
    ~MetricProvider() noexcept;

public:
    kmonitor::MetricsReporterPtr GetReporter() noexcept { return _reporter; }
    MetricPtr DeclareMetric(const std::string& metricName, kmonitor::MetricType type) noexcept;
    void setMetricPrefix(const std::string& metricPrefix) noexcept;

public:
    static MetricProviderPtr Create(const kmonitor::MetricsReporterPtr& reporter) noexcept;

public:
    static const std::string INDEXLIB_DEFAULT_KMONITOR_SERVICE_NAME;

private:
    AUTIL_LOG_DECLARE();

private:
    mutable autil::RecursiveThreadMutex _lock;
    kmonitor::MetricsReporterPtr _reporter;
    std::map<std::string, MetricPtr> _metricMap;
    std::string _metricPrefix;
};
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
}} // namespace indexlib::util
