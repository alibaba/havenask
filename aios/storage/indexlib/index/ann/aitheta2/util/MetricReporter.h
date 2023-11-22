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
#include "autil/TimeUtility.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlibv2::index::ann {

class Metric
{
public:
    Metric(indexlib::util::MetricPtr metric, std::shared_ptr<kmonitor::MetricsTags> tags)
        : _metric(metric)
        , _kmonTags(tags)
    {
    }

public:
    void Report(double value) { _metric->Report(_kmonTags, value); }
    void Report(const std::shared_ptr<kmonitor::MetricsTags> tags, double value) { _metric->Report(tags, value); }

private:
    indexlib::util::MetricPtr _metric;
    std::shared_ptr<kmonitor::MetricsTags> _kmonTags;
};
typedef std::shared_ptr<Metric> MetricPtr;

class MetricReporter
{
public:
    MetricReporter(const indexlib::util::MetricProviderPtr& metricProvider, const std::string& indexName,
                   segmentid_t segmentId = INVALID_SEGMENTID, bool isRtSegment = false)
        : _metricProvider(metricProvider)
        , _tags(std::make_shared<kmonitor::MetricsTags>())
    {
        _tags->AddTag("index", indexName);
        _tags->AddTag("index_type", "aitheta2");
        if (segmentId != INVALID_SEGMENTID) {
            _tags->AddTag("segment_id", autil::StringUtil::toString(segmentId));
            _tags->AddTag("segment_type", isRtSegment ? "realtime" : "normal");
        }
    }

    MetricPtr Declare(const std::string& name, kmonitor::MetricType type)
    {
        if (likely(_metricProvider != nullptr)) {
            auto kmonMetric = _metricProvider->DeclareMetric(name, type);
            return MetricPtr(new Metric(kmonMetric, _tags));
        }
        return MetricPtr();
    }
    std::shared_ptr<kmonitor::MetricsTags> getTags() const { return _tags; }

private:
    indexlib::util::MetricProviderPtr _metricProvider;
    std::shared_ptr<kmonitor::MetricsTags> _tags;
};
typedef std::shared_ptr<MetricReporter> MetricReporterPtr;

class ScopedLatencyReporter
{
public:
    ScopedLatencyReporter(MetricPtr& metric) : _begin(0l), _metric(metric)
    {
        if (_metric) {
            _begin = autil::TimeUtility::currentTime();
        }
    }
    ~ScopedLatencyReporter()
    {
        if (_metric) {
            _metric->Report((autil::TimeUtility::currentTime() - _begin) / 1000.0);
        }
    }

private:
    int64_t _begin;
    MetricPtr& _metric;
};

#define METRIC_SETUP(metric, name, type)                                                                               \
    if (_metricReporter) {                                                                                             \
        metric = _metricReporter->Declare(name, type);                                                                 \
    }

#define METRIC_REPORT(metric, value)                                                                                   \
    if (metric) {                                                                                                      \
        metric->Report(value);                                                                                         \
    }

#define QPS_REPORT(metric)                                                                                             \
    if (metric) {                                                                                                      \
        metric->Report(1.0);                                                                                           \
    }

#define METRIC_DECLARE(name) indexlibv2::index::ann::MetricPtr name

} // namespace indexlibv2::index::ann
