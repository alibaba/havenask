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

#include "kmonitor/client/KMonitor.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "kmonitor_adapter/LightMetric.h"

namespace kmonitor_adapter {

class Metric;
typedef std::unique_ptr<Metric> MetricPtr;

class Metric {
public:
    Metric(kmonitor::MutableMetric *metric) : _metric(metric) {}
    Metric(const Metric &other) = delete;
    ~Metric() {
        if (_tagsMetric) {
            _metric->UndeclareMetric(_tagsMetric);
            _tagsMetric = NULL;
        }
    }

public:
    // immutable tags
    typedef std::vector<std::pair<std::string, std::string>> KVVec;
    void addTags(const KVVec &tagPairs) {
        kmonitor::MetricsTags tags;
        for (auto kvPair : tagPairs) {
            tags.AddTag(kvPair.first, kvPair.second);
        }
        addTags(tags);
    }

    void addTags(kmonitor::MetricsTags &tags) {
        assert(_metric);
        assert(!_tagsMetric);
        _tagsMetric = _metric->DeclareMetric(&tags);
    }

    void report(double value) {
        if (_tagsMetric) {
            _tagsMetric->Update(value);
        }
    }

    LightMetricPtr declareLightMetric(kmonitor::MetricsTags &tags) {
        return LightMetricPtr(new LightMetric(_metric->DeclareMetric(&tags)));
    }

public:
    // mutable tags
    void report(kmonitor::MetricsTags &tags, double value) {
        assert(_metric);
        _metric->Report(&tags, value);
    }
    void report(const kmonitor::MetricsTagsPtr &tags, double value) {
        assert(_metric);
        _metric->Report(tags, value);
    }

protected:
    std::unique_ptr<kmonitor::MutableMetric> _metric = nullptr;
    kmonitor::Metric *_tagsMetric = nullptr;
};

#define KMONITOR_ADAPTER_REPORT(metric, value)                                                                         \
    if (metric) {                                                                                                      \
        metric->report(value);                                                                                         \
    }

#define KMONITOR_ADAPTER_REPORT_TAGS(metric, tag, value)                                                               \
    if (metric) {                                                                                                      \
        metric->report(tag, value);                                                                                    \
    }

} // namespace kmonitor_adapter
