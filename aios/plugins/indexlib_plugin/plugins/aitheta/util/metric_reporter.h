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
#ifndef INDEXLIB_PLUGIN_PLUGINS_AITHETA_METRIC_REPORTER_H
#define INDEXLIB_PLUGIN_PLUGINS_AITHETA_METRIC_REPORTER_H

#include "autil/Lock.h"
#include "autil/TimeUtility.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"

namespace indexlib {
namespace aitheta_plugin {

class Metric {
 public:
    Metric(indexlib::util::MetricPtr metric, const kmonitor::MetricsTags& tags)
        : mKmonMetric(metric), mKmonTags(tags) {}

 public:
    void Report(double value) { mKmonMetric->Report(&mKmonTags, value); }

 private:
    indexlib::util::MetricPtr mKmonMetric;
    const kmonitor::MetricsTags& mKmonTags;
};
typedef std::shared_ptr<Metric> MetricPtr;

class MetricReporter {
 public:
    MetricReporter(indexlib::util::MetricProviderPtr metricProvider, const std::string& tableName = "DEFAULT",
                   const std::string& indexName = "EMBEDDING")
        : mMetricProvider(metricProvider) {
        mTags.AddTag("index_name", indexName);
        mTags.AddTag("table_name", tableName);
    }

    MetricPtr Declare(const std::string& name, kmonitor::MetricType type) {
        if (likely(mMetricProvider != nullptr)) {
            auto kmonMetric = mMetricProvider->DeclareMetric(name, type);
            return MetricPtr(new Metric(kmonMetric, mTags));
        }
        return MetricPtr();
    }
    const kmonitor::MetricsTags& getTags() const { return mTags; }

 private:
    indexlib::util::MetricProviderPtr mMetricProvider;
    kmonitor::MetricsTags mTags;
};
typedef std::shared_ptr<MetricReporter> MetricReporterPtr;

class ScopedLatencyReporter {
 public:
    ScopedLatencyReporter(MetricPtr& metric) : mBegin(0l), mMetric(metric) {
        if (mMetric) {
            mBegin = autil::TimeUtility::currentTime();
        }
    }
    ~ScopedLatencyReporter() {
        if (mMetric) {
            mMetric->Report((autil::TimeUtility::currentTime() - mBegin) / 1000.0);
        }
    }

 private:
    int64_t mBegin;
    MetricPtr& mMetric;
};

#define METRIC_SETUP(metric, name, type)               \
    if (_metricReporter) {                             \
        metric = _metricReporter->Declare(name, type); \
    }

#define METRIC_REPORT(metric, value) \
    if (metric) {                    \
        metric->Report(value);       \
    }

#define QPS_REPORT(metric)   \
    if (metric) {            \
        metric->Report(1.0); \
    }

#define METRIC_DECLARE(name) MetricPtr name

}
}

#endif  // INDEXLIB_PLUGIN_PLUGINS_AITHETA_METRIC_REPORTER_H
