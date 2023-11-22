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
#include <stdint.h>
#include <string>

#include "autil/Log.h"
//#include "indexlib/util/metrics/Metric.h"
#include "kmonitor/client/MetricsReporter.h"

namespace indexlibv2::document {
class IDocumentBatch;
}

namespace indexlibv2::framework {

// BuildDocumentMetrics将按doc的统计指标汇总在indexlib层(2021-12)。此前，各种指标分散在build_service和indexlib中。
// BuildDocumentMetrics有定期按interval汇报指标的选项，原因是调试过程中发现每个doc都做指标汇报对性能有较大影响(比如在100k
// doc/s的速率下)。
class BuildDocumentMetrics
{
public:
    BuildDocumentMetrics(const std::string& tabletName,
                         const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter);
    ~BuildDocumentMetrics() {}

public:
    void RegisterMetrics();
    void ReportBuildDocumentMetrics(document::IDocumentBatch* doc);
    uint64_t GetTotalDocCount() const { return _totalDocCountMetric.GetCount(); }

private:
    std::string _tabletName;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    int64_t _periodicReportIntervalUS = DEFAULT_REPORT_INTERVAL_US;
    uint64_t _lastBuildDocumentMetricsReportTimeUS = 0;
    uint64_t _successDocCount = 0;

public:
    class Metric
    {
    public:
        void Increase(uint64_t count) { _count += count; }
        void Report()
        {
            assert(_metric);
            _metric->Report(&_tags, _count);
        }
        void ReportAndClear()
        {
            Report();
            _count = 0;
        }
        void Init(const kmonitor::MetricsTags& tags, kmonitor::MutableMetric* metric)
        {
            _tags = tags;
            _metric = metric;
        }
        uint64_t GetCount() const { return _count; }

    private:
        uint64_t _count = 0;
        kmonitor::MetricsTags _tags;
        kmonitor::MutableMetric* _metric = nullptr;
    };

private:
    enum { ADD = 0, DELETE, UPDATE, OTHER, DOC_TYPE_SIZE };
    enum BuildStatus { SUCCESS = 0, FAILED, BUILD_STATUS_SIZE };
    enum { COUNT = 0, QPS, METRIC_TYPE_SIZE };

private:
    // general metrics
    Metric _totalBuildQpsMetric;
    Metric _totalDocCountMetric;
    // metrics of different doc types
    Metric _metrics[DOC_TYPE_SIZE][BUILD_STATUS_SIZE][METRIC_TYPE_SIZE] {};
    // detailed metrics
    // util::MetricPtr _addToUpdateQpsMetric;
    // util::MetricPtr _addToUpdateDocCountMetric;
    // util::MetricPtr _indexAddToUpdateQpsMetric;
    // util::MetricPtr _indexAddToUpdateDocCountMetric;

private:
    static const uint32_t DEFAULT_REPORT_INTERVAL_US = 50'000;

private:
    AUTIL_LOG_DECLARE();
    friend class BuildDocumentMetricsTest;
};

} // namespace indexlibv2::framework
