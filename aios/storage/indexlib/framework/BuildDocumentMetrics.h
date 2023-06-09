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
#include <string>

#include "indexlib/framework/MetricsWrapper.h"

namespace indexlibv2::document {
class IDocumentBatch;
}

namespace indexlibv2::framework {

// BuildDocumentMetrics将按doc的统计指标汇总在indexlib层(2021-12)。此前，各种指标分散在build_service和indexlib中。
// BuildDocumentMetrics有定期按interval汇报指标的选项，原因是调试过程中发现每个doc都做指标汇报对性能有较大影响(比如在100k
// doc/s的速率下)。
class BuildDocumentMetrics
{
    enum DocOperateResult { SUCCESS = 0, FAIL = 1 };

public:
    BuildDocumentMetrics(const std::string& tabletName,
                         const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter);
    ~BuildDocumentMetrics() {}

public:
    void RegisterMetrics();
    void ReportBuildDocumentMetrics(document::IDocumentBatch* doc);

    uint32_t GetSuccessDocCount() const { return _successDocCount; }
    uint32_t GetTotalDocCount() const { return _totalDocCount; }

private:
    void ReportAddSuccessMetrics(uint32_t msgCount, bool shouldReport);
    void ReportAddFailedMetrics(uint32_t msgCount, bool shouldReport);

private:
    std::string _tabletName;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;

    int64_t _periodicReportIntervalUS;
    uint64_t _lastBuildDocumentMetricsReportTimeUS;
    uint32_t _totalDocCount;
    uint32_t _successDocCount;
    uint32_t _addDocCount;
    uint32_t _addFailedDocCount;

    uint32_t _totalDocAccumulator;
    uint32_t _addDocAccumulator;
    uint32_t _addFailedDocAccumulator;

private:
    // general metrics
    std::shared_ptr<indexlib::util::Metric> _buildQpsMetric;
    std::shared_ptr<indexlib::util::Metric> _totalDocCountMetric;
    // metrics of different doc types
    std::shared_ptr<indexlib::util::Metric> _addQpsMetric;
    std::shared_ptr<indexlib::util::Metric> _addDocCountMetric;
    std::shared_ptr<indexlib::util::Metric> _addFailedQpsMetric;
    std::shared_ptr<indexlib::util::Metric> _addFailedDocCountMetric;

private:
    static const uint32_t DEFAULT_REPORT_INTERVAL_US = 50'000;

private:
    AUTIL_LOG_DECLARE();
    friend class BuildDocumentMetricsTest;
};

} // namespace indexlibv2::framework
