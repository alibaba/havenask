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

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/clock.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"

DECLARE_REFERENCE_CLASS(document, Document);

namespace indexlib { namespace partition {

// BuildDocumentMetrics将按doc的统计指标汇总在indexlib层(2021-12)。此前，各种指标分散在build_service和indexlib中。
// BuildDocumentMetrics有定期按interval汇报指标的选项，原因是调试过程中发现每个doc都做指标汇报对性能有较大影响(比如在100k
// doc/s的速率下)。
class BuildDocumentMetrics
{
public:
    BuildDocumentMetrics(util::MetricProviderPtr metricProvider, TableType tableType);
    ~BuildDocumentMetrics() {}

public:
    void RegisterMetrics();
    void ReportBuildDocumentMetrics(const document::DocumentPtr& doc, uint32_t successMsgCount);
    void ReportSchemaVersionMismatch();

    uint64_t GetSuccessDocCount() const { return _successDocCount; }
    uint64_t GetAddToUpdateDocCount() const { return _addToUpdateDocCount; }
    uint32_t getTotalDocCount() const { return _totalDocCount; }

private:
    void ReportAddToUpdateField(const document::DocumentPtr& doc, uint32_t successMsgCount, bool shouldReport);
    util::MetricPtr DeclareMetric(const std::string metricName, kmonitor::MetricType type);

private:
    static void ReportMetricsByType(util::MetricPtr successQpsMetric, util::MetricPtr successDocCountMetric,
                                    util::MetricPtr failedQpsMetric, util::MetricPtr failedDocCountMetric,
                                    uint32_t* successDocCount, uint32_t* failedDocCount,
                                    uint32_t* successDocAccumulator, uint32_t* failedDocAccumulator,
                                    uint32_t successMsgCount, uint32_t totalMsgCount, bool shouldReport);

private:
    void TEST_SetClock(std::unique_ptr<util::Clock> clock);

private:
    util::MetricProviderPtr _metricProvider;

    bool _isKVorKKV;
    int32_t _periodicReportIntervalUS;

    uint32_t _totalDocCount;
    uint32_t _successDocCount;
    uint32_t _addToUpdateDocCount;
    uint32_t _indexAddToUpdateDocCount;
    uint32_t _schemaVersionMismatchCount;

    uint32_t _addDocCount;
    uint32_t _addFailedDocCount;
    uint32_t _updateDocCount;
    uint32_t _updateFailedDocCount;
    uint32_t _deleteDocCount;
    uint32_t _deleteFailedDocCount;
    uint32_t _deleteSubDocCount;
    uint32_t _deleteSubFailedDocCount;

    uint32_t _totalDocAccumulator;
    uint32_t _addToUpdateDocAccumulator;
    uint32_t _indexAddToUpdateDocAccumulator;
    uint32_t _schemaVersionMismatchAccumulator;

    uint32_t _addDocAccumulator;
    uint32_t _addFailedDocAccumulator;
    uint32_t _updateDocAccumulator;
    uint32_t _updateFailedDocAccumulator;
    uint32_t _deleteDocAccumulator;
    uint32_t _deleteFailedDocAccumulator;
    uint32_t _deleteSubDocAccumulator;
    uint32_t _deleteSubFailedDocAccumulator;

private:
    std::unique_ptr<util::Clock> _clock;
    uint64_t _lastBuildDocumentMetricsReportTimeUS;
    uint64_t _lastSchemaVersionMismatchReportTimeUS;

private:
    // general metrics
    util::MetricPtr _buildQpsMetric;
    util::MetricPtr _totalDocCountMetric;
    // metrics of different doc types
    util::MetricPtr _addQpsMetric;
    util::MetricPtr _addDocCountMetric;
    util::MetricPtr _addFailedQpsMetric;
    util::MetricPtr _addFailedDocCountMetric;
    util::MetricPtr _updateQpsMetric;
    util::MetricPtr _updateDocCountMetric;
    util::MetricPtr _updateFailedQpsMetric;
    util::MetricPtr _updateFailedDocCountMetric;
    util::MetricPtr _deleteQpsMetric;
    util::MetricPtr _deleteDocCountMetric;
    util::MetricPtr _deleteFailedQpsMetric;
    util::MetricPtr _deleteFailedDocCountMetric;
    util::MetricPtr _deleteSubQpsMetric;
    util::MetricPtr _deleteSubDocCountMetric;
    util::MetricPtr _deleteSubFailedQpsMetric;
    util::MetricPtr _deleteSubFailedDocCountMetric;
    // detailed metrics
    util::MetricPtr _addToUpdateQpsMetric;
    util::MetricPtr _addToUpdateDocCountMetric;
    util::MetricPtr _indexAddToUpdateQpsMetric;
    util::MetricPtr _indexAddToUpdateDocCountMetric;
    util::MetricPtr _schemaVersionMismatchQpsMetric;
    util::MetricPtr _schemaVersionMismatchCountMetric;

private:
    static const uint32_t DEFAULT_REPORT_INTERVAL_US = 50'000;
    friend class BuildDocumentMetricsTest;

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::partition
