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
#include "indexlib/framework/BuildDocumentMetrics.h"

#include "autil/EnvUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/IDocumentBatch.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, BuildDocumentMetrics);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

BuildDocumentMetrics::BuildDocumentMetrics(const std::string& tabletName,
                                           const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter)
    : _tabletName(tabletName)
    , _metricsReporter(metricsReporter)
    , _totalDocCount(0)
    , _successDocCount(0)
    , _addDocCount(0)
    , _addFailedDocCount(0)
    , _totalDocAccumulator(0)
    , _addDocAccumulator(0)
    , _addFailedDocAccumulator(0)
{
    _periodicReportIntervalUS = DEFAULT_REPORT_INTERVAL_US;
    _lastBuildDocumentMetricsReportTimeUS = autil::TimeUtility::currentTimeInMicroSeconds();
}

static std::shared_ptr<indexlib::util::Metric>
DeclareMetric(const std::shared_ptr<kmonitor::MetricsReporter>& metricProvider, const std::string metricName,
              kmonitor::MetricType type)
{
    if (!metricProvider) {
        return nullptr;
    }
    return std::make_shared<indexlib::util::Metric>(metricProvider, metricName, type);
}

void BuildDocumentMetrics::RegisterMetrics()
{
    static std::string buildDocumentMetricsInterval = "build_document_metrics_interval_us";
    int64_t interval = 0;
    if (autil::EnvUtil::getEnvWithoutDefault(buildDocumentMetricsInterval, interval)) {
        _periodicReportIntervalUS = interval;
    }
    TABLET_LOG(INFO, "build_document_metrics_interval %ld", _periodicReportIntervalUS);

    if (_metricsReporter == nullptr) {
        return;
    }

    _buildQpsMetric = DeclareMetric(_metricsReporter, "basic/buildQps", kmonitor::QPS);
    _totalDocCountMetric = DeclareMetric(_metricsReporter, "statistics/totalDocCount", kmonitor::GAUGE);
    _addQpsMetric = DeclareMetric(_metricsReporter, "basic/addQps", kmonitor::QPS);
    _addDocCountMetric = DeclareMetric(_metricsReporter, "statistics/addDocCount", kmonitor::GAUGE);
    _addFailedQpsMetric = DeclareMetric(_metricsReporter, "debug/addFailedQps", kmonitor::QPS);
    _addFailedDocCountMetric = DeclareMetric(_metricsReporter, "statistics/addFailedDocCount", kmonitor::GAUGE);

    _totalDocCount = 0;
    _successDocCount = 0;
    _addDocCount = 0;
    _addFailedDocCount = 0;
}

void BuildDocumentMetrics::ReportAddSuccessMetrics(uint32_t msgCount, bool shouldReport)
{
    if (msgCount == 0) {
        return;
    }
    _addDocCount += msgCount;
    _addDocAccumulator += msgCount;
    if (shouldReport) {
        indexlib::util::Metric::ReportMetric(_addDocCountMetric, _addDocCount);
        indexlib::util::Metric::IncreaseQps(_addQpsMetric, _addDocAccumulator);
        _addDocAccumulator = 0;
    }
}

void BuildDocumentMetrics::ReportAddFailedMetrics(uint32_t msgCount, bool shouldReport)
{
    if (msgCount == 0) {
        return;
    }
    _addFailedDocCount += msgCount;
    _addFailedDocAccumulator += msgCount;
    if (shouldReport) {
        indexlib::util::Metric::ReportMetric(_addFailedDocCountMetric, _addFailedDocCount);
        indexlib::util::Metric::IncreaseQps(_addFailedQpsMetric, _addFailedDocAccumulator);
        _addFailedDocAccumulator = 0;
    }
}

void BuildDocumentMetrics::ReportBuildDocumentMetrics(document::IDocumentBatch* batch)
{
    if (batch == nullptr) {
        return;
    }
    bool shouldReport = true;
    if (_periodicReportIntervalUS > 0) {
        uint64_t now = autil::TimeUtility::currentTimeInMicroSeconds();
        if (now - _lastBuildDocumentMetricsReportTimeUS >= _periodicReportIntervalUS) {
            uint64_t interval = now - _lastBuildDocumentMetricsReportTimeUS;
            TABLET_LOG(DEBUG, "ReportBuildDocumentMetrics interval: [%lu]", interval);
            _lastBuildDocumentMetricsReportTimeUS = now;
        } else {
            shouldReport = false;
        }
    }
    uint32_t totalMsgCount = batch->GetBatchSize();
    uint32_t successMsgCount = batch->GetValidDocCount();
    // TODO: Figure out why the assertion does not work for custom table.
    // assert(totalMsgCount >= successMsgCount);
    _totalDocCount += totalMsgCount;
    _totalDocAccumulator += totalMsgCount;
    _successDocCount += successMsgCount;
    if (shouldReport) {
        indexlib::util::Metric::ReportMetric(_totalDocCountMetric, _totalDocCount);
        indexlib::util::Metric::IncreaseQps(_buildQpsMetric, _totalDocAccumulator);
        _totalDocAccumulator = 0;
    }
    uint32_t addSuccessCount = 0;
    uint32_t addFailedCount = 0;
    for (size_t i = 0; i < batch->GetBatchSize(); ++i) {
        if ((*batch)[i] == nullptr) {
            // This might happen when doc in batch is released during build(e.g. in multi-thread parallel build case),
            // in that case the stats below won't work.
            // TODO: Maybe fix the stats below.
            continue;
        }
        DocOperateType operation = (*batch)[i]->GetDocOperateType();
        bool isDropped = batch->IsDropped(i);
        switch (operation) {
        case ADD_DOC: {
            if (isDropped) {
                addFailedCount++;
            } else {
                addSuccessCount++;
            }
            break;
        }
        default: {
            break;
        }
        }
    }
    ReportAddSuccessMetrics(addSuccessCount, shouldReport);
    ReportAddFailedMetrics(addFailedCount, shouldReport);
}

} // namespace indexlibv2::framework
