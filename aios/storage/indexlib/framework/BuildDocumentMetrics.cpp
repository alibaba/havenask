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

#include <stddef.h>

#include "autil/EnvUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/ElementaryDocumentBatch.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/IDocumentBatch.h"
#include "kmonitor/client/MetricType.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, BuildDocumentMetrics);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

static void ReportMetric(uint32_t count, bool shouldReport, BuildDocumentMetrics::Metric& countMetric,
                         BuildDocumentMetrics::Metric& qpsMetric)
{
    if (count == 0) {
        return;
    }
    countMetric.Increase(count);
    qpsMetric.Increase(count);
    if (shouldReport) {
        countMetric.Report();
        qpsMetric.ReportAndClear();
    }
}

BuildDocumentMetrics::BuildDocumentMetrics(const std::string& tabletName,
                                           const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter)
    : _tabletName(tabletName)
    , _metricsReporter(metricsReporter)
{
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

    auto DeclareMetric = [this](const std::string& buildstatus, const std::string& doctype,
                                const std::string metricName, kmonitor::MetricType metricType, Metric& metric) {
        kmonitor::MetricsTags mergeTags {{{"buildstatus", buildstatus}, {"doctype", doctype}}};
        _metricsReporter->getTags().MergeTags(&mergeTags);
        metric.Init(mergeTags,
                    _metricsReporter->declareMutableMetrics(metricName, metricType, kmonitor::MetricLevel::NORMAL));
    };

    _totalBuildQpsMetric.Init(
        _metricsReporter->getTags(),
        _metricsReporter->declareMutableMetrics("basic.buildQps", kmonitor::QPS, kmonitor::MetricLevel::NORMAL));
    _totalDocCountMetric.Init(
        _metricsReporter->getTags(),
        _metricsReporter->declareMutableMetrics("basic.totalDocCount", kmonitor::GAUGE, kmonitor::MetricLevel::NORMAL));

    DeclareMetric("success", "ADD", "detail.buildQps", kmonitor::QPS, _metrics[ADD][SUCCESS][QPS]);
    DeclareMetric("success", "ADD", "detail.buildDocCount", kmonitor::GAUGE, _metrics[ADD][SUCCESS][COUNT]);
    DeclareMetric("failed", "ADD", "detail.buildqps", kmonitor::QPS, _metrics[ADD][FAILED][QPS]);
    DeclareMetric("failed", "ADD", "detail.buildDocCount", kmonitor::GAUGE, _metrics[ADD][FAILED][COUNT]);

    DeclareMetric("success", "DELETE", "detail.buildQps", kmonitor::QPS, _metrics[DELETE][SUCCESS][QPS]);
    DeclareMetric("success", "DELETE", "detail.buildDocCount", kmonitor::GAUGE, _metrics[DELETE][SUCCESS][COUNT]);
    DeclareMetric("failed", "DELETE", "detail.buildqps", kmonitor::QPS, _metrics[DELETE][FAILED][QPS]);
    DeclareMetric("failed", "DELETE", "detail.buildDocCount", kmonitor::GAUGE, _metrics[DELETE][FAILED][COUNT]);

    DeclareMetric("success", "UPDATE", "detail.buildQps", kmonitor::QPS, _metrics[UPDATE][SUCCESS][QPS]);
    DeclareMetric("success", "UPDATE", "detail.buildDocCount", kmonitor::GAUGE, _metrics[UPDATE][SUCCESS][COUNT]);
    DeclareMetric("failed", "UPDATE", "detail.buildqps", kmonitor::QPS, _metrics[UPDATE][FAILED][QPS]);
    DeclareMetric("failed", "UPDATE", "detail.buildDocCount", kmonitor::GAUGE, _metrics[UPDATE][FAILED][COUNT]);

    DeclareMetric("success", "OTHER", "detail.buildQps", kmonitor::QPS, _metrics[OTHER][SUCCESS][QPS]);
    DeclareMetric("success", "OTHER", "detail.buildDocCount", kmonitor::GAUGE, _metrics[OTHER][SUCCESS][COUNT]);
    DeclareMetric("failed", "OTHER", "detail.buildqps", kmonitor::QPS, _metrics[OTHER][FAILED][QPS]);
    DeclareMetric("failed", "OTHER", "detail.buildDocCount", kmonitor::GAUGE, _metrics[OTHER][FAILED][COUNT]);
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
    _successDocCount += successMsgCount;

    if (_metricsReporter == nullptr) {
        return;
    }
    ReportMetric(totalMsgCount, shouldReport, _totalDocCountMetric, _totalBuildQpsMetric);
    auto elementaryBatch = dynamic_cast<document::ElementaryDocumentBatch*>(batch);
    if (elementaryBatch != nullptr) {
        auto addSuccessCount = batch->GetValidDocCount();
        auto addFailedCount = batch->GetBatchSize() - batch->GetValidDocCount();
        ReportMetric(addSuccessCount, shouldReport, _metrics[ADD][SUCCESS][COUNT], _metrics[ADD][SUCCESS][QPS]);
        ReportMetric(addFailedCount, shouldReport, _metrics[ADD][FAILED][COUNT], _metrics[ADD][FAILED][QPS]);
        return;
    }
    uint64_t counter[DOC_TYPE_SIZE][BUILD_STATUS_SIZE] {{0}};
    for (size_t i = 0; i < batch->GetBatchSize(); ++i) {
        if ((*batch)[i] == nullptr) {
            // This might happen when doc in batch is released during build(e.g. in multi-thread parallel build
            // case), in that case the stats below won't work.
            // TODO: Maybe fix the stats below.
            continue;
        }
        DocOperateType operation = (*batch)[i]->GetDocOperateType();
        BuildStatus status = batch->IsDropped(i) ? FAILED : SUCCESS;
        switch (operation) {
        case ADD_DOC: {
            ++counter[ADD][status];
            break;
        }
        case DELETE_DOC: {
            ++counter[DELETE][status];
            break;
        }
        case UPDATE_FIELD: {
            ++counter[UPDATE][status];
            break;
        }
        default: {
            ++counter[OTHER][status];
            break;
        }
        }
    }

    ReportMetric(counter[ADD][SUCCESS], shouldReport, _metrics[ADD][SUCCESS][COUNT], _metrics[ADD][SUCCESS][QPS]);
    ReportMetric(counter[ADD][FAILED], shouldReport, _metrics[ADD][FAILED][COUNT], _metrics[ADD][FAILED][QPS]);

    ReportMetric(counter[DELETE][SUCCESS], shouldReport, _metrics[DELETE][SUCCESS][COUNT],
                 _metrics[DELETE][SUCCESS][QPS]);
    ReportMetric(counter[DELETE][FAILED], shouldReport, _metrics[DELETE][FAILED][COUNT], _metrics[DELETE][FAILED][QPS]);

    ReportMetric(counter[UPDATE][SUCCESS], shouldReport, _metrics[UPDATE][SUCCESS][COUNT],
                 _metrics[UPDATE][SUCCESS][QPS]);
    ReportMetric(counter[UPDATE][FAILED], shouldReport, _metrics[UPDATE][FAILED][COUNT], _metrics[UPDATE][FAILED][QPS]);

    ReportMetric(counter[OTHER][SUCCESS], shouldReport, _metrics[OTHER][SUCCESS][COUNT], _metrics[OTHER][SUCCESS][QPS]);
    ReportMetric(counter[OTHER][FAILED], shouldReport, _metrics[OTHER][FAILED][COUNT], _metrics[OTHER][FAILED][QPS]);
}

} // namespace indexlibv2::framework
