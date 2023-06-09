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
#include "indexlib/partition/build_document_metrics.h"

#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, BuildDocumentMetrics);

BuildDocumentMetrics::BuildDocumentMetrics(util::MetricProviderPtr metricProvider, TableType tableType)
    : _metricProvider(metricProvider)
    , _totalDocCount(0)
    , _successDocCount(0)
    , _addToUpdateDocCount(0)
    , _indexAddToUpdateDocCount(0)
    , _schemaVersionMismatchCount(0)
    , _addDocCount(0)
    , _addFailedDocCount(0)
    , _updateDocCount(0)
    , _updateFailedDocCount(0)
    , _deleteDocCount(0)
    , _deleteFailedDocCount(0)
    , _deleteSubDocCount(0)
    , _deleteSubFailedDocCount(0)
    , _totalDocAccumulator(0)
    , _addToUpdateDocAccumulator(0)
    , _indexAddToUpdateDocAccumulator(0)
    , _schemaVersionMismatchAccumulator(0)
    , _addDocAccumulator(0)
    , _addFailedDocAccumulator(0)
    , _updateDocAccumulator(0)
    , _updateFailedDocAccumulator(0)
    , _deleteDocAccumulator(0)
    , _deleteFailedDocAccumulator(0)
    , _deleteSubDocAccumulator(0)
    , _deleteSubFailedDocAccumulator(0)
{
    _isKVorKKV = (tableType == tt_kv || tableType == tt_kkv);
    _periodicReportIntervalUS = DEFAULT_REPORT_INTERVAL_US;
    _clock = std::make_unique<util::Clock>();
    _lastBuildDocumentMetricsReportTimeUS = _clock->Now();
    _lastSchemaVersionMismatchReportTimeUS = _clock->Now();
}

void BuildDocumentMetrics::TEST_SetClock(std::unique_ptr<util::Clock> clock)
{
    _clock = std::move(clock);
    _lastBuildDocumentMetricsReportTimeUS = _clock->Now();
    _lastSchemaVersionMismatchReportTimeUS = _clock->Now();
}

util::MetricPtr BuildDocumentMetrics::DeclareMetric(const std::string metricName, kmonitor::MetricType type)
{
    assert(_metricProvider != nullptr);
    util::MetricPtr metric = _metricProvider->DeclareMetric(metricName, type);
    if (metric == nullptr) {
        IE_LOG(ERROR, "Declare metric failed, metricName: %s", metricName.c_str());
    }
    return metric;
}

void BuildDocumentMetrics::RegisterMetrics()
{
    static std::string build_document_metrics_interval = "build_document_metrics_interval_us";
    const char* param = getenv(build_document_metrics_interval.c_str());
    int32_t interval = 0;
    if (param != nullptr and autil::StringUtil::fromString(std::string(param), interval)) {
        _periodicReportIntervalUS = interval;
    }
    IE_LOG(INFO, "build_document_metrics_interval %s interval %u isKVorKKV[%d]", param, _periodicReportIntervalUS,
           _isKVorKKV);

    if (_metricProvider == nullptr) {
        return;
    }

    _buildQpsMetric = DeclareMetric("basic/buildQps", kmonitor::QPS);
    _totalDocCountMetric = DeclareMetric("statistics/totalDocCount", kmonitor::STATUS);

    _addQpsMetric = DeclareMetric("basic/addQps", kmonitor::QPS);
    _addDocCountMetric = DeclareMetric("statistics/addDocCount", kmonitor::STATUS);
    _addFailedQpsMetric = DeclareMetric("debug/addFailedQps", kmonitor::QPS);
    _addFailedDocCountMetric = DeclareMetric("statistics/addFailedDocCount", kmonitor::STATUS);

    _totalDocCount = 0;
    _successDocCount = 0;
    _addDocCount = 0;
    _addFailedDocCount = 0;

    if (_isKVorKKV) {
        return;
    }

    _updateQpsMetric = DeclareMetric("basic/updateQps", kmonitor::QPS);
    _updateDocCountMetric = DeclareMetric("statistics/updateDocCount", kmonitor::STATUS);
    _updateFailedQpsMetric = DeclareMetric("debug/updateFailedQps", kmonitor::QPS);
    _updateFailedDocCountMetric = DeclareMetric("statistics/updateFailedDocCount", kmonitor::STATUS);
    _deleteQpsMetric = DeclareMetric("basic/deleteQps", kmonitor::QPS);
    _deleteDocCountMetric = DeclareMetric("statistics/deleteDocCount", kmonitor::STATUS);
    _deleteFailedQpsMetric = DeclareMetric("debug/deleteFailedQps", kmonitor::QPS);
    _deleteFailedDocCountMetric = DeclareMetric("statistics/deleteFailedDocCount", kmonitor::STATUS);

    _addToUpdateQpsMetric = DeclareMetric("indexlib/build/addToUpdateQps", kmonitor::QPS);
    _addToUpdateDocCountMetric = DeclareMetric("indexlib/build/addToUpdateDocCount", kmonitor::STATUS);
    _indexAddToUpdateQpsMetric = DeclareMetric("indexlib/build/indexAddToUpdateQps", kmonitor::QPS);
    _indexAddToUpdateDocCountMetric = DeclareMetric("indexlib/build/indexAddToUpdateDocCount", kmonitor::STATUS);
    _schemaVersionMismatchQpsMetric = DeclareMetric("indexlib/build/schemaVersionMismatchQps", kmonitor::QPS);
    _schemaVersionMismatchCountMetric = DeclareMetric("indexlib/build/schemaVersionMismatchDocCount", kmonitor::STATUS);

    _updateDocCount = 0;
    _updateFailedDocCount = 0;
    _deleteDocCount = 0;
    _deleteFailedDocCount = 0;
    _deleteSubDocCount = 0;
    _deleteSubFailedDocCount = 0;

    _addToUpdateDocCount = 0;
    _indexAddToUpdateDocCount = 0;
    _schemaVersionMismatchCount = 0;

    _totalDocAccumulator = 0;
    _addToUpdateDocAccumulator = 0;
    _indexAddToUpdateDocAccumulator = 0;
    _schemaVersionMismatchAccumulator = 0;

    _addDocAccumulator = 0;
    _addFailedDocAccumulator = 0;
    _updateDocAccumulator = 0;
    _updateFailedDocAccumulator = 0;
    _deleteDocAccumulator = 0;
    _deleteFailedDocAccumulator = 0;
    _deleteSubDocAccumulator = 0;
    _deleteSubFailedDocAccumulator = 0;
}

void BuildDocumentMetrics::ReportSchemaVersionMismatch()
{
    _schemaVersionMismatchCount++;
    _schemaVersionMismatchAccumulator++;
    bool shouldReport = true;
    if (_periodicReportIntervalUS > 0) {
        uint64_t now = _clock->Now();
        if (now - _lastSchemaVersionMismatchReportTimeUS >= _periodicReportIntervalUS) {
            _lastSchemaVersionMismatchReportTimeUS = now;
        } else {
            shouldReport = false;
        }
    }
    if (shouldReport) {
        util::Metric::ReportMetric(_schemaVersionMismatchCountMetric, _schemaVersionMismatchCount);
        util::Metric::IncreaseQps(_schemaVersionMismatchQpsMetric, _schemaVersionMismatchAccumulator);
        _schemaVersionMismatchAccumulator = 0;
    }
}

void BuildDocumentMetrics::ReportMetricsByType(util::MetricPtr successQpsMetric, util::MetricPtr successDocCountMetric,
                                               util::MetricPtr failedQpsMetric, util::MetricPtr failedDocCountMetric,
                                               uint32_t* successDocCount, uint32_t* failedDocCount,
                                               uint32_t* successDocAccumulator, uint32_t* failedDocAccumulator,
                                               uint32_t successMsgCount, uint32_t totalMsgCount, bool shouldReport)
{
    if (successMsgCount > 0) {
        (*successDocCount) = (*successDocCount) + successMsgCount;
        (*successDocAccumulator) = (*successDocAccumulator) + successMsgCount;
        if (shouldReport) {
            util::Metric::ReportMetric(successDocCountMetric, *successDocCount);
            util::Metric::IncreaseQps(successQpsMetric, *successDocAccumulator);
            (*successDocAccumulator) = 0;
        }
    }
    if (successMsgCount != totalMsgCount) {
        uint32_t failedCount = totalMsgCount - successMsgCount;
        (*failedDocCount) = (*failedDocCount) + failedCount;
        (*failedDocAccumulator) = (*failedDocAccumulator) + failedCount;
        if (shouldReport) {
            util::Metric::ReportMetric(failedDocCountMetric, *failedDocCount);
            util::Metric::IncreaseQps(failedQpsMetric, *failedDocAccumulator);
            (*failedDocAccumulator) = 0;
        }
    }
}

void BuildDocumentMetrics::ReportBuildDocumentMetrics(const document::DocumentPtr& doc, uint32_t successMsgCount)
{
    if (doc == nullptr) {
        return;
    }
    bool shouldReport = true;
    if (_periodicReportIntervalUS > 0) {
        uint64_t now = _clock->Now();
        if (now - _lastBuildDocumentMetricsReportTimeUS >= _periodicReportIntervalUS) {
            uint64_t interval = now - _lastBuildDocumentMetricsReportTimeUS;
            IE_LOG(DEBUG, "ReportBuildDocumentMetrics interval: [%lu]", interval);
            _lastBuildDocumentMetricsReportTimeUS = now;
        } else {
            shouldReport = false;
        }
    }
    uint32_t totalMsgCount = doc->GetMessageCount();
    // TODO: Figure out why the assertion does not work for custom table.
    // assert(totalMsgCount >= successMsgCount);
    _totalDocCount += totalMsgCount;
    _totalDocAccumulator += totalMsgCount;
    _successDocCount += successMsgCount;
    if (shouldReport) {
        util::Metric::ReportMetric(_totalDocCountMetric, _totalDocCount);
        util::Metric::IncreaseQps(_buildQpsMetric, _totalDocAccumulator);
        _totalDocAccumulator = 0;
    }

    DocOperateType operation = doc->GetDocOperateType();
    switch (operation) {
    case ADD_DOC:
        ReportMetricsByType(_addQpsMetric, _addDocCountMetric, _addFailedQpsMetric, _addFailedDocCountMetric,
                            &_addDocCount, &_addFailedDocCount, &_addDocAccumulator, &_addFailedDocAccumulator,
                            successMsgCount, totalMsgCount, shouldReport);
        break;
    case DELETE_DOC: {
        _isKVorKKV ? (void)0
                   : ReportMetricsByType(_deleteQpsMetric, _deleteDocCountMetric, _deleteFailedQpsMetric,
                                         _deleteFailedDocCountMetric, &_deleteDocCount, &_deleteFailedDocCount,
                                         &_deleteDocAccumulator, &_deleteFailedDocAccumulator, successMsgCount,
                                         totalMsgCount, shouldReport);
        break;
    }
    case DELETE_SUB_DOC: {
        _isKVorKKV ? (void)0
                   : ReportMetricsByType(_deleteSubQpsMetric, _deleteSubDocCountMetric, _deleteSubFailedQpsMetric,
                                         _deleteSubFailedDocCountMetric, &_deleteSubDocCount, &_deleteSubFailedDocCount,
                                         &_deleteSubDocAccumulator, &_deleteFailedDocAccumulator, successMsgCount,
                                         totalMsgCount, shouldReport);
    }
    case UPDATE_FIELD: {
        if (_isKVorKKV) {
            break;
        }
        ReportMetricsByType(_updateQpsMetric, _updateDocCountMetric, _updateFailedQpsMetric,
                            _updateFailedDocCountMetric, &_updateDocCount, &_updateFailedDocCount,
                            &_updateDocAccumulator, &_updateFailedDocAccumulator, successMsgCount, totalMsgCount,
                            shouldReport);
        ReportAddToUpdateField(doc, successMsgCount, shouldReport);
        break;
    }
    default: {
        break;
    }
    }
}

// AddToUpdate only supports built-in table type as of 2021-11-25.
void BuildDocumentMetrics::ReportAddToUpdateField(const document::DocumentPtr& doc, uint32_t successMsgCount,
                                                  bool shouldReport)
{
    {
        uint32_t totalMsgCount = doc->GetDocumentCount();
        assert(totalMsgCount == 1u);
        assert(successMsgCount <= 1u);
        DocOperateType originalOperation = doc->GetOriginalOperateType();
        if (originalOperation != ADD_DOC or successMsgCount != totalMsgCount) {
            return;
        }
        _addToUpdateDocCount++;
        _addToUpdateDocAccumulator++;
        if (shouldReport) {
            util::Metric::ReportMetric(_addToUpdateDocCountMetric, _addToUpdateDocCount);
            util::Metric::IncreaseQps(_addToUpdateQpsMetric, _addToUpdateDocAccumulator);
            _addToUpdateDocAccumulator = 0;
        }

        const document::NormalDocumentPtr normalDoc = std::dynamic_pointer_cast<document::NormalDocument>(doc);
        if (normalDoc and normalDoc->HasIndexUpdate()) {
            _indexAddToUpdateDocCount++;
            _indexAddToUpdateDocAccumulator++;
            if (shouldReport) {
                util::Metric::ReportMetric(_indexAddToUpdateDocCountMetric, _indexAddToUpdateDocCount);
                util::Metric::IncreaseQps(_indexAddToUpdateQpsMetric, _indexAddToUpdateDocAccumulator);
                _indexAddToUpdateDocAccumulator = 0;
            }
        }
    }
}

}} // namespace indexlib::partition
