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
#include "build_service/builder/BuilderMetrics.h"

#include "build_service/util/Monitor.h"

using namespace std;
namespace build_service { namespace builder {

BS_LOG_SETUP(builder, BuilderMetrics);

BuilderMetrics::~BuilderMetrics() {}

bool BuilderMetrics::declareMetrics(indexlib::util::MetricProviderPtr metricProvider, bool isKVorKKV)
{
    const char* param = getenv(BS_ENV_ENABLE_LEGACY_BUILDER_METRICS.c_str());
    std::string res;
    _metricsEnabled = false;
    if (param != nullptr and autil::StringUtil::fromString(string(param), res) and res == "true") {
        _metricsEnabled = true;
    }
    res = _metricsEnabled ? "true" : "false";
    BS_LOG(INFO, "bs_enable_legacy_builder_metrics enabled: [%s]", res.c_str());
    if (!_metricsEnabled) {
        return true;
    }

    _buildQpsMetric = DECLARE_METRIC(metricProvider, "basic/buildQps", kmonitor::QPS, "count");
    _totalDocCountMetric = DECLARE_METRIC(metricProvider, "statistics/totalDocCount", kmonitor::STATUS, "count");

#define DEFINE_METRIC(metric)                                                                                          \
    do {                                                                                                               \
        _##metric##QpsMetric = DECLARE_METRIC(metricProvider, "basic/" #metric "Qps", kmonitor::QPS, "count");         \
        _##metric##DocCountMetric =                                                                                    \
            DECLARE_METRIC(metricProvider, "statistics/" #metric "DocCount", kmonitor::STATUS, "count");               \
        _##metric##FailedQpsMetric =                                                                                   \
            DECLARE_METRIC(metricProvider, "debug/" #metric "FailedQps", kmonitor::QPS, "count");                      \
        _##metric##FailedDocCountMetric =                                                                              \
            DECLARE_METRIC(metricProvider, "statistics/" #metric "FailedDocCount", kmonitor::STATUS, "count");         \
    } while (0)

    DEFINE_METRIC(add);
    if (!isKVorKKV) {
        DEFINE_METRIC(update);
        DEFINE_METRIC(delete);
        DEFINE_METRIC(deleteSub);
    }
    return true;
}

void BuilderMetrics::reportMetrics(bool ret, DocOperateType op)
{
    _totalDocCount++;
    if (ret) {
        TEST_successDocCount++;
    }
    if (_metricsEnabled) {
        INCREASE_QPS(_buildQpsMetric);
        REPORT_METRIC(_totalDocCountMetric, _totalDocCount);
    }

#define REPORT_DOC_METRIC(op, metric)                                                                                  \
    case op: {                                                                                                         \
        if (ret) {                                                                                                     \
            _##metric##DocCount++;                                                                                     \
            if (_metricsEnabled) {                                                                                     \
                REPORT_METRIC(_##metric##DocCountMetric, _##metric##DocCount);                                         \
                INCREASE_QPS(_##metric##QpsMetric);                                                                    \
            }                                                                                                          \
        } else {                                                                                                       \
            _##metric##FailedDocCount++;                                                                               \
            if (_metricsEnabled) {                                                                                     \
                REPORT_METRIC(_##metric##FailedDocCountMetric, _##metric##FailedDocCount);                             \
                INCREASE_QPS(_##metric##FailedQpsMetric);                                                              \
            }                                                                                                          \
        }                                                                                                              \
        break;                                                                                                         \
    }

    switch (op) {
        REPORT_DOC_METRIC(ADD_DOC, add);
        REPORT_DOC_METRIC(UPDATE_FIELD, update);
        REPORT_DOC_METRIC(DELETE_DOC, delete);
        REPORT_DOC_METRIC(DELETE_SUB_DOC, deleteSub);
    default: {
        string errorMsg = "unknown doc operator";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
    }
    }
#undef REPORT_DOC_METRIC
} // namespace builder

void BuilderMetrics::reportMetrics(size_t totalMsgCount, size_t consumedMsgCount, DocOperateType op)
{
    _totalDocCount += totalMsgCount;
    TEST_successDocCount += consumedMsgCount;

    if (_metricsEnabled) {
        INCREASE_VALUE(_buildQpsMetric, totalMsgCount);
        REPORT_METRIC(_totalDocCountMetric, _totalDocCount);
    }

#define REPORT_DOC_METRIC(op, metric)                                                                                  \
    case op: {                                                                                                         \
        if (consumedMsgCount > 0) {                                                                                    \
            _##metric##DocCount += consumedMsgCount;                                                                   \
            if (_metricsEnabled) {                                                                                     \
                REPORT_METRIC(_##metric##DocCountMetric, _##metric##DocCount);                                         \
                INCREASE_VALUE(_##metric##QpsMetric, consumedMsgCount);                                                \
            }                                                                                                          \
        }                                                                                                              \
        break;                                                                                                         \
    }

    switch (op) {
        REPORT_DOC_METRIC(ADD_DOC, add);
        REPORT_DOC_METRIC(UPDATE_FIELD, update);
        REPORT_DOC_METRIC(DELETE_DOC, delete);
        REPORT_DOC_METRIC(DELETE_SUB_DOC, deleteSub);
    default: {
        string errorMsg = "unknown doc operator";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
    }
    }
}
}} // namespace build_service::builder
