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
#include "indexlib/index/attribute/AttributeMetrics.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributeMetrics);

AttributeMetrics::AttributeMetrics(const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter)
    : _metricsReporter(metricsReporter)
{
}

void AttributeMetrics::RegisterMetrics()
{
#define REGISTER_ATTRIBUTE_METRIC(metricName)                                                                          \
    _##metricName = 0L;                                                                                                \
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, metricName, "attribute/" #metricName, kmonitor::GAUGE)

    REGISTER_ATTRIBUTE_METRIC(totalReclaimedBytes);
    REGISTER_ATTRIBUTE_METRIC(curReaderReclaimableBytes);
    REGISTER_ATTRIBUTE_METRIC(varAttributeWastedBytes);

    REGISTER_ATTRIBUTE_METRIC(equalCompressExpandFileLen);
    REGISTER_ATTRIBUTE_METRIC(equalCompressWastedBytes);
    REGISTER_ATTRIBUTE_METRIC(equalCompressInplaceUpdateCount);
    REGISTER_ATTRIBUTE_METRIC(equalCompressExpandUpdateCount);
#undef REGISTER_ATTRIBUTE_METRIC

    _reclaimedSliceCountMetric = NULL;
    SetreclaimedSliceCountValue(0);
}

void AttributeMetrics::ReportMetrics()
{
    INDEXLIB_FM_REPORT_METRIC(totalReclaimedBytes);
    INDEXLIB_FM_REPORT_METRIC(curReaderReclaimableBytes);
    INDEXLIB_FM_REPORT_METRIC(varAttributeWastedBytes);

    INDEXLIB_FM_REPORT_METRIC(equalCompressExpandFileLen);
    INDEXLIB_FM_REPORT_METRIC(equalCompressWastedBytes);
    INDEXLIB_FM_REPORT_METRIC(equalCompressInplaceUpdateCount);
    INDEXLIB_FM_REPORT_METRIC(equalCompressExpandUpdateCount);
}

void AttributeMetrics::ResetMetricsForNewReader()
{
    SetvarAttributeWastedBytesValue(0);
    SetcurReaderReclaimableBytesValue(0);
    SetequalCompressExpandFileLenValue(0);
    SetequalCompressWastedBytesValue(0);
}
} // namespace indexlibv2::index
