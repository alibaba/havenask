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
#include "indexlib/index/normal/attribute/attribute_metrics.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttributeMetrics);

AttributeMetrics::AttributeMetrics(MetricProviderPtr metricProvider) : mMetricProvider(metricProvider) {}

AttributeMetrics::~AttributeMetrics() {}

void AttributeMetrics::RegisterMetrics(TableType tableType)
{
    if (tableType == tt_kv || tableType == tt_kkv || tableType == tt_customized) {
        return;
    }

#define INIT_ATTRIBUTE_METRIC(metric, unit)                                                                            \
    IE_INIT_METRIC_GROUP(mMetricProvider, metric, "attribute/" #metric, kmonitor::STATUS, unit)

    INIT_ATTRIBUTE_METRIC(TotalReclaimedBytes, "byte");
    INIT_ATTRIBUTE_METRIC(CurReaderReclaimableBytes, "byte");
    INIT_ATTRIBUTE_METRIC(VarAttributeWastedBytes, "byte");

    INIT_ATTRIBUTE_METRIC(EqualCompressExpandFileLen, "byte");
    INIT_ATTRIBUTE_METRIC(EqualCompressWastedBytes, "byte");
    INIT_ATTRIBUTE_METRIC(EqualCompressInplaceUpdateCount, "count");
    INIT_ATTRIBUTE_METRIC(EqualCompressExpandUpdateCount, "count");
#undef INIT_ATTRIBUTE_METRIC

    mReclaimedSliceCountMetric = NULL;
    SetReclaimedSliceCountValue(0);
}

void AttributeMetrics::ReportMetrics()
{
    IE_REPORT_METRIC(TotalReclaimedBytes, mTotalReclaimedBytes);
    IE_REPORT_METRIC(CurReaderReclaimableBytes, mCurReaderReclaimableBytes);
    IE_REPORT_METRIC(VarAttributeWastedBytes, mVarAttributeWastedBytes);

    IE_REPORT_METRIC(EqualCompressExpandFileLen, mEqualCompressExpandFileLen);
    IE_REPORT_METRIC(EqualCompressWastedBytes, mEqualCompressWastedBytes);
    IE_REPORT_METRIC(EqualCompressInplaceUpdateCount, mEqualCompressInplaceUpdateCount);
    IE_REPORT_METRIC(EqualCompressExpandUpdateCount, mEqualCompressExpandUpdateCount);
}

void AttributeMetrics::ResetMetricsForNewReader()
{
    SetVarAttributeWastedBytesValue(0);
    SetCurReaderReclaimableBytesValue(0);
    SetEqualCompressExpandFileLenValue(0);
    SetEqualCompressWastedBytesValue(0);
}
}} // namespace indexlib::index
