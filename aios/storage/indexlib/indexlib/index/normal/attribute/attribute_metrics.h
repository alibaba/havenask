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
#include <unordered_map>

#include "autil/AtomicCounter.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/util/index_metrics_base.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"

namespace indexlib { namespace index {

class AttributeMetrics : public IndexMetricsBase
{
public:
    AttributeMetrics(util::MetricProviderPtr metricProvider = util::MetricProviderPtr());
    ~AttributeMetrics();

public:
    void RegisterMetrics(TableType tableType);
    void ReportMetrics();
    void ResetMetricsForNewReader();

private:
    util::MetricProviderPtr mMetricProvider;

    IE_DECLARE_PARAM_METRIC(int64_t, VarAttributeWastedBytes);
    IE_DECLARE_PARAM_METRIC(int64_t, TotalReclaimedBytes);
    IE_DECLARE_PARAM_METRIC(int64_t, CurReaderReclaimableBytes);
    IE_DECLARE_PARAM_METRIC(int64_t, ReclaimedSliceCount);

    IE_DECLARE_PARAM_METRIC(int64_t, EqualCompressExpandFileLen);
    IE_DECLARE_PARAM_METRIC(int64_t, EqualCompressWastedBytes);
    IE_DECLARE_PARAM_METRIC(uint32_t, EqualCompressInplaceUpdateCount);
    IE_DECLARE_PARAM_METRIC(uint32_t, EqualCompressExpandUpdateCount);
    IE_DECLARE_PARAM_METRIC(int64_t, PackAttributeReaderBufferSize);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeMetrics);
}} // namespace indexlib::index
