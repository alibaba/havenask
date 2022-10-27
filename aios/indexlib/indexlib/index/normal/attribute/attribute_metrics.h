#ifndef __INDEXLIB_ATTRIBUTE_METRICS_H
#define __INDEXLIB_ATTRIBUTE_METRICS_H

#include <tr1/memory>
#include <autil/AtomicCounter.h>
#include "indexlib/misc/metric_provider.h"
#include <tr1/unordered_map>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/monitor.h"
#include "indexlib/index/normal/index_metrics_base.h"

IE_NAMESPACE_BEGIN(index);

class AttributeMetrics : public IndexMetricsBase
{
public:
    AttributeMetrics(misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    ~AttributeMetrics();

public:
    void RegisterMetrics(TableType tableType);
    void ReportMetrics();
    void ResetMetricsForNewReader();

private:
    misc::MetricProviderPtr mMetricProvider;

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

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_METRICS_H
