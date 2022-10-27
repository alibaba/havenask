#ifndef __INDEXLIB_INDEX_BUILDER_METRICS_H
#define __INDEXLIB_INDEX_BUILDER_METRICS_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/monitor.h"

DECLARE_REFERENCE_CLASS(document, Document);

IE_NAMESPACE_BEGIN(partition);

class IndexBuilderMetrics
{
public:
    IndexBuilderMetrics(misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    ~IndexBuilderMetrics() {}

public:
    void RegisterMetrics(TableType tableType);
    void ReportMetrics(size_t consumedDocCount, const document::DocumentPtr& doc);
    uint64_t GetSuccessDocCount() const { return mSuccessDocCount; }
    void ReportSchemaVersionMismatch();

private:
    void ReportUpdateField(size_t consumedDocCount, const document::DocumentPtr& doc);

private:
    misc::MetricProviderPtr mMetricProvider;

    uint32_t mSuccessDocCount;
    uint32_t mAddToUpdateDocCount;
    uint32_t mSchemaVersionMismatchCount;

    IE_DECLARE_METRIC(addToUpdateQps);
    IE_DECLARE_METRIC(addToUpdateDocCount);
    IE_DECLARE_METRIC(schemaVersionMissMatchQps);
    IE_DECLARE_METRIC(schemaVersionMissMatchDocCount);

private:
    friend class IndexBuilderMetricsTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexBuilderMetrics);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEX_BUILDER_METRICS_H
