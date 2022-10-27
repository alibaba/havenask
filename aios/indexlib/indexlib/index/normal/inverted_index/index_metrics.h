#ifndef __INDEXLIB_INDEX_METRICS_H
#define __INDEXLIB_INDEX_METRICS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/index_metrics_base.h"

IE_NAMESPACE_BEGIN(index);

class IndexMetrics : public IndexMetricsBase
{
public:
    IndexMetrics(misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    ~IndexMetrics();

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexMetrics);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_METRICS_H
