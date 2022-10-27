#ifndef __INDEXLIB_TABLE_RESOURCE_H
#define __INDEXLIB_TABLE_RESOURCE_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/table/segment_meta.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/misc/metric_provider.h"

IE_NAMESPACE_BEGIN(table);

class TableResource
{
public:
    TableResource();
    virtual ~TableResource();
public:
    virtual bool Init(
        const std::vector<SegmentMetaPtr>& segmentMetas,
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options) = 0;
    virtual bool ReInit(const std::vector<SegmentMetaPtr>& segmentMetas) = 0;
    
    virtual size_t EstimateInitMemoryUse(const std::vector<SegmentMetaPtr>& segmentMetas) const = 0; 
    virtual size_t GetCurrentMemoryUse() const = 0;

public:
    void SetMetricProvider(const misc::MetricProviderPtr& metricProvider)
    {
        mMetricProvider = metricProvider;
    }

    const misc::MetricProviderPtr& GetMetricProvider() const
    {
        return mMetricProvider;
    }

private:
    misc::MetricProviderPtr mMetricProvider;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableResource);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLE_RESOURCE_H
