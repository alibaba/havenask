#ifndef __INDEXLIB_INDEX_WRITER_FACTORY_H
#define __INDEXLIB_INDEX_WRITER_FACTORY_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index_base, PartitionSegmentIterator);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(index, IndexWriter);

IE_NAMESPACE_BEGIN(index);

class IndexWriterFactory
{
public:
    IndexWriterFactory();
    ~IndexWriterFactory();
public:
    static size_t EstimateIndexWriterInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const config::IndexPartitionOptions& options,
        const plugin::PluginManagerPtr& pluginManager,
        index_base::SegmentMetricsPtr segmentMetrics,
        const index_base::PartitionSegmentIteratorPtr& segIter);
    
    static index::IndexWriter* CreateIndexWriter(
            const config::IndexConfigPtr& indexConfig,
            index_base::SegmentMetricsPtr segmentMetrics,
            const config::IndexPartitionOptions& options,
            util::BuildResourceMetrics* buildResourceMetrics,
            const plugin::PluginManagerPtr& pluginManager,
            const index_base::PartitionSegmentIteratorPtr& segIter =
            index_base::PartitionSegmentIteratorPtr());

private:
    static index::IndexWriter* ConstructIndexWriter(
        const config::IndexConfigPtr& indexConfig,
        index_base::SegmentMetricsPtr segmentMetrics,
        const config::IndexPartitionOptions& options,
        util::BuildResourceMetrics* buildResourceMetrics,
        const plugin::PluginManagerPtr& pluginManager);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexWriterFactory);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_WRITER_FACTORY_H
