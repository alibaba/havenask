#ifndef __INDEXLIB_SORTED_INDEX_PARTITION_MERGER_H
#define __INDEXLIB_SORTED_INDEX_PARTITION_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/index_base/index_meta/partition_meta.h"

DECLARE_REFERENCE_CLASS(plugin, PluginManager);
IE_NAMESPACE_BEGIN(merger);

class SortedIndexPartitionMerger : public IndexPartitionMerger
{
public:
    SortedIndexPartitionMerger(
            const SegmentDirectoryPtr& segDir, 
            const config::IndexPartitionSchemaPtr& schema,
            const config::IndexPartitionOptions& options,
            const common::SortDescriptions &sortDesc,
            DumpStrategyPtr dumpStrategy = DumpStrategyPtr(),
            misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr(),
            const plugin::PluginManagerPtr& pluginManager
            = plugin::PluginManagerPtr(),
            const PartitionRange& targetRange=PartitionRange());
    ~SortedIndexPartitionMerger();

protected:
    bool IsSortMerge() const override { return true; }

    index::ReclaimMapCreatorPtr CreateReclaimMapCreator() override;

private:
    common::SortDescriptions mSortDescs;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SortedIndexPartitionMerger);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_SORTED_INDEX_PARTITION_MERGER_H
