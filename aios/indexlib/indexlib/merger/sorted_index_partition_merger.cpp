#include "indexlib/document/locator.h"
#include "indexlib/merger/sorted_index_partition_merger.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index/normal/reclaim_map/sorted_reclaim_map_creator.h"
#include "indexlib/plugin/plugin_manager.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, SortedIndexPartitionMerger);

SortedIndexPartitionMerger::SortedIndexPartitionMerger(
        const SegmentDirectoryPtr& segDir, 
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const SortDescriptions &sortDescs,
        DumpStrategyPtr dumpStrategy,
        misc::MetricProviderPtr metricProvider,
        const plugin::PluginManagerPtr& pluginManager,
        const PartitionRange& targetRange)
    : IndexPartitionMerger(segDir, schema, options,
                           dumpStrategy, metricProvider,
                           pluginManager, targetRange)
    , mSortDescs(sortDescs)
{
}

SortedIndexPartitionMerger::~SortedIndexPartitionMerger() 
{
}

ReclaimMapCreatorPtr SortedIndexPartitionMerger::CreateReclaimMapCreator()
{
    return ReclaimMapCreatorPtr(new SortedReclaimMapCreator(
        mMergeConfig, mAttrReaderContainer, mSchema, mSortDescs, mSegmentDirectory));
}

IE_NAMESPACE_END(merger);

