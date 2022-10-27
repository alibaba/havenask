#ifndef __INDEXLIB_PARTITION_MERGER_CREATOR_H
#define __INDEXLIB_PARTITION_MERGER_CREATOR_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/merger/partition_merger.h"
#include "indexlib/merger/custom_partition_merger.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"

DECLARE_REFERENCE_CLASS(merger, SegmentDirectory);
DECLARE_REFERENCE_CLASS(merger, DumpStrategy);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

IE_NAMESPACE_BEGIN(merger);

class PartitionMergerCreator
{
public:
    PartitionMergerCreator();
    ~PartitionMergerCreator();
public:
    static PartitionMerger* CreateSinglePartitionMerger(
            const std::string& rootDir,
            const config::IndexPartitionOptions& options,
            misc::MetricProviderPtr metricProvider,
            const std::string& indexPuginPath,
            const PartitionRange& targetRange = PartitionRange());

    static PartitionMerger* CreateIncParallelPartitionMerger(
        const std::string& mergeDest,
        const index_base::ParallelBuildInfo& info,
        const config::IndexPartitionOptions& options,
        misc::MetricProviderPtr metricProvider,
        const std::string& indexPuginPath,
        const PartitionRange& targetRange = PartitionRange());

    static PartitionMerger* CreateFullParallelPartitionMerger(
        const std::vector<std::string>& mergeSrc,
        const std::string& mergeDest,
        const config::IndexPartitionOptions& options,
        misc::MetricProviderPtr metricProvider,
        const std::string& indexPuginPath,
        const PartitionRange& targetRange = PartitionRange());
    
private:
    static CustomPartitionMerger* CreateCustomPartitionMerger(
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const SegmentDirectoryPtr& segDir,
        const DumpStrategyPtr& dumpStrategy,
        misc::MetricProviderPtr metricProvider,
        const plugin::PluginManagerPtr& pluginManager,
        const PartitionRange& targetRange);

    
    static void CreateDestPath(const std::string& destPath,
                               const config::IndexPartitionSchemaPtr& schema,
                               const config::IndexPartitionOptions& options,
                               const index_base::PartitionMeta &partMeta);

    static void CheckSrcPath(const std::vector<std::string>& mergeSrc,
                             const config::IndexPartitionSchemaPtr& schema,
                             const config::IndexPartitionOptions& options,
                             const index_base::PartitionMeta &partMeta,
                             std::vector<index_base::Version>& versions);

    static void CheckPartitionConsistence(const std::string& rootDir,
                                          const config::IndexPartitionSchemaPtr& schema,
                                          const config::IndexPartitionOptions& options,
                                          const index_base::PartitionMeta &partMeta);

    static void CheckSchema(const std::string& rootDir,
                            const config::IndexPartitionSchemaPtr& schema,
                            const config::IndexPartitionOptions& options);

    static void CheckIndexFormat(const std::string& rootDir);

    static void CheckPartitionMeta(const std::string& rootDir,
                                   const index_base::PartitionMeta &partMeta);
    static void SortSubPartitions(std::vector<std::string>& mergeSrcVec,
                                  std::vector<index_base::Version>& versions);

    static schemavid_t GetSchemaId(const std::string& rootDir,
                                   const index_base::ParallelBuildInfo& parallelInfo);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionMergerCreator);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_PARTITION_MERGER_CREATOR_H
