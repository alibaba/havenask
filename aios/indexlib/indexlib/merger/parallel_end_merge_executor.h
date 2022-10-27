#ifndef __INDEXLIB_PARALLEL_END_MERGE_EXECUTOR_H
#define __INDEXLIB_PARALLEL_END_MERGE_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/merger/merge_task_item.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(merger, IndexMergeMeta);
DECLARE_REFERENCE_CLASS(index, DeletionMapMerger);
DECLARE_REFERENCE_CLASS(index, SummaryMerger);
DECLARE_REFERENCE_CLASS(index, AttributeMerger);
DECLARE_REFERENCE_CLASS(index, IndexMerger);
DECLARE_REFERENCE_CLASS(index_base, MergeTaskResourceManager);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(merger);

class ParallelEndMergeExecutor
{
public:
    ParallelEndMergeExecutor(
            const config::IndexPartitionSchemaPtr &schema,
            const plugin::PluginManagerPtr& pluginManager,
            const file_system::DirectoryPtr& rootDirectory);

    virtual ~ParallelEndMergeExecutor();
    
public:
    void ParallelEndMerge(const IndexMergeMetaPtr& mergeMeta);

private:
    static std::vector<MergeTaskItems> ExtractParallelMergeTaskGroup(
            const IndexMergeMetaPtr& mergeMeta);
    
    static void CheckOneTaskGroup(const MergeTaskItems& items);

    void SingleGroupParallelEndMerge(
            const IndexMergeMetaPtr& mergeMeta, const MergeTaskItems& taskGroup);

    virtual index::SummaryMergerPtr CreateSummaryMerger(
            const MergeTaskItem &item, 
            const config::IndexPartitionSchemaPtr &schema) const;
    
    virtual index::AttributeMergerPtr CreateAttributeMerger(
            const MergeTaskItem &item, 
            const config::IndexPartitionSchemaPtr &schema) const;
    
    virtual index::IndexMergerPtr CreateIndexMerger(
            const MergeTaskItem &item,
            const config::IndexPartitionSchemaPtr &schema) const;

    std::vector<index_base::MergeTaskResourceVector> ExtractMergeTaskResource(
            const index_base::MergeTaskResourceManagerPtr& mgr,
            const MergeTaskItems &taskGroup) const;

private:
    config::IndexPartitionSchemaPtr mSchema;
    plugin::PluginManagerPtr mPluginManager;
    file_system::DirectoryPtr mRootDirectory;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ParallelEndMergeExecutor);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_END_PARALLEL_MERGE_EXECUTOR_H
