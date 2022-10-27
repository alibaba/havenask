#ifndef __INDEXLIB_MERGE_TASK_EXECUTOR_H
#define __INDEXLIB_MERGE_TASK_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/table/task_execute_meta.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(merger, IndexPartitionMergerMetrics);
DECLARE_REFERENCE_CLASS(merger, MergeWorkItem);
DECLARE_REFERENCE_CLASS(merger, MergeFileSystem);
DECLARE_REFERENCE_CLASS(merger, TableMergeMeta);
DECLARE_REFERENCE_CLASS(table, TableFactoryWrapper);
DECLARE_REFERENCE_CLASS(table, SegmentMeta);
DECLARE_REFERENCE_CLASS(util, CounterMap);

IE_NAMESPACE_BEGIN(merger);

class MergeTaskExecutor
{
public:
    MergeTaskExecutor(
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const util::CounterMapPtr& counterMap,
        merger::IndexPartitionMergerMetrics* metrics,
        bool isFullMerge,
        const merger::MergeFileSystemPtr& mergeFileSystem);
    ~MergeTaskExecutor();
public:
    bool Init(
        const merger::TableMergeMetaPtr& tableMergeMeta,
        const table::TableFactoryWrapperPtr& tableFactory,
        const std::vector<table::SegmentMetaPtr>& segmentMetas);

    merger::MergeWorkItem* CreateMergeWorkItem(
        const table::TaskExecuteMeta& taskExecMeta,
        bool& errorFlag) const;
    
private:
    static std::string GetCheckpointName(
        const table::TaskExecuteMeta& taskExecMeta);
    static std::string GetTaskDumpDir(
        const table::TaskExecuteMeta& taskExecMeta);
    
private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    util::CounterMapPtr mCounterMap;
    merger::IndexPartitionMergerMetrics* mMetrics;
    bool mIsFullMerge;

    merger::TableMergeMetaPtr mMergeMeta;
    table::TableFactoryWrapperPtr mTableFactory;
    std::map<segmentid_t, table::SegmentMetaPtr> mSegMetaMap;
    merger::MergeFileSystemPtr mMergeFileSystem;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeTaskExecutor);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_TASK_EXECUTOR_H
