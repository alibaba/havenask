#ifndef __INDEXLIB_TABLE_MERGE_WORK_ITEM_H
#define __INDEXLIB_TABLE_MERGE_WORK_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/table/merge_task_description.h"
#include "indexlib/table/task_execute_meta.h"
#include "indexlib/merger/merge_work_item.h"
#include "indexlib/config/index_partition_options.h"

DECLARE_REFERENCE_CLASS(table, TableMerger);
DECLARE_REFERENCE_CLASS(table, TableMergePlan);
DECLARE_REFERENCE_CLASS(table, TableMergePlanMeta);
DECLARE_REFERENCE_CLASS(table, SegmentMeta);
DECLARE_REFERENCE_CLASS(merger, TableMergeMeta);
DECLARE_REFERENCE_CLASS(merger, IndexPartitionMergerMetrics);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(util, CounterMap);

IE_NAMESPACE_BEGIN(merger);

class TableMergeWorkItem : public MergeWorkItem
{
public:
    TableMergeWorkItem(
        const table::TableMergerPtr& tableMerger,
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const merger::TableMergeMetaPtr& mergeMeta,
        const table::TaskExecuteMeta& execMeta,        
        const std::vector<table::SegmentMetaPtr>& inPlanSegMetas,
        bool isFullMerge,
        const std::string& taskDumpPath,
        const merger::MergeFileSystemPtr& mergeFileSystem);
    ~TableMergeWorkItem();

public:
    bool Init(merger::IndexPartitionMergerMetrics* metrics,
              const util::CounterMapPtr& counterMap);
    int64_t GetRequiredResource() const override;
    int64_t EstimateMemoryUse() const override;
    void Destroy() override;

private:
    void DoProcess() override;
    
private:
    table::TableMergerPtr mTableMerger;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    merger::TableMergeMetaPtr mMergeMeta;
    table::TaskExecuteMeta mTaskExecMeta;
    std::vector<table::SegmentMetaPtr> mInPlanSegMetas;
    bool mIsFullMerge;
    std::string mTaskDumpPath;
    int64_t mEstimateMemUse;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableMergeWorkItem);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_TABLE_MERGE_WORK_ITEM_H
