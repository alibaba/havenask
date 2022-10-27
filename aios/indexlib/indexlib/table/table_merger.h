#ifndef __INDEXLIB_TABLE_MERGER_H
#define __INDEXLIB_TABLE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/table/table_merge_plan_resource.h"
#include "indexlib/table/table_merge_plan_meta.h"
#include "indexlib/table/merge_task_description.h"
#include "indexlib/table/segment_meta.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(table);

class TableMerger
{
public:
    TableMerger();
    virtual ~TableMerger();
public:
    virtual bool Init(
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const TableMergePlanResourcePtr& mergePlanResources,
        const TableMergePlanMetaPtr& mergePlanMeta) = 0;

    virtual size_t EstimateMemoryUse(
        const std::vector<table::SegmentMetaPtr>& inPlanSegMetas,
        const table::MergeTaskDescription& taskDescription) const = 0;

    virtual bool Merge(
        const std::vector<table::SegmentMetaPtr>& inPlanSegMetas,
        const table::MergeTaskDescription& taskDescription,
        const file_system::DirectoryPtr& outputDirectory) = 0;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableMerger);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLE_MERGER_H
