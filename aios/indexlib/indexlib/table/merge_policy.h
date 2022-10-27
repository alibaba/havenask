#ifndef __INDEXLIB_MERGE_POLICY_H
#define __INDEXLIB_MERGE_POLICY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/table/table_common.h"
#include "indexlib/table/table_merge_plan.h"
#include "indexlib/table/table_merge_plan_resource.h"
#include "indexlib/table/merge_task_description.h"
#include "indexlib/table/merge_task_dispatcher.h"
#include "indexlib/table/segment_meta.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/merge_strategy_parameter.h"
#include "indexlib/file_system/directory.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(table);

class MergePolicy
{
public:
    MergePolicy();
    virtual ~MergePolicy();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema,
              const config::IndexPartitionOptions& options);

    virtual bool DoInit();
    
    virtual std::vector<TableMergePlanPtr> CreateMergePlansForFullMerge(
        const std::string& mergeStrategyStr,
        const config::MergeStrategyParameter& mergeStrategyParameter,
        const std::vector<SegmentMetaPtr>& allSegmentMetas,
        const PartitionRange& targetRange) const;
    
    virtual std::vector<TableMergePlanPtr> CreateMergePlansForIncrementalMerge(
        const std::string& mergeStrategyStr,
        const config::MergeStrategyParameter& mergeStrategyParameter,
        const std::vector<SegmentMetaPtr>& allSegmentMetas,
        const PartitionRange& targetRange) const = 0;

    virtual TableMergePlanResourcePtr CreateMergePlanResource() const;
    
    virtual std::vector<MergeTaskDescription> CreateMergeTaskDescriptions(
        const TableMergePlanPtr& mergePlan,
        const TableMergePlanResourcePtr& planResource,
        const std::vector<SegmentMetaPtr>& inPlanSegmentMetas,
        MergeSegmentDescription& segmentDescription) const = 0;

    // virtual bool ReduceMergeTasks(const TableMergePlanPtr& mergePlan,
    //                               const std::vector<MergeTaskDescription>& taskDescriptions,
    //                               const std::vector<std::string>& mergeTaskDumpPaths,
    //                               const std::string& segmentDataPath, bool isFailOver) const = 0;
    virtual bool ReduceMergeTasks(
        const TableMergePlanPtr& mergePlan,
        const std::vector<MergeTaskDescription>& taskDescriptions,
        const std::vector<file_system::DirectoryPtr>& inputDirectorys,
        const file_system::DirectoryPtr& outputDirectory, bool isFailOver) const = 0;


    virtual MergeTaskDispatcherPtr CreateMergeTaskDispatcher() const;
    
protected:    
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    
public:
    static std::string DEFAULT_MERGE_TASK_NAME;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergePolicy);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_MERGE_POLICY_H
