#ifndef __INDEXLIB_SIMPLE_MERGE_POLICY_H
#define __INDEXLIB_SIMPLE_MERGE_POLICY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/table/merge_policy.h"
#include "indexlib/util/key_value_map.h"

IE_NAMESPACE_BEGIN(table);

class SimpleMergePolicy : public MergePolicy
{
public:
    SimpleMergePolicy(const util::KeyValueMap& parameters);
    ~SimpleMergePolicy();
public:
    std::vector<TableMergePlanPtr> CreateMergePlansForIncrementalMerge(
        const std::string& mergeStrategyStr,
        const config::MergeStrategyParameter& mergeStrategyParameter,
        const std::vector<SegmentMetaPtr>& segmentMetas,
        const PartitionRange& targetRange) const override;

    std::vector<MergeTaskDescription> CreateMergeTaskDescriptions(
        const TableMergePlanPtr& mergePlan,
        const TableMergePlanResourcePtr& planResource,
        const std::vector<SegmentMetaPtr>& segmentMetas,
        MergeSegmentDescription& segmentDescription) const override;

    bool ReduceMergeTasks(const TableMergePlanPtr& mergePlan,
                          const std::vector<MergeTaskDescription>& taskDescriptions,
                          const std::vector<file_system::DirectoryPtr>& inputDirectorys,
                          const file_system::DirectoryPtr& outputDirectory,
                          bool isFailOver) const override;     

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SimpleMergePolicy);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_SIMPLE_MERGE_POLICY_H
