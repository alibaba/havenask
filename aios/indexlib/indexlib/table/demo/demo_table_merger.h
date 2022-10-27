#ifndef __INDEXLIB_DEMO_TABLE_MERGER_H
#define __INDEXLIB_DEMO_TABLE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/table/table_merger.h"

IE_NAMESPACE_BEGIN(table);

class DemoTableMerger : public TableMerger
{
public:
    DemoTableMerger(const util::KeyValueMap& parameters);
    ~DemoTableMerger();
public:
    bool Init(
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const TableMergePlanResourcePtr& mergePlanResources,
        const TableMergePlanMetaPtr& mergePlanMeta) override;

    size_t EstimateMemoryUse(
        const std::vector<table::SegmentMetaPtr>& inPlanSegMetas,
        const table::MergeTaskDescription& taskDescription) const override;

    bool Merge(
        const std::vector<table::SegmentMetaPtr>& inPlanSegMetas,
        const table::MergeTaskDescription& taskDescription,
        const file_system::DirectoryPtr& outputDirectory) override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoTableMerger);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_DEMO_TABLE_MERGER_H
