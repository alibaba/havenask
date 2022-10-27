#ifndef __INDEXLIB_TABLE_MERGE_PLAN_RESOURCE_H
#define __INDEXLIB_TABLE_MERGE_PLAN_RESOURCE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/table/table_merge_plan.h"
#include "indexlib/table/segment_meta.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(table);

class TableMergePlanResource
{
public:
    TableMergePlanResource();
    virtual ~TableMergePlanResource();
public:
    virtual bool Init(const TableMergePlanPtr& mergePlan,
                      const std::vector<SegmentMetaPtr>& segmentMetas) = 0;
    virtual bool Store(const file_system::DirectoryPtr& root) const = 0;
    virtual bool Load(const file_system::DirectoryPtr& root) = 0;
    
    virtual size_t EstimateMemoryUse() const = 0;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableMergePlanResource);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLE_MERGE_PLAN_RESOURCE_H
