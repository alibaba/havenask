#ifndef __INDEXLIB_DEMO_TABLE_RESOURCE_H
#define __INDEXLIB_DEMO_TABLE_RESOURCE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/table/table_resource.h"

DECLARE_REFERENCE_CLASS(partition, PartitionSegmentIterator);

IE_NAMESPACE_BEGIN(table);

class DemoTableResource : public TableResource
{
public:
    DemoTableResource(const util::KeyValueMap& parameters);
    ~DemoTableResource();
public:
    bool Init(const std::vector<SegmentMetaPtr>& segmentMetas,
              const config::IndexPartitionSchemaPtr& schema,
              const config::IndexPartitionOptions& options) override;
    bool ReInit(const std::vector<SegmentMetaPtr>& segmentMetas) override;
    size_t EstimateInitMemoryUse(const std::vector<SegmentMetaPtr>& segmentMetas) const override; 
    size_t GetCurrentMemoryUse() const override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoTableResource);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_DEMO_TABLE_RESOURCE_H
