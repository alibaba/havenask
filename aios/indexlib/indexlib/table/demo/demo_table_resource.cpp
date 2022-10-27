#include "indexlib/table/demo/demo_table_resource.h"

using namespace std;

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, DemoTableResource);

DemoTableResource::DemoTableResource(const util::KeyValueMap& parameters) 
{
}

DemoTableResource::~DemoTableResource() 
{
}

bool DemoTableResource::Init(
    const std::vector<table::SegmentMetaPtr>& segmentMetas,
    const config::IndexPartitionSchemaPtr& schema,
    const config::IndexPartitionOptions& options)
{
    return true;
}

bool DemoTableResource::ReInit(const std::vector<table::SegmentMetaPtr>& segmentMetas)
{
    return true;
}

size_t DemoTableResource::GetCurrentMemoryUse() const
{
    return 0u;
}

size_t DemoTableResource::EstimateInitMemoryUse(const vector<SegmentMetaPtr>& segmentMetas) const
{
    return 0;
}

IE_NAMESPACE_END(table);

