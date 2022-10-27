#include "indexlib/partition/segment/segment_dump_item.h"
#include "indexlib/misc/metric.h"

IE_NAMESPACE_USE(misc);
using namespace std;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SegmentDumpItem);


SegmentDumpItem::SegmentDumpItem(
    const config::IndexPartitionOptions& options,
    const config::IndexPartitionSchemaPtr& schema,
    const string& partitionName)
    : mOptions(options)
    , mSchema(schema)
    , mPartitionName(partitionName)
{
}

SegmentDumpItem::~SegmentDumpItem() 
{
}

IE_NAMESPACE_END(partition);

