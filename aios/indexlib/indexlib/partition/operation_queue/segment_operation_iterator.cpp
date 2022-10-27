#include "indexlib/partition/operation_queue/segment_operation_iterator.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SegmentOperationIterator);

SegmentOperationIterator::SegmentOperationIterator(
    const config::IndexPartitionSchemaPtr& schema,
    const OperationMeta& operationMeta, 
    segmentid_t segmentId,
    size_t offset, int64_t timestamp)
    : mOpMeta(operationMeta)
    , mBeginPos(offset)
    , mTimestamp(timestamp)
    , mLastCursor(segmentId, -1)
{
    mOpFactory.Init(schema);
}

IE_NAMESPACE_END(partition);

