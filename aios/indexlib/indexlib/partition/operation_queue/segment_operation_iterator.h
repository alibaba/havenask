#ifndef __INDEXLIB_SEGMENT_OPERATION_ITERATOR_H
#define __INDEXLIB_SEGMENT_OPERATION_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/operation_meta.h"
#include "indexlib/partition/operation_queue/operation_factory.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/operation_cursor.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(partition);

class SegmentOperationIterator
{
public:
    SegmentOperationIterator(const config::IndexPartitionSchemaPtr& schema,
                             const OperationMeta& operationMeta, 
                             segmentid_t segmentId, size_t offset, int64_t timestamp);
    
    virtual ~SegmentOperationIterator() {}
    
public:
    bool HasNext() const
    {
        return mLastCursor.pos < int32_t(mOpMeta.GetOperationCount()) - 1;
    }
    
    virtual OperationBase* Next() { assert(false); return NULL; }
    OperationCursor GetLastCursor() const
    { return mLastCursor; }

protected:
    OperationMeta mOpMeta;
    size_t mBeginPos; // points to the first operation to be read
    int64_t mTimestamp;
    OperationCursor mLastCursor;
    OperationFactory mOpFactory;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentOperationIterator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SEGMENT_OPERATION_ITERATOR_H
