#ifndef __INDEXLIB_NORMAL_SEGMENT_OPERATION_ITERATOR_H
#define __INDEXLIB_NORMAL_SEGMENT_OPERATION_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/in_mem_segment_operation_iterator.h"
#include "indexlib/partition/operation_queue/operation_factory.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(file_system, FileReader);

IE_NAMESPACE_BEGIN(partition);

class NormalSegmentOperationIterator : public InMemSegmentOperationIterator
{
public:
    NormalSegmentOperationIterator(
            const config::IndexPartitionSchemaPtr& schema,
            const OperationMeta& operationMeta, 
            segmentid_t segmentId,
            size_t offset,
            int64_t timestamp)
        : InMemSegmentOperationIterator(
                schema, operationMeta, segmentId, offset, timestamp)
    {}

    ~NormalSegmentOperationIterator() {}
    
public:
    void Init(const file_system::DirectoryPtr& operationDirectory);

private:
    file_system::FileReaderPtr mFileReader;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalSegmentOperationIterator);

IE_NAMESPACE_END(partition);
#endif //__INDEXLIB_NORMAL_SEGMENT_OPERATION_ITERATOR_H
