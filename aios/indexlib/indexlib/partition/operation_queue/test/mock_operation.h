#ifndef __INDEXLIB_MOCK_OPERATION_H
#define __INDEXLIB_MOCK_OPERATION_H

#include "indexlib/common_define.h"


#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/operation_factory.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

IE_NAMESPACE_BEGIN(partition);

class MockOperation : public OperationBase
{
public:
    MockOperation(int64_t timestamp)
        : OperationBase(timestamp)
    {}
    
    
    OperationBase* Clone(autil::mem_pool::Pool *pool) override
    {
        MockOperation* cloneOperation = IE_POOL_COMPATIBLE_NEW_CLASS(
                pool, MockOperation, mTimestamp);
        return cloneOperation;
    }

    MOCK_METHOD2(Load, bool(autil::mem_pool::Pool *pool, char*& cursor));
    MOCK_METHOD2(Process, void(const partition::PartitionModifierPtr& modifier,
                               const OperationRedoHint& redoHint));
    MOCK_CONST_METHOD2(Serialize, size_t(char* buffer, size_t bufferLen));
    MOCK_CONST_METHOD0(GetMemoryUse, size_t());
    MOCK_CONST_METHOD0(GetSegmentId, segmentid_t());
    MOCK_CONST_METHOD0(GetSerializeSize, size_t());
    MOCK_CONST_METHOD0(GetDocOperateType, DocOperateType()); 
    
};

class MockOperationFactory : public OperationFactory
{
public:
    MOCK_METHOD2(CreateOperation, OperationBase*(const document::NormalDocumentPtr& doc,
                                                 autil::mem_pool::Pool *pool));
};

class MockOperationMaker
{
public:
    static MockOperation* MakeOperation(int64_t ts, autil::mem_pool::Pool* pool)
    {
        MockOperation *operation = 
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, MockOperation, ts);
        return operation;
    }
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MOCK_OPERATION_H
