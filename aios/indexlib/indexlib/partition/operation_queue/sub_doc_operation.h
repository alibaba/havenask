#ifndef __INDEXLIB_SUB_DOC_OPERATION_H
#define __INDEXLIB_SUB_DOC_OPERATION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/operation_base.h"

IE_NAMESPACE_BEGIN(partition);

class SubDocOperation : public OperationBase
{
public:
    SubDocOperation(
        int64_t timestamp, IndexType mainPkType, IndexType subPkType)
        : OperationBase(timestamp)
        , mMainOperation(NULL)
        , mSubOperations(NULL)
        , mSubOperationCount(0)
        , mDocOperateType(UNKNOWN_OP)
        , mMainPkType(mainPkType)
        , mSubPkType(subPkType)
    {
        assert(mainPkType == it_primarykey64 || mainPkType == it_primarykey128);
        assert(subPkType == it_primarykey64 || subPkType == it_primarykey128);
    }
    
    ~SubDocOperation();
    
public:
    void Init(DocOperateType docOperateType, OperationBase* mainOperation, 
              OperationBase** subOperations, size_t subOperationCount);

    
    bool Load(autil::mem_pool::Pool *pool, char*& cursor) override;
    
    void Process(const partition::PartitionModifierPtr& modifier,
                 const OperationRedoHint& redoHint) override;

    OperationBase* Clone(autil::mem_pool::Pool *pool) override;
    
    
    SerializedOperationType GetSerializedType() const override
    {
        return SUB_DOC_OP;
    }

    DocOperateType GetDocOperateType() const override
    { return mDocOperateType; }

    size_t GetMemoryUse() const override;

    
    segmentid_t GetSegmentId() const override;

    
    size_t GetSerializeSize() const override;

    
    size_t Serialize(char* buffer, size_t bufferLen) const override;

private:
    OperationBase* LoadOperation(
        autil::mem_pool::Pool *pool, char*& cursor,
        DocOperateType docOpType, IndexType pkType);
    
private:
    OperationBase* mMainOperation;
    OperationBase** mSubOperations;
    size_t mSubOperationCount;
    DocOperateType mDocOperateType;
    IndexType mMainPkType;
    IndexType mSubPkType;

private:
    friend class SubDocOperationTest;
    friend class SubDocOperationCreatorTest;
    friend class OperationFactoryTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocOperation);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SUB_DOC_OPERATION_H
