#ifndef __INDEXLIB_OPERATION_FACTORY_H
#define __INDEXLIB_OPERATION_FACTORY_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/operation_creator.h"
#include "indexlib/partition/operation_queue/remove_operation.h"
#include "indexlib/partition/operation_queue/update_field_operation.h"
#include "indexlib/partition/operation_queue/sub_doc_operation.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(partition);

class OperationFactory
{
public:
    OperationFactory()
        : mMainPkType(it_unknown)
        , mSubPkType(it_unknown)
    {}
    
    virtual ~OperationFactory() {}

public:
    void Init(const config::IndexPartitionSchemaPtr& schema);
    virtual OperationBase* CreateOperation(const document::NormalDocumentPtr& doc,
                                           autil::mem_pool::Pool* pool);

    OperationBase* CreateUnInitializedOperation(
        OperationBase::SerializedOperationType opType,
        int64_t timestamp,
        autil::mem_pool::Pool* pool) const;
    
    OperationBase* DeserializeOperation(const char* buffer, 
            autil::mem_pool::Pool* pool, size_t& opSize) const;

private:
    OperationCreatorPtr CreateRemoveOperationCreator(
            const config::IndexPartitionSchemaPtr& schema);
    OperationCreatorPtr CreateUpdateFieldOperationCreator(
            const config::IndexPartitionSchemaPtr& schema);

private:
    IndexType mMainPkType;
    IndexType mSubPkType;
    
    OperationCreatorPtr mRemoveOperationCreator;
    OperationCreatorPtr mRemoveSubOperationCreator;
    OperationCreatorPtr mUpdateOperationCreator;

private:
    friend class OperationFactoryTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationFactory);

inline OperationBase* OperationFactory::CreateUnInitializedOperation(
    OperationBase::SerializedOperationType opType,
    int64_t timestamp,
    autil::mem_pool::Pool *pool) const
{
    OperationBase* operation = NULL;
    assert(mMainPkType == it_primarykey64 || mMainPkType == it_primarykey128);
    
    switch (opType)
    {
    case OperationBase::REMOVE_OP:
        operation = (mMainPkType == it_primarykey64)
            ? static_cast<OperationBase*>(
                IE_POOL_COMPATIBLE_NEW_CLASS(pool,
                                          RemoveOperation<uint64_t>, 
                                          timestamp))
            : static_cast<OperationBase*>(
                IE_POOL_COMPATIBLE_NEW_CLASS(pool,
                                          RemoveOperation<autil::uint128_t>,
                                          timestamp));
        break;
    case OperationBase::UPDATE_FIELD_OP:
        operation = (mMainPkType == it_primarykey64)
            ? static_cast<OperationBase*>(
                IE_POOL_COMPATIBLE_NEW_CLASS(pool,
                                          UpdateFieldOperation<uint64_t>, 
                                          timestamp))
            : static_cast<OperationBase*>(
                IE_POOL_COMPATIBLE_NEW_CLASS(pool,
                                          UpdateFieldOperation<autil::uint128_t>,
                                          timestamp));
        break;
    case OperationBase::SUB_DOC_OP:
        assert(mSubPkType == it_primarykey64 || mSubPkType == it_primarykey128);
        operation = IE_POOL_COMPATIBLE_NEW_CLASS(
            pool, SubDocOperation, timestamp, mMainPkType, mSubPkType);
        break;
    default:
        assert(false);
    }
    return operation;
}

inline OperationBase* OperationFactory::DeserializeOperation(const char* buffer, 
            autil::mem_pool::Pool* pool, size_t& opSize) const
{
    assert(buffer);
    char* baseAddr = const_cast<char*>(buffer);
    uint8_t serializeOpType = *(uint8_t*)baseAddr;
    OperationBase::SerializedOperationType opType = 
        (OperationBase::SerializedOperationType)serializeOpType;
    baseAddr += sizeof(serializeOpType);
    int64_t timestamp = *(int64_t*)baseAddr;
    baseAddr += sizeof(timestamp);
    OperationBase* operation = CreateUnInitializedOperation(opType, timestamp, pool);
    if (!operation)
    {
        INDEXLIB_FATAL_ERROR(
                InconsistentState,"Deserialize operation[type: %d] failed.", 
                serializeOpType);
    }
    operation->Load(pool, baseAddr);
    opSize = (const char*)baseAddr - buffer;
    return operation;
}

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPERATION_FACTORY_H
