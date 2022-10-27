#include "indexlib/partition/operation_queue/sub_doc_operation.h"
#include "indexlib/partition/operation_queue/update_field_operation.h"
#include "indexlib/partition/operation_queue/remove_operation.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"


using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SubDocOperation);

SubDocOperation::~SubDocOperation()
{
    if (mMainOperation)
    {
        mMainOperation->~OperationBase();
    }
    for (size_t i = 0; i < mSubOperationCount; ++i)
    {
        mSubOperations[i]->~OperationBase();
    }
}

void SubDocOperation::Init(
    DocOperateType docOperateType, OperationBase* mainOperation, 
    OperationBase** subOperations, size_t subOperationCount)
{
    mDocOperateType = docOperateType;
    mMainOperation = mainOperation;
    mSubOperations = subOperations;
    mSubOperationCount = subOperationCount;
}

void SubDocOperation::Process(
    const PartitionModifierPtr& modifier,
    const OperationRedoHint& redoHint)
{
    SubDocModifierPtr subDocModifier = 
        DYNAMIC_POINTER_CAST(SubDocModifier, modifier);
    assert(subDocModifier);
    if (!subDocModifier)
    {
        IE_LOG(ERROR, "cast modifier to SubDocModifier failed");
        return;
    }

    if (mMainOperation)
    {
        mMainOperation->Process(subDocModifier->GetMainModifier(), redoHint);
    }
    if (mSubOperationCount > 0)
    {
        const PartitionModifierPtr& subModifier = subDocModifier->GetSubModifier();
        OperationRedoHint subRedoHint;
        for (size_t i = 0; i < mSubOperationCount; ++i)
        {
            assert(mSubOperations[i]);
            mSubOperations[i]->Process(subModifier, subRedoHint);
        }
    }
}

OperationBase* SubDocOperation::Clone(Pool *pool)
{
    OperationBase* mainOperation = NULL;
    if (mMainOperation)
    {
        mainOperation = mMainOperation->Clone(pool);
    }

    OperationBase** subOperations = NULL;
    if (mSubOperationCount > 0)
    {
        assert(mSubOperations);
        subOperations = IE_POOL_COMPATIBLE_NEW_VECTOR(
                pool, OperationBase*, mSubOperationCount);
        if (!subOperations)
        {
            IE_LOG(ERROR, "allocate memory fail!");
            return NULL;
        }
        for (size_t i = 0; i < mSubOperationCount; ++i)
        {
            assert(mSubOperations[i]);
            subOperations[i] = mSubOperations[i]->Clone(pool);
        }
    }

    SubDocOperation *operation = 
        IE_POOL_COMPATIBLE_NEW_CLASS(
            pool, SubDocOperation, GetTimestamp(), mMainPkType, mSubPkType);
    operation->Init(mDocOperateType, mainOperation,
                    subOperations, mSubOperationCount);
    return operation;
}

size_t SubDocOperation::GetMemoryUse() const
{
    size_t totalSize = sizeof(SubDocOperation);
    if (mMainOperation)
    {
        totalSize += mMainOperation->GetMemoryUse();
    }

    for (size_t i = 0; i < mSubOperationCount; i++)
    {
        assert(mSubOperations[i]);
        totalSize += mSubOperations[i]->GetMemoryUse();
        totalSize += sizeof(OperationBase*);
    }
    return totalSize;
}

segmentid_t SubDocOperation::GetSegmentId() const
{
    if (mMainOperation)
    {
        segmentid_t segmentId = mMainOperation->GetSegmentId();
        if (segmentId != INVALID_SEGMENTID)
        {
            return segmentId;
        }
    }
    if (mSubOperationCount > 0)
    {
        return mSubOperations[0]->GetSegmentId();
    }
    return INVALID_SEGMENTID;
}

size_t SubDocOperation::GetSerializeSize() const 
{
    size_t totalSize 
        = sizeof(uint8_t)  // docOperateType
        + sizeof(uint8_t)  // is main operation exist flag 
        + (mMainOperation ? mMainOperation->GetSerializeSize() : 0) // main operation data
        + common::VByteCompressor::GetVInt32Length(mSubOperationCount); // num of sub operations
    for (size_t i = 0; i < mSubOperationCount; ++i)
    {
        totalSize += mSubOperations[i]->GetSerializeSize();
    }
    return totalSize;
}

size_t SubDocOperation::Serialize(char* buffer, size_t bufferLen) const
{
    char* cur = buffer;
    *(uint8_t*)cur = (uint8_t)mDocOperateType;
    cur += sizeof(uint8_t);
    uint8_t isMainOpExist = mMainOperation ? 1 : 0;
    *(uint8_t*)cur = isMainOpExist;
    cur += sizeof(isMainOpExist);
    if (isMainOpExist)
    {
        cur += mMainOperation->Serialize(cur, bufferLen - (cur - buffer));
    }
    common::VByteCompressor::WriteVUInt32(mSubOperationCount, cur);
    for (size_t i = 0; i < mSubOperationCount; ++i)
    {
        cur += mSubOperations[i]->Serialize(cur, bufferLen - (cur - buffer));
    }
    return cur - buffer;
}

bool SubDocOperation::Load(Pool *pool, char*& cursor)
{
    mDocOperateType = static_cast<DocOperateType>(*(uint8_t*)cursor);
    cursor += sizeof(uint8_t);
    bool isMainOpExist = *(uint8_t*)cursor ? true : false;
    cursor += sizeof(uint8_t);
    if (isMainOpExist)
    {
        mMainOperation = LoadOperation(pool, cursor, mDocOperateType, mMainPkType);
        if (!mMainOperation)
        {
            return false;
        }
    }
    mSubOperationCount = common::VByteCompressor::ReadVUInt32(cursor);
    if (mSubOperationCount > 0)
    {
        mSubOperations = IE_POOL_COMPATIBLE_NEW_VECTOR(
            pool, OperationBase*, mSubOperationCount);
        if (!mSubOperations)
        {
            IE_LOG(ERROR, "allocate memory fail!");
            return false;
        }
    }
    for (size_t i = 0; i < mSubOperationCount; ++i)
    {
        mSubOperations[i] = LoadOperation(pool, cursor, mDocOperateType, mSubPkType);
        if (!mSubOperations[i])
        {
            return false;
        }
    }
    return true;
}

OperationBase* SubDocOperation::LoadOperation(
    Pool *pool, char*& cursor,
    DocOperateType docOpType, IndexType pkType)
{
    OperationBase* operation = NULL;
    switch(docOpType)
    {
    case UPDATE_FIELD:
        operation =
            (pkType == it_primarykey64)
            ? static_cast<OperationBase*>(
                IE_POOL_COMPATIBLE_NEW_CLASS(
                    pool, UpdateFieldOperation<uint64_t>, mTimestamp))
            : static_cast<OperationBase*>(
                IE_POOL_COMPATIBLE_NEW_CLASS(
                    pool, UpdateFieldOperation<uint128_t>, mTimestamp));
        break;
    case DELETE_SUB_DOC:
        operation =
            (pkType == it_primarykey64)
            ? static_cast<OperationBase*>(
                IE_POOL_COMPATIBLE_NEW_CLASS(
                    pool, RemoveOperation<uint64_t>, mTimestamp))
            : static_cast<OperationBase*>(
                IE_POOL_COMPATIBLE_NEW_CLASS(
                    pool, RemoveOperation<uint128_t>, mTimestamp));
        break;
    default: assert(false); return NULL;
    }
    if (!operation)
    {
        IE_LOG(ERROR, "allocate memory fail!");
        return NULL;
    }
    if (!operation->Load(pool, cursor))
    {
        IE_LOG(ERROR, "load operation fail!");
        return NULL;
    }
    return operation;
}



IE_NAMESPACE_END(partition);

