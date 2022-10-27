#ifndef __INDEXLIB_REMOVE_OPERATION_H
#define __INDEXLIB_REMOVE_OPERATION_H

#include <tr1/memory>
#include <autil/LongHashValue.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/index/partition_info.h"

IE_NAMESPACE_BEGIN(partition);

template<typename T>
class RemoveOperation : public OperationBase
{
public:
    RemoveOperation(int64_t timestamp);
    ~RemoveOperation() {}
    
public:
    void Init(T pk, segmentid_t segmentId);

    
    bool Load(autil::mem_pool::Pool *pool, char*& cursor) override;
     
    
    void Process(
        const partition::PartitionModifierPtr& modifier,
        const OperationRedoHint& redoHint) override;

    
    OperationBase* Clone(autil::mem_pool::Pool *pool) override;
    
    
    SerializedOperationType GetSerializedType() const override
    {
        return REMOVE_OP;
    }

    
    DocOperateType GetDocOperateType() const override
    { return DELETE_DOC; }

    
    size_t GetMemoryUse() const override
    { return sizeof(RemoveOperation); }

    
    segmentid_t GetSegmentId() const override
    { return mSegmentId; }

    
    size_t GetSerializeSize() const override
    { return sizeof(mPkHash) + sizeof(mSegmentId); }

    
    size_t Serialize(char* buffer, size_t bufferLen) const override;

    const T& GetPkHash() const
    {
        return mPkHash;
    }
    
private:
    T mPkHash;
    segmentid_t mSegmentId;

private:
    friend class RemoveOperationTest;
    friend class RemoveOperationCreatorTest;
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////////

IE_LOG_SETUP_TEMPLATE(partition, RemoveOperation);

template<typename T>
RemoveOperation<T>::RemoveOperation(int64_t timestamp)
    : OperationBase(timestamp)
    , mPkHash(T())
    , mSegmentId(INVALID_SEGMENTID)
{
}

template<typename T>
void RemoveOperation<T>::Init(T pk, segmentid_t segmentId)
{
    mPkHash = pk;
    mSegmentId = segmentId;
}

template<typename T>
void RemoveOperation<T>::Process(
    const partition::PartitionModifierPtr& modifier,
    const OperationRedoHint& redoHint)
{
    assert(modifier);
    const index::PartitionInfoPtr& partInfo = modifier->GetPartitionInfo();
    assert(partInfo);
    
    docid_t docId = INVALID_DOCID;
    segmentid_t segId = INVALID_SEGMENTID;
    if (redoHint.IsValid())
    {
        segId = redoHint.GetSegmentId();
        docId = partInfo->GetBaseDocId(segId) + redoHint.GetLocalDocId();
    }
    else
    {
        const index::PrimaryKeyIndexReaderPtr& pkIndexReader = 
            modifier->GetPrimaryKeyIndexReader();
        assert(pkIndexReader);
        docId =  pkIndexReader->LookupWithPKHash(mPkHash);
        segId = partInfo->GetSegmentId(docId);
    }
    modifier->RemoveDocument(docId);
    mSegmentId = segId;
}

template<typename T>
OperationBase* RemoveOperation<T>::Clone(autil::mem_pool::Pool *pool)
{
    RemoveOperation *clonedOperation = 
        IE_POOL_COMPATIBLE_NEW_CLASS(pool, RemoveOperation, 
                                  mTimestamp);
    clonedOperation->Init(mPkHash, mSegmentId);
    if (!clonedOperation)
    {
        IE_LOG(ERROR, "allocate memory fail!");
        return NULL;
    }
    return clonedOperation;
}

template<typename T>
size_t RemoveOperation<T>::Serialize(char* buffer, size_t bufferLen) const
{
    assert(bufferLen >= GetSerializeSize());
    char* cur = buffer;
    *(T*)cur = mPkHash;
    cur += sizeof(T);
    *(segmentid_t*)cur = mSegmentId;
    cur += sizeof(mSegmentId);
    return cur - buffer;
}

template<typename T>
bool RemoveOperation<T>::Load(autil::mem_pool::Pool *pool, char*& cursor)
{
    mPkHash = *(T*)cursor;
    cursor += sizeof(T);
    mSegmentId = *(segmentid_t*)cursor;
    cursor += sizeof(mSegmentId);
    return true;
}

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_REMOVE_OPERATION_H
