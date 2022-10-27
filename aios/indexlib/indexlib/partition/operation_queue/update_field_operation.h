#ifndef __INDEXLIB_UPDATE_FIELD_OPERATION_H
#define __INDEXLIB_UPDATE_FIELD_OPERATION_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include <autil/LongHashValue.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/index/partition_info.h"

IE_NAMESPACE_BEGIN(partition);

typedef std::pair<fieldid_t, autil::ConstString> OperationItem;

template<typename T>
class UpdateFieldOperation : public OperationBase
{
public:
    UpdateFieldOperation(int64_t timestamp);
    ~UpdateFieldOperation() {}

public:
    void Init(T pk, OperationItem* items, size_t itemSize,
              segmentid_t segmentId);

    
    bool Load(autil::mem_pool::Pool *pool, char*& cursor) override;
    
    
    void Process(
        const partition::PartitionModifierPtr& modifier,
        const OperationRedoHint& redoHint) override;

    
    OperationBase* Clone(autil::mem_pool::Pool *pool) override;

    
    SerializedOperationType GetSerializedType() const override
    {
        return UPDATE_FIELD_OP;
    }
    
    
    DocOperateType GetDocOperateType() const override
    { return UPDATE_FIELD; }

    
    size_t GetMemoryUse() const override;

    
    segmentid_t GetSegmentId() const override
    { return mSegmentId; }

    
    size_t GetSerializeSize() const override;

    
    size_t Serialize(char* buffer, size_t bufferLen) const override;

private:
    T mPkHash;
    OperationItem* mItems;
    uint32_t mItemSize;
    segmentid_t mSegmentId;

private:
    friend class UpdateFieldOperationTest;
    friend class UpdateFieldOperationCreatorTest;

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////////

IE_LOG_SETUP_TEMPLATE(partition, UpdateFieldOperation);

template<typename T>
UpdateFieldOperation<T>::UpdateFieldOperation(
    int64_t timestamp)
    : OperationBase(timestamp)
    , mPkHash(T())
    , mItems(NULL)
    , mItemSize(0)
    , mSegmentId(INVALID_SEGMENTID)
{
}

template<typename T>
void UpdateFieldOperation<T>::Init(
    T pk, OperationItem* items, size_t itemSize,
    segmentid_t segmentId)
{
    assert(items);
    assert(itemSize > 0);

    mPkHash = pk;
    mItems = items;
    mItemSize = itemSize;
    mSegmentId = segmentId;
}

template<typename T>
void UpdateFieldOperation<T>::Process(
    const partition::PartitionModifierPtr& modifier,
    const OperationRedoHint& redoHint)
{
    assert(modifier);
    const index::PrimaryKeyIndexReaderPtr& pkIndexReader = 
        modifier->GetPrimaryKeyIndexReader();
    assert(pkIndexReader);
    docid_t docId = pkIndexReader->LookupWithPKHash(mPkHash);

    const index::PartitionInfoPtr& partInfo = modifier->GetPartitionInfo();
    assert(partInfo);
    mSegmentId = partInfo->GetSegmentId(docId);

    if (docId == INVALID_DOCID)
    {
        IE_LOG(DEBUG, "get docid from pkReader invalid.");   
        return;
    }
    
    for (uint32_t i = 0; i < mItemSize; i++)
    {
        fieldid_t fieldId = mItems[i].first;
        const autil::ConstString& value = mItems[i].second;
        modifier->UpdateField(docId, fieldId, value);
    }
}

template<typename T>
OperationBase* UpdateFieldOperation<T>::Clone(autil::mem_pool::Pool *pool)
{
    OperationItem *clonedItems = IE_POOL_COMPATIBLE_NEW_VECTOR(
            pool, OperationItem, mItemSize);
    if (!clonedItems)
    {
        IE_LOG(ERROR, "allocate memory fail!");
        return NULL;
    }
    
    for (uint32_t i = 0; i < mItemSize; ++i)
    {
        const autil::ConstString &srcData = mItems[i].second;
        autil::ConstString copyDataValue(srcData.data(), srcData.size(), pool);

        clonedItems[i].first = mItems[i].first;
        clonedItems[i].second = copyDataValue; 
    }

    UpdateFieldOperation *clonedOperation = 
        IE_POOL_COMPATIBLE_NEW_CLASS(pool, UpdateFieldOperation, 
                                  mTimestamp);
    clonedOperation->Init(mPkHash, clonedItems, mItemSize, mSegmentId);
    if (!clonedOperation)
    {
        IE_LOG(ERROR, "allocate memory fail!");
        return NULL;
    }
    return clonedOperation;
}

template<typename T>
size_t UpdateFieldOperation<T>::GetMemoryUse() const
{
    size_t totalSize = sizeof(UpdateFieldOperation);
    for (uint32_t i = 0; i < mItemSize; i++)
    {
        totalSize += sizeof(OperationItem);
        totalSize += mItems[i].second.size();
    }
    return totalSize;
}

template<typename T>
size_t UpdateFieldOperation<T>::GetSerializeSize() const
{
    size_t totalSize = sizeof(mPkHash) + sizeof(mSegmentId) +
        common::VByteCompressor::GetVInt32Length(mItemSize);
     
    for (uint32_t i = 0; i < mItemSize; i++)
    {
        fieldid_t fieldId = mItems[i].first;
        uint32_t valueSize = mItems[i].second.size();
        totalSize += common::VByteCompressor::GetVInt32Length(fieldId)
                   + common::VByteCompressor::GetVInt32Length(valueSize)
                   + valueSize;
    }
    return totalSize;
}

template<typename T>
size_t UpdateFieldOperation<T>::Serialize(char* buffer, size_t bufferLen) const
{
    assert(bufferLen >= GetSerializeSize());
    char* cur = buffer;
    *(T*)cur = mPkHash;
    cur += sizeof(T);
    *(segmentid_t*)cur = mSegmentId;
    cur += sizeof(mSegmentId);
    common::VByteCompressor::WriteVUInt32(mItemSize, cur);
    for (uint32_t i = 0; i < mItemSize; i++)
    {
        common::VByteCompressor::WriteVUInt32(mItems[i].first, cur);
        uint32_t valueLen = mItems[i].second.size();
        common::VByteCompressor::WriteVUInt32(valueLen, cur);
        memcpy(cur, mItems[i].second.data(), valueLen);
        cur += valueLen;
    }
    return cur - buffer;
}

template <typename T>
bool UpdateFieldOperation<T>::Load(autil::mem_pool::Pool *pool, char*& cursor)
{
    mPkHash = *(T*)cursor;
    cursor += sizeof(T);
    mSegmentId = *(segmentid_t*)cursor;
    cursor += sizeof(mSegmentId);
    mItemSize = common::VByteCompressor::ReadVUInt32(cursor);
    mItems = IE_POOL_COMPATIBLE_NEW_VECTOR(
            pool, OperationItem, mItemSize);
    if (!mItems)
    {
        IE_LOG(ERROR, "allocate memory fail!");
        return false;
    }
    
    for (uint32_t i = 0; i < mItemSize; ++i)
    {
        mItems[i].first = common::VByteCompressor::ReadVUInt32(cursor);
        uint32_t valueLen = common::VByteCompressor::ReadVUInt32(cursor);
        // instead allocate space from pool, item value reuse fileNode's memory buffer
        mItems[i].second = autil::ConstString(cursor, valueLen);
        cursor += valueLen;
    }
    return true;
}


IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_UPDATE_FIELD_OPERATION_H
