#ifndef __INDEXLIB_OPERATION_BASE_H
#define __INDEXLIB_OPERATION_BASE_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/operation_queue/operation_redo_hint.h"


IE_NAMESPACE_BEGIN(partition);

class OperationBase
{
public:
    enum SerializedOperationType
    {
        INVALID_SERIALIZE_OP = 0,
        REMOVE_OP = 1,
        UPDATE_FIELD_OP = 2,
        SUB_DOC_OP = 3
    };
    
public:
    OperationBase(int64_t timestamp) 
        : mTimestamp(timestamp)
    {}

    virtual ~OperationBase() {}
    
public:
    virtual bool Load(autil::mem_pool::Pool *pool, char*& cursor) = 0;
    virtual void Process(const partition::PartitionModifierPtr& modifier,
                         const OperationRedoHint& redoHint) = 0;
    virtual OperationBase* Clone(autil::mem_pool::Pool *pool) = 0;
    virtual SerializedOperationType GetSerializedType () const
    { return INVALID_SERIALIZE_OP; }
    
    virtual DocOperateType GetDocOperateType() const
    { return UNKNOWN_OP; }
    
    int64_t GetTimestamp() const { return mTimestamp; }
    virtual size_t GetMemoryUse() const = 0;
    virtual segmentid_t GetSegmentId() const = 0;
    virtual size_t GetSerializeSize() const = 0;
    virtual size_t Serialize(char* buffer, size_t bufferLen) const = 0;

protected:
    docid_t GetDocIdFromPrimaryKey(IndexPartitionReader* reader,
                                   const autil::uint128_t& pkHash);
    
protected:
    int64_t mTimestamp;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationBase);
///////////////////////////////////////////////////////////////////
inline docid_t OperationBase::GetDocIdFromPrimaryKey(
        IndexPartitionReader* reader, const autil::uint128_t& pkHash)
{
    assert(reader);
    index::PrimaryKeyIndexReaderPtr pkReader = reader->GetPrimaryKeyReader();
    if (!pkReader)
    {
        return INVALID_DOCID;
    }
    return pkReader->LookupWithPKHash(pkHash);
}

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPERATION_BASE_H
