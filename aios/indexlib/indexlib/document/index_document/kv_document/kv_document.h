#ifndef __INDEXLIB_KV_DOCUMENT_H
#define __INDEXLIB_KV_DOCUMENT_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/document/index_document/kv_document/kv_index_document.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(document);

class KVDocument
{
public:
    KVDocument(autil::mem_pool::Pool* pool)
        : mPool(pool)
    {}
    ~KVDocument() {}
    
public:
    void SetKVIndexDocument(const KVIndexDocument& kvIndexDoc)
    {
        mKVIndexDocument = kvIndexDoc;
    }
    void SetValue(const autil::ConstString& value)
    {
        mValue = autil::ConstString(value.data(), value.size(), mPool);
    }

    void SetOperateType(DocOperateType op) 
    { mOpType = op; }
    DocOperateType GetOperateType() const { return mOpType; }

    bool DeleteAllPkey()
    { return mOpType == DELETE_DOC && !mKVIndexDocument.HasSkey(); }
    void SetTimestamp(int64_t timestamp) { mTimestamp = timestamp; }
    void SetUserTimestamp(int64_t timestamp) { mUserTimestamp = timestamp; }
    int64_t GetTimestamp() const { return mTimestamp; }
    int64_t GetUserTimestamp() const { return mUserTimestamp; }
    const KVIndexDocument& GetKVIndexDocument() const
    { return mKVIndexDocument; }
    const autil::ConstString& GetValue() const { return mValue; }
    size_t GetMemoryUse() { return sizeof(KVDocument) + mValue.size(); }

private:
    KVIndexDocument mKVIndexDocument;
    autil::ConstString mValue;
    int64_t mTimestamp;
    int64_t mUserTimestamp;
    DocOperateType mOpType;
    autil::mem_pool::Pool* mPool;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVDocument);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_KV_DOCUMENT_H
