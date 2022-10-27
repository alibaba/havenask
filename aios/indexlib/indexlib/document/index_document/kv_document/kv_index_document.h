#ifndef __INDEXLIB_KV_INDEX_DOCUMENT_H
#define __INDEXLIB_KV_INDEX_DOCUMENT_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(document);

class KVIndexDocument
{
public:
    KVIndexDocument(autil::mem_pool::Pool* pool = NULL);
    ~KVIndexDocument();

public:
    KVIndexDocument& operator = (const KVIndexDocument& other);
    uint64_t GetPkeyHash() const { return mPkeyHash; }
    uint64_t GetSkeyHash() const { return mSkeyHash; }
    void SetPkeyHash(uint64_t key) { mPkeyHash = key; }
    void SetSkeyHash(uint64_t key) { mHasSkey = true; mSkeyHash = key; }
    bool HasSkey() const { return mHasSkey; }
    void serialize(autil::DataBuffer &dataBuffer) const;
    
    void deserialize(autil::DataBuffer &dataBuffer,
                     autil::mem_pool::Pool *pool,
                     uint32_t docVersion = DOCUMENT_BINARY_VERSION);
    
    bool operator == (const KVIndexDocument& right) const 
    {
        return mPkeyHash == right.mPkeyHash
        && mSkeyHash == right.mSkeyHash && mHasSkey == right.mHasSkey;
    }
    
    bool operator != (const KVIndexDocument& right) const 
    {
        return !( operator == (right));
    }
    
    void Reset() {
        mPkeyHash = 0;
        mSkeyHash = 0;
        mHasSkey = false;
    }
private:
    uint64_t mPkeyHash;
    uint64_t mSkeyHash;
    bool mHasSkey;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVIndexDocument);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_KV_INDEX_DOCUMENT_H
