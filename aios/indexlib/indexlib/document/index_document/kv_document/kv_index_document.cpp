#include "indexlib/document/index_document/kv_document/kv_index_document.h"

using namespace std;

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, KVIndexDocument);

KVIndexDocument::KVIndexDocument(autil::mem_pool::Pool* pool)
    : mPkeyHash(0)
    , mSkeyHash(0)
    , mHasSkey(false)
{
}

KVIndexDocument::~KVIndexDocument() 
{
}

KVIndexDocument& KVIndexDocument::operator = (
    const KVIndexDocument& other)
{
    mPkeyHash = other.mPkeyHash;
    mSkeyHash = other.mSkeyHash;
    mHasSkey = other.mHasSkey;
    return *this;
}

void KVIndexDocument::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(mPkeyHash);
    dataBuffer.write(mSkeyHash);
    dataBuffer.write(mHasSkey);
}

void KVIndexDocument::deserialize(autil::DataBuffer &dataBuffer,
                                  autil::mem_pool::Pool *pool,
                                  uint32_t docVersion)
{
    dataBuffer.read(mPkeyHash);
    dataBuffer.read(mSkeyHash);
    dataBuffer.read(mHasSkey);
}

IE_NAMESPACE_END(document);

