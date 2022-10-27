#ifndef __INDEXLIB_SHARDING_INDEX_HASHER_H
#define __INDEXLIB_SHARDING_INDEX_HASHER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/term.h"
#include "indexlib/util/key_hasher_typed.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/document/index_document/normal_document/token.h"

DECLARE_REFERENCE_CLASS(config, IndexConfig);

IE_NAMESPACE_BEGIN(index);

class ShardingIndexHasher
{
public:
    ShardingIndexHasher() {}
    ~ShardingIndexHasher() {}

public:
    void Init(const config::IndexConfigPtr& indexConfig);

    bool GetShardingIdx(const common::Term &term, dictkey_t &termHashKey,
                        size_t &shardingIdx)
    {
        if (!mHasher->GetHashKey(term, termHashKey))
        {
            IE_LOG(ERROR, "invalid term [%s], index name [%s]",
                    term.GetWord().c_str(), term.GetIndexName().c_str());
            return false;
        }

        dictkey_t retrievalHashKey = index::IndexWriter::GetRetrievalHashKey(
                mIsNumberIndex, termHashKey);
        shardingIdx = GetShardingIdxByRetrievalKey(retrievalHashKey, mShardingCount);
        return true;
    }

    size_t GetShardingIdx(const document::Token* token)
    {
        dictkey_t retrievalHashKey = index::IndexWriter::GetRetrievalHashKey(
                mIsNumberIndex, token->GetHashKey());
        return GetShardingIdxByRetrievalKey(retrievalHashKey, mShardingCount);
    }

    size_t GetShardingIdx(dictkey_t termHashKey)
    {
        dictkey_t retrievalHashKey = index::IndexWriter::GetRetrievalHashKey(
                mIsNumberIndex, termHashKey);
        return GetShardingIdxByRetrievalKey(retrievalHashKey, mShardingCount);
    }

public:
    static size_t GetShardingIdxByRetrievalKey(
            dictkey_t retrievalHashKey, size_t shardingCount)
    {
        return retrievalHashKey % shardingCount;
    }
    
private:
    util::KeyHasherPtr mHasher;
    size_t mShardingCount;
    bool mIsNumberIndex;
            
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ShardingIndexHasher);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SHARDING_INDEX_HASHER_H
