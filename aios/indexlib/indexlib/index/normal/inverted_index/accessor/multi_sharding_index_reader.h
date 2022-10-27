#ifndef __INDEXLIB_MULTI_SHARDING_INDEX_READER_H
#define __INDEXLIB_MULTI_SHARDING_INDEX_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/format/sharding_index_hasher.h"

DECLARE_REFERENCE_CLASS(index, SegmentPosting);
DECLARE_REFERENCE_CLASS(index, SectionAttributeReader);
DECLARE_REFERENCE_CLASS(index, KeyIterator);
DECLARE_REFERENCE_CLASS(index, PostingIterator);
DECLARE_REFERENCE_CLASS(index, DictionaryReader);

IE_NAMESPACE_BEGIN(index);

class MultiShardingIndexReader : public index::IndexReader
{
public:
    MultiShardingIndexReader() {};
    ~MultiShardingIndexReader() {};
public:
    void Open(const config::IndexConfigPtr& indexConfig,
              const index_base::PartitionDataPtr& partitionData) override;

    index::PostingIterator *Lookup(
            const common::Term& term,
            uint32_t statePoolSize = 1000, 
            PostingType type = pt_default,
            autil::mem_pool::Pool *pool = NULL) override;

    index::PostingIterator* PartialLookup(const common::Term& term, const DocIdRangeVector& ranges,
        uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* pool) override;    
    
    void SetAccessoryReader(
            const index::IndexAccessoryReaderPtr &accessoryReader) override;

    const index::SectionAttributeReader* GetSectionReader(
            const std::string& indexName) const override;

    index::KeyIteratorPtr CreateKeyIterator(const std::string& indexName) override;
    bool GetSegmentPosting(
            dictkey_t key, uint32_t segmentIdx,
            index::SegmentPosting &segPosting) override;
        
    IndexReader* Clone() const override
    {
        assert(false);
        return NULL;
    }

    void SetMultiFieldIndexReader(IndexReader *truncateReader) override
    {
        for (size_t i = 0; i < mShardingIndexReaders.size(); i++)
        {
            mShardingIndexReaders[i]->SetMultiFieldIndexReader(truncateReader);
        }
    }
    
public:
    std::vector<index::DictionaryReaderPtr> GetDictReaders() const;
    index::PostingIterator *CreateMainPostingIterator(
            dictkey_t key, uint32_t statePoolSize,
            autil::mem_pool::Pool *sessionPool);

private:
    std::vector<NormalIndexReaderPtr> mShardingIndexReaders;
    ShardingIndexHasherPtr mShardingIndexHasher;
    index::IndexAccessoryReaderPtr mAccessoryReader;
    
private:
    friend class MultiShardingIndexReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiShardingIndexReader);

/////////////////////////////////////////////////////////////////////////
inline bool MultiShardingIndexReader::GetSegmentPosting(
        dictkey_t key, uint32_t segmentIdx, index::SegmentPosting &segPosting)
{
    size_t shardingIdx = mShardingIndexHasher->GetShardingIdx(key);
    return mShardingIndexReaders[shardingIdx]->
        GetSegmentPosting(key, segmentIdx, segPosting);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_SHARDING_INDEX_READER_H
