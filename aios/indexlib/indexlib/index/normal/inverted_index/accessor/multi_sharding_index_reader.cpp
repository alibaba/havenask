#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"
#include "indexlib/index/normal/inverted_index/accessor/key_iterator_typed.h"
#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiShardingIndexReader);

void MultiShardingIndexReader::Open(const IndexConfigPtr& indexConfig,
                                    const PartitionDataPtr& partitionData)
{
    assert(indexConfig);
    assert(partitionData);
    assert(indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING);

    mIndexConfig = indexConfig;
    mShardingIndexHasher.reset(new ShardingIndexHasher);
    mShardingIndexHasher->Init(mIndexConfig);
    mIndexFormatOption.Init(indexConfig);
    
    const vector<IndexConfigPtr>& shardIndexConfigs =
        mIndexConfig->GetShardingIndexConfigs();
    for (size_t i = 0; i < shardIndexConfigs.size(); ++i)
    {
        IndexReaderPtr indexReader(IndexReaderFactory::CreateIndexReader(
                        shardIndexConfigs[i]->GetIndexType()));
        NormalIndexReaderPtr normalReader = DYNAMIC_POINTER_CAST(
                NormalIndexReader, indexReader);
        if (!normalReader)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "Cast in sharding index reader fail!\n");
        }
        normalReader->Open(shardIndexConfigs[i], partitionData);
        mShardingIndexReaders.push_back(normalReader);
    }
}


index::PostingIterator* MultiShardingIndexReader::Lookup(
        const common::Term& term, uint32_t statePoolSize, 
        PostingType type, Pool *pool)
{
    dictkey_t termHashKey = dictkey_t();
    size_t shardingIdx = 0;
    if (!mShardingIndexHasher->GetShardingIdx(term, termHashKey, shardingIdx))
    {
        IE_LOG(ERROR, "failed to get sharding idx for term [%s] in index [%s]",
               term.GetWord().c_str(), term.GetIndexName().c_str());
        return NULL;
    }
    assert(shardingIdx < mShardingIndexReaders.size());
    return mShardingIndexReaders[shardingIdx]->Lookup(
            term, statePoolSize, type, pool);
}

index::PostingIterator* MultiShardingIndexReader::PartialLookup(const common::Term& term,
    const DocIdRangeVector& ranges, uint32_t statePoolSize, PostingType type,
    autil::mem_pool::Pool* pool)
{
    dictkey_t termHashKey = dictkey_t();
    size_t shardingIdx = 0;
    if (!mShardingIndexHasher->GetShardingIdx(term, termHashKey, shardingIdx))
    {
        IE_LOG(ERROR, "failed to get sharding idx for term [%s] in index [%s]",
               term.GetWord().c_str(), term.GetIndexName().c_str());
        return nullptr;
    }
    assert(shardingIdx < mShardingIndexReaders.size());
    return mShardingIndexReaders[shardingIdx]->PartialLookup(
        term, ranges, statePoolSize, type, pool);
}
    
void MultiShardingIndexReader::SetAccessoryReader(
        const IndexAccessoryReaderPtr &accessoryReader)
{
    mAccessoryReader = accessoryReader;
    for (size_t i = 0; i < mShardingIndexReaders.size(); i++)
    {
        mShardingIndexReaders[i]->SetAccessoryReader(accessoryReader);
    }
}

const SectionAttributeReader* MultiShardingIndexReader::GetSectionReader(
        const string& indexName) const
{
    if (mAccessoryReader)
    {
        return mAccessoryReader->GetSectionReader(indexName).get();
    }
    return NULL;
}

KeyIteratorPtr MultiShardingIndexReader::CreateKeyIterator(const std::string& indexName)
{
    return index::KeyIteratorPtr(
            new index::KeyIteratorTyped<MultiShardingIndexReader>(*this));
}

vector<DictionaryReaderPtr> MultiShardingIndexReader::GetDictReaders() const
{
    vector<DictionaryReaderPtr> dictReaders;
    for (size_t i = 0; i < mShardingIndexReaders.size(); ++i)
    {
        vector<DictionaryReaderPtr> inShardingDictReaders = 
            mShardingIndexReaders[i]->GetDictReaders();
        dictReaders.insert(dictReaders.end(), inShardingDictReaders.begin(), 
                           inShardingDictReaders.end());
    }
    return dictReaders;
}

PostingIterator* MultiShardingIndexReader::CreateMainPostingIterator(
        dictkey_t key, uint32_t statePoolSize,
        autil::mem_pool::Pool *sessionPool)
{
    size_t shardingIdx = mShardingIndexHasher->GetShardingIdx(key);
    assert(shardingIdx < mShardingIndexReaders.size());
    return mShardingIndexReaders[shardingIdx]->CreateMainPostingIterator(
            key, statePoolSize, sessionPool);
}

IE_NAMESPACE_END(index);

