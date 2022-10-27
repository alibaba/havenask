#ifndef __INDEXLIB_TRIE_INDEX_READER_H
#define __INDEXLIB_TRIE_INDEX_READER_H

#include <tr1/memory>
#include <vector>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/normal/trie/trie_index_segment_reader.h"
#include "indexlib/index/normal/trie/trie_index_define.h"
#include "indexlib/index/normal/trie/trie_index_iterator.h"
#include "indexlib/index/normal/trie/building_trie_index_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"

IE_NAMESPACE_BEGIN(index);

class TrieIndexReader : public PrimaryKeyIndexReader
{
public:
    typedef std::pair<autil::ConstString, docid_t> KVPair;
    typedef std::vector<KVPair, autil::mem_pool::pool_allocator<KVPair> > KVPairVector;
    typedef TrieIndexIterator Iterator;
private:
    typedef TrieIndexSegmentReader SegmentReader;
    typedef TrieIndexSegmentReaderPtr SegmentReaderPtr;
    typedef std::pair<docid_t, SegmentReaderPtr> SegmentPair;
    typedef std::vector<SegmentPair> SegmentVector;

public:
    TrieIndexReader()
        : mOptimizeSearch(false)
    {}
    ~TrieIndexReader() {}

public:
    void Open(const config::IndexConfigPtr& indexConfig,
              const index_base::PartitionDataPtr& partitionData) override;
    void OpenWithoutPKAttribute(
            const config::IndexConfigPtr& indexConfig, 
            const index_base::PartitionDataPtr& partitionData) override
    { return Open(indexConfig, partitionData); }

    std::string GetIndexName() const
    { return mIndexConfig->GetIndexName(); }
    size_t EstimateLoadSize(const index_base::PartitionDataPtr& partitionData,
                            const config::IndexConfigPtr& indexConfig,
                            const index_base::Version& lastLoadVersion) override;

public:
    docid_t Lookup(const std::string &pkStr) const override
    { return Lookup(autil::ConstString(pkStr.data(), pkStr.size())); }
    docid_t Lookup(const autil::ConstString& pkStr) const override;
    
    //TODO: not support building segment search now
    // MatchBeginning
    size_t PrefixSearch(const autil::ConstString& key, KVPairVector& results,
                        int32_t maxMatches = TRIE_MAX_MATCHES) const;
    Iterator* PrefixSearch(const autil::ConstString& key, 
                           autil::mem_pool::Pool* sessionPool, 
                           int32_t maxMatches = TRIE_MAX_MATCHES) const;
    // MatchAll / SubSearch
    size_t InfixSearch(const autil::ConstString& key, KVPairVector& results,
                       int32_t maxMatches = TRIE_MAX_MATCHES) const;
    Iterator* InfixSearch(const autil::ConstString& key, 
                          autil::mem_pool::Pool* sessionPool, 
                          int32_t maxMatches = TRIE_MAX_MATCHES) const;

private:
    void LoadSegments(const index_base::PartitionDataPtr& partitionData,
                      SegmentVector& segmentReaders);
    
    void InitBuildingIndexReader(const index_base::PartitionDataPtr& partitionData);

    void AddSegmentResults(docid_t baseDocid, const KVPairVector& segmentResults,
                           KVPairVector& results, int32_t& leftMatches) const;
    bool IsDocidValid(docid_t gDocId) const;
    size_t InnerPrefixSearch(const autil::ConstString& key, KVPairVector& results,
                             int32_t maxMatches) const;

    IndexReader* Clone() const override
    { assert(false); return NULL; }
    PostingIterator* Lookup(const common::Term& term, 
            uint32_t statePoolSize = 1000, PostingType type = pt_default,
                            autil::mem_pool::Pool *sessionPool = NULL) override
    { assert(false); return NULL; }
    AttributeReaderPtr GetPKAttributeReader() const override
    { assert(false); return AttributeReaderPtr(); }
    docid_t LookupWithPKHash(const autil::uint128_t& pkHash) const override
    { assert(false); return INVALID_DOCID; }
    
private:
    SegmentVector mSegmentVector;
    config::IndexConfigPtr mIndexConfig;
    DeletionMapReaderPtr mDeletionMapReader;
    BuildingTrieIndexReaderPtr mBuildingIndexReader;
    bool mOptimizeSearch;

private:
    friend class TrieIndexInteTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TrieIndexReader);

///////////////////////////////////////////////////////////////
inline TrieIndexReader::Iterator* TrieIndexReader::PrefixSearch(
        const autil::ConstString& key, autil::mem_pool::Pool* sessionPool,
        int32_t maxMatches) const
{
    KVPairVector* results = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            KVPairVector, KVPairVector::allocator_type(sessionPool));
    PrefixSearch(key, *results, maxMatches);
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, Iterator, results);
}

inline size_t TrieIndexReader::PrefixSearch(const autil::ConstString& key,
        KVPairVector& results, int32_t maxMatches) const
{
    results.clear();
    return InnerPrefixSearch(key, results, maxMatches);
}

inline TrieIndexReader::Iterator* TrieIndexReader::InfixSearch(
        const autil::ConstString& key, autil::mem_pool::Pool* sessionPool, 
        int32_t maxMatches) const
{
    KVPairVector* results = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            KVPairVector, KVPairVector::allocator_type(sessionPool));
    InfixSearch(key, *results, maxMatches);
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, Iterator, results);
}

inline size_t TrieIndexReader::InfixSearch(const autil::ConstString& key,
        KVPairVector& results, int32_t maxMatches) const
{
    results.clear();
    int32_t leftMatches = maxMatches;
    for (size_t i = 0; i < key.size(); ++i)
    {
        InnerPrefixSearch(key.subString(i), results, leftMatches);
        leftMatches = maxMatches - results.size();
        if (leftMatches <= 0)
        {
            assert(leftMatches == 0);
            break;
        }
    }
    return results.size();
}

inline size_t TrieIndexReader::InnerPrefixSearch(const autil::ConstString& key,
        KVPairVector& results, int32_t maxMatches) const
{
    //TODO: not support building search now
    //nomarly we have only one segment(full) and no delete
    if (likely(mOptimizeSearch))
    {
        const SegmentReaderPtr& segmentReader = mSegmentVector[0].second;
        return segmentReader->PrefixSearch(key, results, maxMatches);
    }

    int32_t leftMatches = maxMatches;
    for (size_t i = 0; i < mSegmentVector.size(); i++)
    {
        if (unlikely(leftMatches <= 0))
        {
            assert(leftMatches == 0);
            break;
        }

        KVPairVector segmentResults(results.get_allocator());
        const SegmentReaderPtr& segmentReader = mSegmentVector[i].second;
        segmentReader->PrefixSearch(key, segmentResults, TRIE_MAX_MATCHES);
        AddSegmentResults(mSegmentVector[i].first, segmentResults,
                          results, leftMatches);
    }
    return results.size();
}

inline bool TrieIndexReader::IsDocidValid(docid_t gDocId) const
{
    assert(gDocId != INVALID_DOCID);
    if (mDeletionMapReader && mDeletionMapReader->IsDeleted(gDocId))
    {
        return false;
    }
    return true;
}

inline void TrieIndexReader::AddSegmentResults(docid_t baseDocid,
        const KVPairVector& segmentResults, KVPairVector& results,
        int32_t& leftMatches) const
{
    for (size_t i = 0; i < segmentResults.size(); i++)
    {
        docid_t gDocid = segmentResults[i].second + baseDocid;
        if (IsDocidValid(gDocid))
        {
            results.push_back(std::make_pair(segmentResults[i].first, gDocid));
            --leftMatches;
            if (unlikely(leftMatches <= 0))
            {
                assert(leftMatches == 0);
                return;
            }
        }
    }
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRIE_INDEX_READER_H
