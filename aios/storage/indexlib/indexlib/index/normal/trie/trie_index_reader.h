/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader_interface.h"
#include "indexlib/index/normal/trie/building_trie_index_reader.h"
#include "indexlib/index/normal/trie/trie_index_define.h"
#include "indexlib/index/normal/trie/trie_index_iterator.h"
#include "indexlib/index/normal/trie/trie_index_segment_reader.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class TrieIndexReader : public PrimaryKeyIndexReader, public LegacyPrimaryKeyReaderInterface
{
public:
    typedef std::pair<autil::StringView, docid_t> KVPair;
    typedef std::vector<KVPair, autil::mem_pool::pool_allocator<KVPair>> KVPairVector;
    typedef TrieIndexIterator Iterator;

private:
    typedef TrieIndexSegmentReader SegmentReader;
    typedef TrieIndexSegmentReaderPtr SegmentReaderPtr;
    typedef std::pair<docid_t, SegmentReaderPtr> SegmentPair;
    typedef std::vector<SegmentPair> SegmentVector;

public:
    TrieIndexReader() : PrimaryKeyIndexReader(), mOptimizeSearch(false) {}
    ~TrieIndexReader() {}

public:
    void Open(const config::IndexConfigPtr& indexConfig, const index_base::PartitionDataPtr& partitionData,
              const InvertedIndexReader* hintReader = nullptr) override;
    void OpenWithoutPKAttribute(const config::IndexConfigPtr& indexConfig,
                                const index_base::PartitionDataPtr& partitionData, bool forceReverseLookup) override
    {
        return Open(indexConfig, partitionData, nullptr);
    }

    size_t EstimateLoadSize(const index_base::PartitionDataPtr& partitionData,
                            const config::IndexConfigPtr& indexConfig,
                            const index_base::Version& lastLoadVersion) override;

    Status OpenWithoutPKAttribute(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                  const indexlibv2::framework::TabletData* tabletData) override
    {
        assert(false);
        return Status::Unimplement();
    }
    AttributeReaderPtr GetLegacyPKAttributeReader() const override
    {
        assert(0);
        return nullptr;
    }
    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const indexlibv2::framework::TabletData* tabletData) override
    {
        assert(0);
        return Status::Unimplement();
    }

public:
    docid64_t Lookup(const std::string& pkStr) const override { return Lookup(pkStr, nullptr); }
    docid64_t Lookup(const std::string& pkStr, future_lite::Executor* executor) const override
    {
        return Lookup(autil::StringView(pkStr.data(), pkStr.size()));
    }
    docid64_t Lookup(const autil::StringView& pkStr) const override;

    // TODO: not support building segment search now
    // MatchBeginning
    size_t PrefixSearch(const autil::StringView& key, KVPairVector& results,
                        int32_t maxMatches = TRIE_MAX_MATCHES) const;
    Iterator* PrefixSearch(const autil::StringView& key, autil::mem_pool::Pool* sessionPool,
                           int32_t maxMatches = TRIE_MAX_MATCHES) const;
    // MatchAll / SubSearch
    size_t InfixSearch(const autil::StringView& key, KVPairVector& results,
                       int32_t maxMatches = TRIE_MAX_MATCHES) const;
    Iterator* InfixSearch(const autil::StringView& key, autil::mem_pool::Pool* sessionPool,
                          int32_t maxMatches = TRIE_MAX_MATCHES) const;

public:
    void SetMultiFieldIndexReader(InvertedIndexReader* truncateReader) override {}
    void InitBuildResourceMetricsNode(util::BuildResourceMetrics* buildResourceMetrics) override {}
    void SetLegacyAccessoryReader(const std::shared_ptr<LegacyIndexAccessoryReader>& accessorReader) override {}
    std::shared_ptr<LegacyIndexAccessoryReader> GetLegacyAccessoryReader() const override { return nullptr; }

    void Update(docid_t docId, const document::ModifiedTokens& tokens) override { assert(false); }
    void Update(docid_t docId, index::DictKeyInfo termKey, bool isDelete) override { assert(false); }
    // UpdateIndex is used to update patch format index.
    void UpdateIndex(IndexUpdateTermIterator* iter) override { assert(false); }

private:
    void LoadSegments(const index_base::PartitionDataPtr& partitionData, SegmentVector& segmentReaders,
                      std::vector<segmentid_t>& segmentIds, const InvertedIndexReader* hintReader);

    void InitBuildingIndexReader(const index_base::PartitionDataPtr& partitionData);

    void AddSegmentResults(docid_t baseDocid, const KVPairVector& segmentResults, KVPairVector& results,
                           int32_t& leftMatches) const;
    bool IsDocidValid(docid_t gDocId) const;
    size_t InnerPrefixSearch(const autil::StringView& key, KVPairVector& results, int32_t maxMatches) const;

    index::Result<PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize = 1000,
                                           PostingType type = pt_default,
                                           autil::mem_pool::Pool* sessionPool = NULL) override
    {
        assert(false);
        return NULL;
    }
    std::shared_ptr<indexlibv2::index::AttributeReader> GetPKAttributeReader() const override
    {
        assert(false);
        return nullptr;
    }
    docid64_t LookupWithPKHash(const autil::uint128_t& pkHash, future_lite::Executor* executor) const override
    {
        assert(false);
        return INVALID_DOCID;
    }
    virtual bool LookupWithPKHash(const autil::uint128_t& pkHash, segmentid_t specifySegment,
                                  docid64_t* docid) const override
    {
        assert(false);
        return INVALID_DOCID;
    }

    docid64_t LookupWithDocRange(const autil::uint128_t& pkHash, std::pair<docid_t, docid_t> docRange,
                                 future_lite::Executor* executor) const override
    {
        assert(false);
        return INVALID_DOCID;
    }

private:
    SegmentVector mSegmentVector;
    DeletionMapReaderPtr mDeletionMapReader;
    BuildingTrieIndexReaderPtr mBuildingIndexReader;
    bool mOptimizeSearch;

    std::vector<segmentid_t> mSegmentIds;

private:
    friend class TrieIndexInteTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TrieIndexReader);

///////////////////////////////////////////////////////////////
inline TrieIndexReader::Iterator* TrieIndexReader::PrefixSearch(const autil::StringView& key,
                                                                autil::mem_pool::Pool* sessionPool,
                                                                int32_t maxMatches) const
{
    KVPairVector* results =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, KVPairVector, KVPairVector::allocator_type(sessionPool));
    PrefixSearch(key, *results, maxMatches);
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, Iterator, results);
}

inline size_t TrieIndexReader::PrefixSearch(const autil::StringView& key, KVPairVector& results,
                                            int32_t maxMatches) const
{
    results.clear();
    return InnerPrefixSearch(key, results, maxMatches);
}

inline TrieIndexReader::Iterator*
TrieIndexReader::InfixSearch(const autil::StringView& key, autil::mem_pool::Pool* sessionPool, int32_t maxMatches) const
{
    KVPairVector* results =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, KVPairVector, KVPairVector::allocator_type(sessionPool));
    InfixSearch(key, *results, maxMatches);
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, Iterator, results);
}

inline size_t TrieIndexReader::InfixSearch(const autil::StringView& key, KVPairVector& results,
                                           int32_t maxMatches) const
{
    results.clear();
    int32_t leftMatches = maxMatches;
    for (size_t i = 0; i < key.size(); ++i) {
        InnerPrefixSearch(key.substr(i), results, leftMatches);
        leftMatches = maxMatches - results.size();
        if (leftMatches <= 0) {
            assert(leftMatches == 0);
            break;
        }
    }
    return results.size();
}

inline size_t TrieIndexReader::InnerPrefixSearch(const autil::StringView& key, KVPairVector& results,
                                                 int32_t maxMatches) const
{
    // TODO: not support building search now
    // nomarly we have only one segment(full) and no delete
    if (likely(mOptimizeSearch)) {
        const SegmentReaderPtr& segmentReader = mSegmentVector[0].second;
        return segmentReader->PrefixSearch(key, results, maxMatches);
    }

    int32_t leftMatches = maxMatches;
    for (size_t i = 0; i < mSegmentVector.size(); i++) {
        if (unlikely(leftMatches <= 0)) {
            assert(leftMatches == 0);
            break;
        }

        KVPairVector segmentResults(results.get_allocator());
        const SegmentReaderPtr& segmentReader = mSegmentVector[i].second;
        segmentReader->PrefixSearch(key, segmentResults, TRIE_MAX_MATCHES);
        AddSegmentResults(mSegmentVector[i].first, segmentResults, results, leftMatches);
    }
    return results.size();
}

inline bool TrieIndexReader::IsDocidValid(docid_t gDocId) const
{
    assert(gDocId != INVALID_DOCID);
    if (mDeletionMapReader && mDeletionMapReader->IsDeleted(gDocId)) {
        return false;
    }
    return true;
}

inline void TrieIndexReader::AddSegmentResults(docid_t baseDocid, const KVPairVector& segmentResults,
                                               KVPairVector& results, int32_t& leftMatches) const
{
    for (size_t i = 0; i < segmentResults.size(); i++) {
        docid_t gDocid = segmentResults[i].second + baseDocid;
        if (IsDocidValid(gDocid)) {
            results.push_back(std::make_pair(segmentResults[i].first, gDocid));
            --leftMatches;
            if (unlikely(leftMatches <= 0)) {
                assert(leftMatches == 0);
                return;
            }
        }
    }
}
}} // namespace indexlib::index
