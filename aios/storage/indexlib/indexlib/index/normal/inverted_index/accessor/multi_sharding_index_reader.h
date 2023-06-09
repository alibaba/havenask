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
#ifndef __INDEXLIB_MULTI_SHARDING_INDEX_READER_H
#define __INDEXLIB_MULTI_SHARDING_INDEX_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/format/ShardingIndexHasher.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, KeyIterator);
DECLARE_REFERENCE_CLASS(index, PostingIterator);
DECLARE_REFERENCE_CLASS(index, IndexMetrics);

namespace indexlib { namespace index {
class DictionaryReader;
class SegmentPosting;
class SectionAttributeReader;

class MultiShardingIndexReader : public index::LegacyIndexReader
{
public:
    MultiShardingIndexReader(IndexMetrics* indexMetrics) : mIndexMetrics(indexMetrics) {};
    ~MultiShardingIndexReader() {};

public:
    void Open(const config::IndexConfigPtr& indexConfig, const index_base::PartitionDataPtr& partitionData,
              const InvertedIndexReader* hintReader = nullptr) override;

    index::Result<index::PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize = 1000,
                                                  PostingType type = pt_default,
                                                  autil::mem_pool::Pool* pool = NULL) override;
    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* pool,
                file_system::ReadOption option) noexcept override;

    index::Result<index::PostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                                         uint32_t statePoolSize, PostingType type,
                                                         autil::mem_pool::Pool* pool) override;

    void Update(docid_t docId, const document::ModifiedTokens& modifiedTokens) override;
    void UpdateByShardId(size_t shardId, docid_t docId, const document::ModifiedTokens& modifiedTokens);

    void UpdateIndex(IndexUpdateTermIterator* iterator) override;
    void SetLegacyAccessoryReader(const std::shared_ptr<LegacyIndexAccessoryReader>& accessoryReader) override;

    const index::SectionAttributeReader* GetSectionReader(const std::string& indexName) const override;

    std::shared_ptr<KeyIterator> CreateKeyIterator(const std::string& indexName) override;

    bool GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           InvertedIndexSearchTracer* tracer = nullptr) override;
    future_lite::coro::Lazy<index::Result<bool>>
    GetSegmentPostingAsync(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           file_system::ReadOption option, InvertedIndexSearchTracer* tracer) noexcept override;

    void SetMultiFieldIndexReader(InvertedIndexReader* truncateReader) override
    {
        for (size_t i = 0; i < mShardingIndexReaders.size(); i++) {
            mShardingIndexReaders[i]->SetMultiFieldIndexReader(truncateReader);
        }
    }

private:
    void Update(docid_t docId, index::DictKeyInfo termKey, bool isDelete) override;
    void UpdateByShardId(size_t shardId, docid_t docId, index::DictKeyInfo termKey, bool isDelete);

public:
    std::vector<std::shared_ptr<DictionaryReader>> GetDictReaders() const;
    [[deprecated("use CreateMainPostingIteratorAsync")]] index::PostingIterator*
    CreateMainPostingIterator(const index::DictKeyInfo& key, uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool,
                              bool needBuildingSegment);
    future_lite::coro::Lazy<index::Result<index::PostingIterator*>>
    CreateMainPostingIteratorAsync(const index::DictKeyInfo key, uint32_t statePoolSize,
                                   autil::mem_pool::Pool* sessionPool, bool needBuildingSegment,
                                   file_system::ReadOption option,
                                   indexlib::index::InvertedIndexSearchTracer* tracer = nullptr) noexcept;

private:
    index::IndexMetrics* mIndexMetrics;
    std::vector<NormalIndexReaderPtr> mShardingIndexReaders;
    std::shared_ptr<ShardingIndexHasher> mShardingIndexHasher;
    std::shared_ptr<LegacyIndexAccessoryReader> mAccessoryReader;

private:
    friend class MultiShardingIndexReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiShardingIndexReader);

/////////////////////////////////////////////////////////////////////////
inline bool MultiShardingIndexReader::GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                        SegmentPosting& segPosting, InvertedIndexSearchTracer* tracer)
{
    size_t shardingIdx = mShardingIndexHasher->GetShardingIdx(key);
    return mShardingIndexReaders[shardingIdx]->GetSegmentPosting(key, segmentIdx, segPosting, tracer);
}

inline future_lite::coro::Lazy<index::Result<bool>>
MultiShardingIndexReader::GetSegmentPostingAsync(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                 SegmentPosting& segPosting, file_system::ReadOption option,
                                                 InvertedIndexSearchTracer* tracer) noexcept
{
    size_t shardingIdx = mShardingIndexHasher->GetShardingIdx(key);
    co_return co_await mShardingIndexReaders[shardingIdx]->GetSegmentPostingAsync(key, segmentIdx, segPosting, option,
                                                                                  tracer);
}
}} // namespace indexlib::index

#endif //__INDEXLIB_MULTI_SHARDING_INDEX_READER_H
