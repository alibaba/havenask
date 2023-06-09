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

#include "autil/Log.h"
#include "future_lite/Executor.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/InvertedIndexReaderImpl.h"
#include "indexlib/index/inverted_index/format/ShardingIndexHasher.h"

namespace indexlibv2::framework {
class TabletData;
}

namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlib::index {

class MultiShardInvertedIndexReader : public InvertedIndexReader
{
public:
    constexpr static uint32_t DEFAULT_STATE_POOL_SIZE = 1000;

    explicit MultiShardInvertedIndexReader(const indexlibv2::index::IndexerParameter& indexerParam);
    ~MultiShardInvertedIndexReader() = default;

    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const indexlibv2::framework::TabletData* tabletData) override;

    void SetAccessoryReader(const std::shared_ptr<IndexAccessoryReader>& accessoryReader) override;

    index::Result<PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize = DEFAULT_STATE_POOL_SIZE,
                                           PostingType type = pt_default,
                                           autil::mem_pool::Pool* sessionPool = nullptr) override;

    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* pool,
                file_system::ReadOption option) noexcept override;

    index::Result<PostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                                  uint32_t statePoolSize, PostingType type,
                                                  autil::mem_pool::Pool* sessionPool) override;
    const SectionAttributeReader* GetSectionReader(const std::string& indexName) const override;

    std::shared_ptr<KeyIterator> CreateKeyIterator(const std::string& indexName) override;

    bool GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           file_system::ReadOption option, InvertedIndexSearchTracer* tracer = nullptr) override;
    future_lite::coro::Lazy<index::Result<bool>>
    GetSegmentPostingAsync(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           file_system::ReadOption option,
                           InvertedIndexSearchTracer* tracer = nullptr) noexcept override;

    std::vector<std::shared_ptr<DictionaryReader>> GetDictReaders() const;

    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    CreateMainPostingIteratorAsync(const index::DictKeyInfo key, uint32_t statePoolSize,
                                   autil::mem_pool::Pool* sessionPool, bool needBuildingSegment,
                                   file_system::ReadOption option) noexcept;

private:
    Status DoOpen(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                  const std::vector<InvertedIndexReaderImpl::Indexer>& indexers,
                  const indexlibv2::framework::TabletData* tabletData);

private:
    indexlibv2::index::IndexerParameter _indexerParam;
    std::vector<std::shared_ptr<InvertedIndexReaderImpl>> _indexReaders;
    std::unique_ptr<ShardingIndexHasher> _indexHasher;
    std::shared_ptr<IndexAccessoryReader> _accessoryReader;

    AUTIL_LOG_DECLARE();
};

inline bool MultiShardInvertedIndexReader::GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                             SegmentPosting& segPosting, file_system::ReadOption option,
                                                             InvertedIndexSearchTracer* tracer)
{
    size_t shardIdx = _indexHasher->GetShardingIdx(key);
    return _indexReaders[shardIdx]->GetSegmentPosting(key, segmentIdx, segPosting, option, tracer);
}

inline future_lite::coro::Lazy<index::Result<bool>>
MultiShardInvertedIndexReader::GetSegmentPostingAsync(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                      SegmentPosting& segPosting, file_system::ReadOption option,
                                                      InvertedIndexSearchTracer* tracer) noexcept
{
    size_t shardIdx = _indexHasher->GetShardingIdx(key);
    co_return co_await _indexReaders[shardIdx]->GetSegmentPostingAsync(key, segmentIdx, segPosting, option, tracer);
}

} // namespace indexlib::index
