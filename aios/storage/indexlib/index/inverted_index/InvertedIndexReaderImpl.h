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
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/common/DictHasher.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/InvertedLeafReader.h"
#include "indexlib/index/inverted_index/KeyIteratorTyped.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexReader.h"

namespace indexlib::util {
class KeyHasher;
}

namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlib::index {
class DictionaryReader;
class IndexAccessoryReader;
class SegmentPosting;
class SectionAttributeReader;
class BuildingIndexReader;
class IndexMetrics;
class BufferedPostingIterator;
class DynamicIndexReader;
class InvertedLeafReader;
class InvertedIndexMetrics;
class TermMeta;
class InvertedDiskIndexer;

class InvertedIndexReaderImpl : public InvertedIndexReader
{
public:
    explicit InvertedIndexReaderImpl(const std::shared_ptr<InvertedIndexMetrics>& metrics);
    ~InvertedIndexReaderImpl();

    using Indexer = std::tuple<docid64_t, std::shared_ptr<indexlibv2::index::IIndexer>, segmentid_t,
                               indexlibv2::framework::Segment::SegmentStatus>;

public:
    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const indexlibv2::framework::TabletData* tabletData) override;

    void SetAccessoryReader(const std::shared_ptr<IndexAccessoryReader>& accessoryReader) override;

    index::Result<PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize, PostingType type,
                                           autil::mem_pool::Pool* sessionPool) override;

    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* pool,
                file_system::ReadOption option) noexcept override;

    index::Result<PostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                                  uint32_t statePoolSize, PostingType type,
                                                  autil::mem_pool::Pool* sessionPool) override;
    const SectionAttributeReader* GetSectionReader(const std::string& indexName) const override;

    std::shared_ptr<KeyIterator> CreateKeyIterator(const std::string& indexName) override
    {
        return std::shared_ptr<KeyIterator>(new KeyIteratorTyped<InvertedIndexReaderImpl>(*this));
    }

    bool GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           file_system::ReadOption option = file_system::ReadOption(),
                           InvertedIndexSearchTracer* tracer = nullptr) override;
    future_lite::coro::Lazy<index::Result<bool>>
    GetSegmentPostingAsync(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           file_system::ReadOption option, InvertedIndexSearchTracer* tracer) noexcept override;

public:
    std::vector<std::shared_ptr<DictionaryReader>> GetDictReaders() const;

    // only main chain and no section reader
    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    CreateMainPostingIteratorAsync(index::DictKeyInfo key, uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool,
                                   bool needBuildingSegment, file_system::ReadOption option) noexcept;

    index::Result<bool> GetMainChainTermMeta(const SegmentPosting& mainChainSegPosting, const index::DictKeyInfo& key,
                                             uint32_t segmentIdx, TermMeta& termMeta, file_system::ReadOption option,
                                             InvertedIndexSearchTracer* tracer) noexcept;

protected:
    virtual BufferedPostingIterator*
    CreateBufferedPostingIterator(autil::mem_pool::Pool* sessionPool,
                                  util::PooledUniquePtr<InvertedIndexSearchTracer> tracer) const;
    virtual std::shared_ptr<BuildingIndexReader> CreateBuildingIndexReader() const;
    virtual Status DoOpen(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                          const std::vector<Indexer>& indexers);

    DocIdRangeVector MergeDocIdRanges(int32_t hintValues, const DocIdRangeVector& ranges) const;
    bool ValidatePartitonRange(const DocIdRangeVector& ranges);
    future_lite::coro::Lazy<Result<PostingIterator*>>
    CreatePostingIteratorByHashKey(const index::Term* term, index::DictKeyInfo termHashKey,
                                   const DocIdRangeVector& ranges, uint32_t statePoolSize,
                                   autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept;

private:
    std::pair<Status, std::shared_ptr<InvertedDiskIndexer>>
    CreateDefaultDiskIndexer(const std::shared_ptr<indexlibv2::framework::Segment>& segment,
                             const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig);
    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    DoLookupAsync(const index::Term* term, const DocIdRangeVector& ranges, uint32_t statePoolSize, PostingType type,
                  autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept;
    future_lite::coro::Lazy<Result<PostingIterator*>>
    CreateDynamicPostingIteratorAsync(const Term* term, const DocIdRangeVector& ranges, uint32_t statePoolSize,
                                      autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept;
    future_lite::coro::Lazy<Result<PostingIterator*>>
    CreatePostingIteratorAsync(const Term* term, const DocIdRangeVector& ranges, uint32_t statePoolSize,
                               autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept;
    void AddBuildingSegmentReader(docid64_t baseDocId, const std::shared_ptr<IndexSegmentReader>& segReader);
    bool NeedTruncatePosting(const index::Term& term) const;

    std::tuple<std::vector<future_lite::coro::Lazy<index::Result<bool>>>, SegmentPostingVector,
               /*needBuildingSegment*/ bool>
    FillRangeByBuiltSegments(const index::Term* term, const index::DictKeyInfo& termHashKey,
                             const DocIdRangeVector& ranges, file_system::ReadOption option,
                             InvertedIndexSearchTracer* tracer) noexcept;
    future_lite::coro::Lazy<index::Result<bool>>
    FillSegmentPostingAsync(const index::Term* term, const index::DictKeyInfo& key, uint32_t segmentIdx,
                            SegmentPosting& segPosting, file_system::ReadOption option,
                            InvertedIndexSearchTracer* tracer) noexcept;
    future_lite::coro::Lazy<index::Result<bool>>
    FillTruncSegmentPosting(const index::Term& term, const index::DictKeyInfo& key, uint32_t segmentIdx,
                            SegmentPosting& segPosting, file_system::ReadOption option,
                            InvertedIndexSearchTracer* tracer) noexcept;
    future_lite::coro::Lazy<index::Result<bool>>
    GetSegmentPostingFromTruncIndex(const index::Term& term, const index::DictKeyInfo& key, uint32_t segmentIdx,
                                    file_system::ReadOption option, SegmentPosting& segPosting,
                                    InvertedIndexSearchTracer* tracer) noexcept;
    Status TryOpenTruncateIndexReader(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                      const indexlibv2::framework::TabletData* tabletData);

protected:
    std::vector<std::shared_ptr<InvertedLeafReader>> _segmentReaders;
    std::vector<docid64_t> _baseDocIds;
    std::shared_ptr<BuildingIndexReader> _buildingIndexReader;

private:
    IndexDictHasher _dictHasher;
    std::shared_ptr<config::HighFrequencyVocabulary> _highFreqVol;
    std::unique_ptr<BitmapIndexReader> _bitmapIndexReader;
    std::unique_ptr<DynamicIndexReader> _dynamicIndexReader;
    future_lite::Executor* _executor = nullptr;
    bool _indexSupportNull = false;
    std::shared_ptr<IndexAccessoryReader> _accessoryReader;
    std::shared_ptr<InvertedIndexMetrics> _indexMetrics;
    std::map<std::string, std::shared_ptr<InvertedIndexReaderImpl>> _truncateIndexReaders;

private:
    friend class MultiShardInvertedIndexReader;
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
inline bool InvertedIndexReaderImpl::GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                       SegmentPosting& segPosting, file_system::ReadOption option,
                                                       InvertedIndexSearchTracer* tracer)
{
    // TODO: use session pool when use thread bind pool object
    return _segmentReaders[segmentIdx]->GetSegmentPosting(key, _baseDocIds[segmentIdx], segPosting, nullptr, option,
                                                          tracer);
}

inline future_lite::coro::Lazy<index::Result<bool>>
InvertedIndexReaderImpl::GetSegmentPostingAsync(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                SegmentPosting& segPosting, file_system::ReadOption option,
                                                InvertedIndexSearchTracer* tracer) noexcept
{
    if (segmentIdx >= _segmentReaders.size()) {
        co_return false;
    }
    if (_executor) {
        co_return co_await _segmentReaders[segmentIdx]->GetSegmentPostingAsync(key, _baseDocIds[segmentIdx], segPosting,
                                                                               nullptr, option, tracer);
    }
    try {
        bool success = GetSegmentPosting(key, segmentIdx, segPosting, option, tracer);
        co_return success;
    } catch (...) {
        AUTIL_LOG(ERROR, "get segment posting failed");
        co_return ErrorCode::FileIO;
    }
    co_return false;
}

} // namespace indexlib::index
