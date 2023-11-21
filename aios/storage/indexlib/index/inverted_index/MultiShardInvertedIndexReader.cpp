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
#include "indexlib/index/inverted_index/MultiShardInvertedIndexReader.h"

#include "indexlib/framework/TabletData.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/inverted_index/IndexAccessoryReader.h"
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/InvertedMemIndexer.h"
#include "indexlib/index/inverted_index/MultiShardInvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/MultiShardInvertedMemIndexer.h"
#include "indexlib/index/inverted_index/SectionAttributeReader.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeReaderImpl.h"

namespace indexlib::index {
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::InvertedIndexConfig;
AUTIL_LOG_SETUP(indexlib.index, MultiShardInvertedIndexReader);

MultiShardInvertedIndexReader::MultiShardInvertedIndexReader(
    const indexlibv2::index::IndexReaderParameter& indexReaderParam)
    : _indexReaderParam(indexReaderParam)
{
}

Status MultiShardInvertedIndexReader::Open(const std::shared_ptr<IIndexConfig>& indexConfig,
                                           const indexlibv2::framework::TabletData* tabletData)
{
    auto config = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
    if (config == nullptr) {
        auto status = Status::InvalidArgs("fail to cast legacy index config, index name[%s] index type[%s]",
                                          config->GetIndexName().c_str(), config->GetIndexType().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    auto segments = tabletData->CreateSlice();
    std::vector<InvertedIndexReaderImpl::Indexer> indexers;
    docid64_t baseDocId = 0;
    for (auto it = segments.begin(); it != segments.end(); ++it) {
        const auto& segment = *it;
        auto [status, indexer] = segment->GetIndexer(indexConfig->GetIndexType(), config->GetIndexName());
        if (!status.IsOK()) {
            auto status = Status::InternalError("no indexer for [%s] in segment [%d]",
                                                indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
        if (segment->GetSegmentStatus() == indexlibv2::framework::Segment::SegmentStatus::ST_BUILT &&
            segment->GetSegmentInfo()->GetDocCount() == 0) {
            AUTIL_LOG(INFO, "segment [%d] has no doc count, open [%s] do nothing", segment->GetSegmentId(),
                      indexConfig->GetIndexName().c_str());
            continue;
        }
        indexers.emplace_back(baseDocId, indexer, segment->GetSegmentId(), segment->GetSegmentStatus());
        baseDocId += segment->GetSegmentInfo()->docCount;
    }
    return DoOpen(config, indexers, tabletData);
}

Status MultiShardInvertedIndexReader::DoOpen(const std::shared_ptr<InvertedIndexConfig>& indexConfig,
                                             const std::vector<InvertedIndexReaderImpl::Indexer>& indexers,
                                             const indexlibv2::framework::TabletData* tabletData)
{
    assert(indexConfig);
    _indexConfig = indexConfig;
    assert(_indexConfig->GetShardingType() == InvertedIndexConfig::IST_NEED_SHARDING);
    _indexHasher = std::make_unique<ShardingIndexHasher>();
    _indexHasher->Init(_indexConfig);
    _indexFormatOption->Init(_indexConfig);

    const std::vector<std::shared_ptr<InvertedIndexConfig>>& shardIndexConfigs =
        _indexConfig->GetShardingIndexConfigs();
    auto indexFactoryCreator = indexlibv2::index::IndexFactoryCreator::GetInstance();
    auto [status, indexFactory] = indexFactoryCreator->Create(indexConfig->GetIndexType());
    RETURN_IF_STATUS_ERROR(status, "get index factory for index type [%s] failed", indexConfig->GetIndexType().c_str());

    for (const auto& shardIndexConfig : shardIndexConfigs) {
        std::shared_ptr<IIndexReader> reader = indexFactory->CreateIndexReader(shardIndexConfig, _indexReaderParam);
        if (reader == nullptr) {
            AUTIL_LOG(ERROR, "failed to create index reader [%s]", shardIndexConfig->GetIndexName().c_str());
            return Status::InvalidArgs();
        }
        auto indexReader = std::dynamic_pointer_cast<InvertedIndexReaderImpl>(reader);
        if (indexReader == nullptr) {
            AUTIL_LOG(ERROR, "failed to cast to inverted index reader [%s]", shardIndexConfig->GetIndexName().c_str());
            return Status::InternalError();
        }

        std::vector<InvertedIndexReaderImpl::Indexer> shardIndexers;
        for (auto [baseDocId, indexer, segId, segStatus] : indexers) {
            if (segStatus == indexlibv2::framework::Segment::SegmentStatus::ST_BUILT) {
                auto shardedDiskIndexer = std::dynamic_pointer_cast<MultiShardInvertedDiskIndexer>(indexer);
                if (shardedDiskIndexer == nullptr) {
                    AUTIL_LOG(ERROR, "[%s] cast to sharded disk indexer failed", indexConfig->GetIndexName().c_str());
                    return Status::InternalError();
                }
                auto singleDiskIndexer = shardedDiskIndexer->GetSingleShardIndexer(shardIndexConfig->GetIndexName());
                if (singleDiskIndexer == nullptr) {
                    return Status::InternalError();
                }
                shardIndexers.emplace_back(baseDocId,
                                           std::static_pointer_cast<indexlibv2::index::IIndexer>(singleDiskIndexer),
                                           segId, segStatus);
            } else {
                auto shardedMemIndexer = std::dynamic_pointer_cast<MultiShardInvertedMemIndexer>(indexer);
                if (shardedMemIndexer == nullptr) {
                    AUTIL_LOG(ERROR, "[%s] cast to sharded disk indexer failed", indexConfig->GetIndexName().c_str());
                    return Status::InternalError();
                }
                auto singleMemIndexer = shardedMemIndexer->GetSingleShardIndexer(shardIndexConfig->GetIndexName());
                if (singleMemIndexer == nullptr) {
                    AUTIL_LOG(ERROR, "single mem indexer is nullptr");
                    return Status::InternalError();
                }
                shardIndexers.emplace_back(baseDocId,
                                           std::static_pointer_cast<indexlibv2::index::IIndexer>(singleMemIndexer),
                                           segId, segStatus);
            }
        }
        RETURN_IF_STATUS_ERROR(indexReader->DoOpen(shardIndexConfig, shardIndexers),
                               "do open shard indexer failed, indexName[%s]", shardIndexConfig->GetIndexName().c_str());
        RETURN_IF_STATUS_ERROR(indexReader->TryOpenTruncateIndexReader(shardIndexConfig, tabletData),
                               "do open shard indexer failed, indexName[%s]", shardIndexConfig->GetIndexName().c_str());
        _indexReaders.push_back(indexReader);
    }
    return Status::OK();
}

future_lite::coro::Lazy<index::Result<PostingIterator*>>
MultiShardInvertedIndexReader::LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type,
                                           autil::mem_pool::Pool* pool, file_system::ReadOption option) noexcept
{
    index::DictKeyInfo termHashKey;
    size_t shardIdx = 0;
    if (!_indexHasher->GetShardingIdx(*term, termHashKey, shardIdx)) {
        AUTIL_LOG(ERROR, "failed to get sharding idx for term [%s] in index [%s]", term->GetWord().c_str(),
                  term->GetIndexName().c_str());
        co_return nullptr;
    }
    assert(shardIdx < _indexReaders.size());
    co_return co_await _indexReaders[shardIdx]->LookupAsync(term, statePoolSize, type, pool, option);
}

index::Result<PostingIterator*> MultiShardInvertedIndexReader::Lookup(const index::Term& term, uint32_t statePoolSize,
                                                                      PostingType type, autil::mem_pool::Pool* pool)
{
    index::DictKeyInfo termHashKey;
    size_t shardIdx = 0;
    if (!_indexHasher->GetShardingIdx(term, termHashKey, shardIdx)) {
        AUTIL_LOG(ERROR, "failed to get sharding idx for term [%s] in index [%s]", term.GetWord().c_str(),
                  term.GetIndexName().c_str());
        return nullptr;
    }
    assert(shardIdx < _indexReaders.size());
    return _indexReaders[shardIdx]->Lookup(term, statePoolSize, type, pool);
}

index::Result<PostingIterator*> MultiShardInvertedIndexReader::PartialLookup(const index::Term& term,
                                                                             const DocIdRangeVector& ranges,
                                                                             uint32_t statePoolSize, PostingType type,
                                                                             autil::mem_pool::Pool* pool)
{
    index::DictKeyInfo termHashKey;
    size_t shardIdx = 0;
    if (!_indexHasher->GetShardingIdx(term, termHashKey, shardIdx)) {
        AUTIL_LOG(ERROR, "failed to get sharding idx for term [%s] in index [%s]", term.GetWord().c_str(),
                  term.GetIndexName().c_str());
        return nullptr;
    }
    assert(shardIdx < _indexReaders.size());
    return _indexReaders[shardIdx]->PartialLookup(term, ranges, statePoolSize, type, pool);
}

void MultiShardInvertedIndexReader::SetAccessoryReader(const std::shared_ptr<IndexAccessoryReader>& accessoryReader)
{
    _accessoryReader = accessoryReader;
    for (size_t i = 0; i < _indexReaders.size(); i++) {
        _indexReaders[i]->SetAccessoryReader(accessoryReader);
    }
}

const SectionAttributeReader* MultiShardInvertedIndexReader::GetSectionReader(const std::string& indexName) const
{
    if (_accessoryReader) {
        return _accessoryReader->GetSectionReader(indexName).get();
    }
    return nullptr;
}

std::shared_ptr<KeyIterator> MultiShardInvertedIndexReader::CreateKeyIterator(const std::string& indexName)
{
    return std::make_shared<KeyIteratorTyped<MultiShardInvertedIndexReader>>(*this);
}

std::vector<std::shared_ptr<DictionaryReader>> MultiShardInvertedIndexReader::GetDictReaders() const
{
    std::vector<std::shared_ptr<DictionaryReader>> dictReaders;
    for (size_t i = 0; i < _indexReaders.size(); ++i) {
        auto shardDictReaders = _indexReaders[i]->GetDictReaders();
        dictReaders.insert(dictReaders.end(), shardDictReaders.begin(), shardDictReaders.end());
    }
    return dictReaders;
}

future_lite::coro::Lazy<indexlib::index::Result<PostingIterator*>>
MultiShardInvertedIndexReader::CreateMainPostingIteratorAsync(const index::DictKeyInfo key, uint32_t statePoolSize,
                                                              autil::mem_pool::Pool* sessionPool,
                                                              bool needBuildingSegment,
                                                              file_system::ReadOption option) noexcept
{
    size_t shardIdx = _indexHasher->GetShardingIdx(key);
    assert(shardIdx < _indexReaders.size());
    co_return co_await _indexReaders[shardIdx]->CreateMainPostingIteratorAsync(key, statePoolSize, sessionPool,
                                                                               needBuildingSegment, option);
}

} // namespace indexlib::index
