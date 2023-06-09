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
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_reader.h"

#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/KeyIteratorTyped.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, MultiShardingIndexReader);

void MultiShardingIndexReader::Open(const IndexConfigPtr& indexConfig, const PartitionDataPtr& partitionData,
                                    const InvertedIndexReader* hintReader)
{
    assert(indexConfig);
    assert(partitionData);
    assert(indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING);

    _indexConfig = indexConfig;
    mShardingIndexHasher.reset(new ShardingIndexHasher);
    mShardingIndexHasher->Init(_indexConfig);
    _indexFormatOption->Init(indexConfig);

    const vector<IndexConfigPtr>& shardIndexConfigs = GetIndexConfig()->GetShardingIndexConfigs();
    auto typedHintReader = dynamic_cast<const MultiShardingIndexReader*>(hintReader);
    assert(typedHintReader == nullptr || shardIndexConfigs.size() == typedHintReader->mShardingIndexReaders.size());
    for (size_t i = 0; i < shardIndexConfigs.size(); ++i) {
        std::shared_ptr<InvertedIndexReader> indexReader(
            IndexReaderFactory::CreateIndexReader(shardIndexConfigs[i]->GetInvertedIndexType(), mIndexMetrics));
        NormalIndexReaderPtr normalReader = DYNAMIC_POINTER_CAST(NormalIndexReader, indexReader);
        if (!normalReader) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "Cast in sharding index reader fail!\n");
        }
        const InvertedIndexReader* hintShardReader =
            typedHintReader ? typedHintReader->mShardingIndexReaders[i].get() : nullptr;
        normalReader->Open(shardIndexConfigs[i], partitionData, hintShardReader);
        mShardingIndexReaders.push_back(normalReader);
    }
}

future_lite::coro::Lazy<index::Result<PostingIterator*>>
MultiShardingIndexReader::LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type,
                                      autil::mem_pool::Pool* pool, file_system::ReadOption option) noexcept
{
    index::DictKeyInfo termHashKey;
    size_t shardingIdx = 0;
    if (!mShardingIndexHasher->GetShardingIdx(*term, termHashKey, shardingIdx)) {
        IE_LOG(ERROR, "failed to get sharding idx for term [%s] in index [%s]", term->GetWord().c_str(),
               term->GetIndexName().c_str());
        co_return nullptr;
    }
    assert(shardingIdx < mShardingIndexReaders.size());
    co_return co_await mShardingIndexReaders[shardingIdx]->LookupAsync(term, statePoolSize, type, pool, option);
}

index::Result<index::PostingIterator*> MultiShardingIndexReader::Lookup(const index::Term& term, uint32_t statePoolSize,
                                                                        PostingType type, Pool* pool)
{
    index::DictKeyInfo termHashKey;
    size_t shardingIdx = 0;
    if (!mShardingIndexHasher->GetShardingIdx(term, termHashKey, shardingIdx)) {
        IE_LOG(ERROR, "failed to get sharding idx for term [%s] in index [%s]", term.GetWord().c_str(),
               term.GetIndexName().c_str());
        return NULL;
    }
    assert(shardingIdx < mShardingIndexReaders.size());
    return mShardingIndexReaders[shardingIdx]->Lookup(term, statePoolSize, type, pool);
}

index::Result<index::PostingIterator*> MultiShardingIndexReader::PartialLookup(const index::Term& term,
                                                                               const DocIdRangeVector& ranges,
                                                                               uint32_t statePoolSize, PostingType type,
                                                                               autil::mem_pool::Pool* pool)
{
    index::DictKeyInfo termHashKey;
    size_t shardingIdx = 0;
    if (!mShardingIndexHasher->GetShardingIdx(term, termHashKey, shardingIdx)) {
        IE_LOG(ERROR, "failed to get sharding idx for term [%s] in index [%s]", term.GetWord().c_str(),
               term.GetIndexName().c_str());
        return nullptr;
    }
    assert(shardingIdx < mShardingIndexReaders.size());
    return mShardingIndexReaders[shardingIdx]->PartialLookup(term, ranges, statePoolSize, type, pool);
}

void MultiShardingIndexReader::UpdateByShardId(size_t shardId, docid_t docId,
                                               const document::ModifiedTokens& modifiedTokens)
{
    for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
        auto token = modifiedTokens[i];
        index::DictKeyInfo termKey(token.first);
        UpdateByShardId(shardId, docId, termKey, token.second == document::ModifiedTokens::Operation::REMOVE);
    }
    document::ModifiedTokens::Operation op;
    if (modifiedTokens.GetNullTermOperation(&op)) {
        index::DictKeyInfo termKey = index::DictKeyInfo::NULL_TERM;
        UpdateByShardId(shardId, docId, termKey, op == document::ModifiedTokens::Operation::REMOVE);
    }
}

void MultiShardingIndexReader::UpdateByShardId(size_t shardId, docid_t docId, index::DictKeyInfo termKey, bool isDelete)
{
    size_t shardingIdx = mShardingIndexHasher->GetShardingIdx(termKey);
    if (shardingIdx != shardId) {
        return;
    }
    mShardingIndexReaders[shardingIdx]->Update(docId, termKey, isDelete);
}

void MultiShardingIndexReader::Update(docid_t docId, const document::ModifiedTokens& modifiedTokens)
{
    for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
        auto token = modifiedTokens[i];
        index::DictKeyInfo termKey(token.first);
        Update(docId, termKey, token.second == document::ModifiedTokens::Operation::REMOVE);
    }
    document::ModifiedTokens::Operation op;
    if (modifiedTokens.GetNullTermOperation(&op)) {
        index::DictKeyInfo termKey = index::DictKeyInfo::NULL_TERM;
        Update(docId, termKey, op == document::ModifiedTokens::Operation::REMOVE);
    }
}

void MultiShardingIndexReader::Update(docid_t docId, index::DictKeyInfo termKey, bool isDelete)
{
    size_t shardingIdx = mShardingIndexHasher->GetShardingIdx(termKey);
    mShardingIndexReaders[shardingIdx]->Update(docId, termKey, isDelete);
}

void MultiShardingIndexReader::UpdateIndex(IndexUpdateTermIterator* iterator)
{
    auto termKey = iterator->GetTermKey();
    size_t shardingIdx = mShardingIndexHasher->GetShardingIdx(termKey);
    mShardingIndexReaders[shardingIdx]->UpdateIndex(iterator);
}

void MultiShardingIndexReader::SetLegacyAccessoryReader(
    const std::shared_ptr<LegacyIndexAccessoryReader>& accessoryReader)
{
    mAccessoryReader = accessoryReader;
    for (size_t i = 0; i < mShardingIndexReaders.size(); i++) {
        mShardingIndexReaders[i]->SetLegacyAccessoryReader(accessoryReader);
    }
}

const SectionAttributeReader* MultiShardingIndexReader::GetSectionReader(const string& indexName) const
{
    if (mAccessoryReader) {
        return mAccessoryReader->GetSectionReader(indexName).get();
    }
    return NULL;
}

std::shared_ptr<KeyIterator> MultiShardingIndexReader::CreateKeyIterator(const std::string& indexName)
{
    return std::shared_ptr<KeyIterator>(new KeyIteratorTyped<MultiShardingIndexReader>(*this));
}

vector<std::shared_ptr<DictionaryReader>> MultiShardingIndexReader::GetDictReaders() const
{
    vector<std::shared_ptr<DictionaryReader>> dictReaders;
    for (size_t i = 0; i < mShardingIndexReaders.size(); ++i) {
        vector<std::shared_ptr<DictionaryReader>> inShardingDictReaders = mShardingIndexReaders[i]->GetDictReaders();
        dictReaders.insert(dictReaders.end(), inShardingDictReaders.begin(), inShardingDictReaders.end());
    }
    return dictReaders;
}

PostingIterator* MultiShardingIndexReader::CreateMainPostingIterator(const index::DictKeyInfo& key,
                                                                     uint32_t statePoolSize,
                                                                     autil::mem_pool::Pool* sessionPool,
                                                                     bool needBuildingSegment)
{
    // size_t shardingIdx = mShardingIndexHasher->GetShardingIdx(key);
    // assert(shardingIdx < mShardingIndexReaders.size());
    // return mShardingIndexReaders[shardingIdx]->CreateMainPostingIterator(key, statePoolSize, sessionPool,
    //                                                                      needBuildingSegment);
    assert(false);
    return nullptr;
}

future_lite::coro::Lazy<index::Result<index::PostingIterator*>>
MultiShardingIndexReader::CreateMainPostingIteratorAsync(const index::DictKeyInfo key, uint32_t statePoolSize,
                                                         autil::mem_pool::Pool* sessionPool, bool needBuildingSegment,
                                                         file_system::ReadOption option,
                                                         indexlib::index::InvertedIndexSearchTracer* tracer) noexcept
{
    size_t shardingIdx = mShardingIndexHasher->GetShardingIdx(key);
    assert(shardingIdx < mShardingIndexReaders.size());
    co_return co_await mShardingIndexReaders[shardingIdx]->CreateMainPostingIteratorAsync(
        key, statePoolSize, sessionPool, needBuildingSegment, option, tracer);
}
}} // namespace indexlib::index
