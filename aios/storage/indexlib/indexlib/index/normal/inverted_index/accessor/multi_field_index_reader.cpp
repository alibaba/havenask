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
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"

#include "indexlib/config/index_config.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/normal/framework/multi_field_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/index_metrics.h"

using namespace indexlib::config;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, MultiFieldIndexReader);

using namespace std;
using namespace autil;

std::shared_ptr<InvertedIndexReader> MultiFieldIndexReader::RET_EMPTY_PTR = std::shared_ptr<InvertedIndexReader>();

MultiFieldIndexReader::MultiFieldIndexReader(IndexMetrics* indexMetrics)
    : mIndexMetrics(indexMetrics)
    , mEnableAccessCountors(false)
{
}

index::Result<PostingIterator*> MultiFieldIndexReader::Lookup(const Term& term, uint32_t statePoolSize,
                                                              PostingType type, autil::mem_pool::Pool* sessionPool)
{
    return future_lite::coro::syncAwait(LookupAsync(&term, statePoolSize, type, sessionPool, nullptr));
}

future_lite::coro::Lazy<index::Result<PostingIterator*>>
MultiFieldIndexReader::LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type,
                                   autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept
{
    if (!term) {
        co_return nullptr;
    }
    const std::shared_ptr<InvertedIndexReader>& reader = GetInvertedIndexReader(term->GetIndexName());
    if (reader) {
        co_return co_await reader->LookupAsync(term, statePoolSize, type, sessionPool, option);
    }
    co_return nullptr;
}

index::Result<PostingIterator*> MultiFieldIndexReader::PartialLookup(const index::Term& term,
                                                                     const DocIdRangeVector& ranges,
                                                                     uint32_t statePoolSize, PostingType type,
                                                                     autil::mem_pool::Pool* pool)
{
    const std::shared_ptr<InvertedIndexReader>& reader = GetInvertedIndexReader(term.GetIndexName());
    if (reader) {
        return reader->PartialLookup(term, ranges, statePoolSize, type, pool);
    }
    return nullptr;
}

const SectionAttributeReader* MultiFieldIndexReader::GetSectionReader(const string& indexName) const
{
    const std::shared_ptr<InvertedIndexReader>& reader = GetInvertedIndexReader(indexName);
    if (reader) {
        return reader->GetSectionReader(indexName);
    }
    return NULL;
}

void MultiFieldIndexReader::AddIndexReader(const IndexConfigPtr& indexConf,
                                           const std::shared_ptr<InvertedIndexReader>& reader)
{
    const string& indexName = indexConf->GetIndexName();
    indexid_t indexId = indexConf->GetIndexId();

    if (mName2PosMap.find(indexName) != mName2PosMap.end()) {
        INDEXLIB_FATAL_ERROR(Duplicate, "index reader for [%s] already exist!", indexName.c_str());
    }

    if (mIndexId2PosMap.find(indexId) != mIndexId2PosMap.end()) {
        INDEXLIB_FATAL_ERROR(Duplicate, "index reader for [id:%d] already exist!", indexId);
    }

    mName2PosMap.insert(std::make_pair(indexName, mIndexReaders.size()));
    mIndexId2PosMap.insert(std::make_pair(indexId, mIndexReaders.size()));
    mIndexReaders.push_back(reader);

    const auto& legacyReader = std::dynamic_pointer_cast<LegacyIndexReaderInterface>(reader);
    legacyReader->SetMultiFieldIndexReader(this);
    legacyReader->SetLegacyAccessoryReader(mAccessoryReader);
}

const std::shared_ptr<InvertedIndexReader>& MultiFieldIndexReader::GetInvertedIndexReader(const string& field) const
{
    auto it = mName2PosMap.find(field);
    if (it != mName2PosMap.end()) {
        if (likely(mIndexMetrics != NULL && mEnableAccessCountors)) {
            mIndexMetrics->IncAccessCounter(field);
        }
        return mIndexReaders[it->second];
    }
    return RET_EMPTY_PTR;
}

const std::shared_ptr<InvertedIndexReader>& MultiFieldIndexReader::GetIndexReaderWithIndexId(indexid_t indexId) const
{
    auto it = mIndexId2PosMap.find(indexId);
    if (it != mIndexId2PosMap.end()) {
        return mIndexReaders[it->second];
    }
    return RET_EMPTY_PTR;
}

void MultiFieldIndexReader::SetLegacyAccessoryReader(const std::shared_ptr<LegacyIndexAccessoryReader>& accessoryReader)
{
    mAccessoryReader = accessoryReader;
    for (size_t i = 0; i < mIndexReaders.size(); ++i) {
        const auto& legacyReader = std::dynamic_pointer_cast<LegacyIndexReaderInterface>(mIndexReaders[i]);
        assert(legacyReader);
        legacyReader->SetLegacyAccessoryReader(accessoryReader);
    }
}

string MultiFieldIndexReader::GetIndexName() const
{
    INDEXLIB_THROW(util::UnImplementException, "GetIndexName is not supported in MultifieldIndexReader");
    return "";
}

std::shared_ptr<KeyIterator> MultiFieldIndexReader::CreateKeyIterator(const string& indexName)
{
    const std::shared_ptr<InvertedIndexReader>& indexReader = GetInvertedIndexReader(indexName);
    if (indexReader) {
        return indexReader->CreateKeyIterator(indexName);
    }
    return std::shared_ptr<KeyIterator>();
}

bool MultiFieldIndexReader::GenFieldMapMask(const std::string& indexName,
                                            const std::vector<std::string>& termFieldNames, fieldmap_t& targetFieldMap)
{
    const std::shared_ptr<InvertedIndexReader>& reader = GetInvertedIndexReader(indexName);
    if (reader != NULL) {
        return reader->GenFieldMapMask(indexName, termFieldNames, targetFieldMap);
    }
    return false;
}
}}} // namespace indexlib::index::legacy
