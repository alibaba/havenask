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
#include "indexlib/index/inverted_index/MultiFieldIndexReader.h"

#include "indexlib/index/inverted_index/IndexAccessoryReader.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/util/counter/AccumulativeCounter.h"

namespace indexlib::index {
namespace {
}

AUTIL_LOG_SETUP(indexlib.index, MultiFieldIndexReader);

std::shared_ptr<InvertedIndexReader> MultiFieldIndexReader::RET_EMPTY_PTR = std::shared_ptr<InvertedIndexReader>();

MultiFieldIndexReader::MultiFieldIndexReader(
    const std::unordered_map<std::string, std::shared_ptr<indexlib::util::AccumulativeCounter>>* invertedAccessCounter)
    : _accessCounter(invertedAccessCounter)
{
}

index::Result<PostingIterator*> MultiFieldIndexReader::Lookup(const index::Term& term, uint32_t statePoolSize,
                                                              PostingType type, autil::mem_pool::Pool* sessionPool)
{
    return future_lite::coro::syncAwait(LookupAsync(&term, statePoolSize, type, sessionPool, nullptr));
}

future_lite::coro::Lazy<index::Result<PostingIterator*>>
MultiFieldIndexReader::LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type,
                                   autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept
{
    assert(sessionPool);
    if (!term) {
        co_return nullptr;
    }
    const std::shared_ptr<InvertedIndexReader>& reader = GetIndexReader(term->GetIndexName());
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
    assert(pool);
    const std::shared_ptr<InvertedIndexReader>& reader = GetIndexReader(term.GetIndexName());
    if (reader) {
        return reader->PartialLookup(term, ranges, statePoolSize, type, pool);
    }
    return nullptr;
}

const SectionAttributeReader* MultiFieldIndexReader::GetSectionReader(const std::string& indexName) const
{
    const std::shared_ptr<InvertedIndexReader>& reader = GetIndexReader(indexName);
    if (reader) {
        return reader->GetSectionReader(indexName);
    }
    return nullptr;
}

Status MultiFieldIndexReader::AddIndexReader(const indexlibv2::config::InvertedIndexConfig* indexConfig,
                                             const std::shared_ptr<InvertedIndexReader>& reader)
{
    const std::string& indexName = indexConfig->GetIndexName();
    indexid_t indexId = indexConfig->GetIndexId();

    if (_name2PosMap.find(indexName) != _name2PosMap.end()) {
        AUTIL_LOG(ERROR, "index reader for [%s] already exist!", indexName.c_str());
        return Status::InvalidArgs();
    }

    if (_indexId2PosMap.find(indexId) != _indexId2PosMap.end()) {
        AUTIL_LOG(ERROR, "index reader for [id:%d] already exist!", indexId);
        return Status::InvalidArgs();
    }

    _name2PosMap.insert(std::make_pair(indexName, _indexReaders.size()));
    _indexId2PosMap.insert(std::make_pair(indexId, _indexReaders.size()));
    _indexReaders.push_back(reader);

    reader->SetAccessoryReader(_accessoryReader);
    return Status::OK();
}

const std::shared_ptr<InvertedIndexReader>& MultiFieldIndexReader::GetIndexReader(const std::string& field) const
{
    auto it = _name2PosMap.find(field);
    if (it != _name2PosMap.end()) {
        IncAccessCounter(field);
        return _indexReaders[it->second];
    }
    return RET_EMPTY_PTR;
}

const std::shared_ptr<InvertedIndexReader>& MultiFieldIndexReader::GetIndexReaderWithIndexId(indexid_t indexId) const
{
    auto it = _indexId2PosMap.find(indexId);
    if (it != _indexId2PosMap.end()) {
        return _indexReaders[it->second];
    }
    return RET_EMPTY_PTR;
}

void MultiFieldIndexReader::SetAccessoryReader(const std::shared_ptr<IndexAccessoryReader>& accessoryReader)
{
    _accessoryReader = accessoryReader;
    for (size_t i = 0; i < _indexReaders.size(); ++i) {
        _indexReaders[i]->SetAccessoryReader(accessoryReader);
    }
}

std::shared_ptr<KeyIterator> MultiFieldIndexReader::CreateKeyIterator(const std::string& indexName)
{
    const std::shared_ptr<InvertedIndexReader>& indexReader = GetIndexReader(indexName);
    if (indexReader) {
        return indexReader->CreateKeyIterator(indexName);
    }
    return std::shared_ptr<KeyIterator>();
}

bool MultiFieldIndexReader::GenFieldMapMask(const std::string& indexName,
                                            const std::vector<std::string>& termFieldNames, fieldmap_t& targetFieldMap)
{
    const std::shared_ptr<InvertedIndexReader>& reader = GetIndexReader(indexName);
    if (reader != nullptr) {
        return reader->GenFieldMapMask(indexName, termFieldNames, targetFieldMap);
    }
    return false;
}

void MultiFieldIndexReader::IncAccessCounter(const std::string& indexName) const
{
    if (_accessCounter) {
        auto iter = _accessCounter->find(indexName);
        if (iter != _accessCounter->end() && iter->second.get() != nullptr) {
            iter->second->Increase(1);
        }
    }
}

} // namespace indexlib::index
