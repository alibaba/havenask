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
#include "indexlib/index/inverted_index/BuildingIndexReader.h"

namespace indexlib::index {

void BuildingIndexReader::AddSegmentReader(docid64_t baseDocId,
                                           const std::shared_ptr<IndexSegmentReader>& inMemSegReader)
{
    if (inMemSegReader) {
        _innerSegReaders.push_back(std::make_pair(baseDocId, inMemSegReader));
    }
}

void BuildingIndexReader::GetSegmentPosting(const index::DictKeyInfo& key, std::vector<SegmentPosting>& segPostings,
                                            autil::mem_pool::Pool* sessionPool, InvertedIndexSearchTracer* tracer) const
{
    file_system::ReadOption option;
    for (size_t i = 0; i < _innerSegReaders.size(); ++i) {
        SegmentPosting segPosting(_postingFormatOption);
        if (_innerSegReaders[i].second->GetSegmentPosting(key, _innerSegReaders[i].first, segPosting, sessionPool,
                                                          option, tracer)) {
            segPostings.push_back(std::move(segPosting));
        }
    }
}

size_t BuildingIndexReader::GetSegmentCount() const { return _innerSegReaders.size(); }

std::vector<std::shared_ptr<DictionaryReader>> BuildingIndexReader::GetDictionaryReader()
{
    std::vector<std::shared_ptr<DictionaryReader>> readers;
    for (size_t i = 0; i < _innerSegReaders.size(); ++i) {
        readers.push_back(_innerSegReaders[i].second->GetDictionaryReader());
    }
    return readers;
}

void BuildingIndexReader::Update(docid_t docId, const document::ModifiedTokens& tokens)
{
    auto segInfo = GetSegmentReader(docId);
    if (segInfo.first != INVALID_DOCID) {
        segInfo.second->Update(docId - segInfo.first, tokens);
    }
}

void BuildingIndexReader::Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete)
{
    auto segInfo = GetSegmentReader(docId);
    if (segInfo.first != INVALID_DOCID) {
        segInfo.second->Update(docId - segInfo.first, key, isDelete);
    }
}

std::pair<docid64_t, IndexSegmentReader*> BuildingIndexReader::GetSegmentReader(docid64_t docId)
{
    IndexSegmentReader* segReader = nullptr;
    docid64_t baseDocId = INVALID_DOCID;
    for (const auto& segPair : _innerSegReaders) {
        docid64_t curBaseDocId = segPair.first;
        if (docId < curBaseDocId) {
            return {baseDocId, segReader};
        }
        baseDocId = segPair.first;
        segReader = segPair.second.get();
    }
    return {baseDocId, segReader};
}
} // namespace indexlib::index
