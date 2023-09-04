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
#include "indexlib/index/ann/aitheta2/util/EmbeddingDataDumper.h"

namespace indexlibv2::index::ann {

bool EmbeddingDataHolder::Add(embedding_t embedding, docid_t docId, index_id_t indexId)
{
    auto& embData = _embDataMap[indexId];
    if (!embData) {
        embData = std::make_shared<EmbeddingData>(indexId, _meta);
    }
    return embData->add(embedding, docId);
}

bool EmbeddingDataHolder::Add(embedding_t embedding, docid_t docId, const std::vector<index_id_t>& indexIds)
{
    for (index_id_t indexId : indexIds) {
        if (!Add(embedding, docId, indexId)) {
            return false;
        }
    }
    return true;
}

void EmbeddingDataHolder::Merge(const EmbeddingDataHolder& other)
{
    for (auto& [indexId, otherEmbData] : other.GetEmbeddingDataMap()) {
        auto& embData = _embDataMap[indexId];
        if (!embData) {
            embData = std::make_shared<EmbeddingData>(indexId, _meta);
        }
        embData->merge(*otherEmbData);
    }
}

bool EmbeddingDataHolder::Remove(docid_t docId, const std::vector<index_id_t>& indexIds)
{
    for (index_id_t indexId : indexIds) {
        auto iterator = _embDataMap.find(indexId);
        if (iterator == _embDataMap.end()) {
            return false;
        }
        if (!iterator->second->remove(docId)) {
            return false;
        }
    }
    return true;
}

void EmbeddingDataHolder::SetDocIdMapper(const std::vector<docid_t>* old2NewDocId)
{
    for (const auto& [indexId, embeddingDataPtr] : _embDataMap) {
        embeddingDataPtr->set_doc_id_mapper(old2NewDocId);
    }
}

} // namespace indexlibv2::index::ann