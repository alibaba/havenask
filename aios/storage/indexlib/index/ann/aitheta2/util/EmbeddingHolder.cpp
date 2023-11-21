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
#include "indexlib/index/ann/aitheta2/util/EmbeddingHolder.h"

namespace indexlibv2::index::ann {
bool EmbeddingHolder::Add(const char* embedding, docid_t docId, index_id_t indexId)
{
    auto iter = _bufferMap.find(indexId);
    if (iter == _bufferMap.end()) {
        if (HasMultiIndexId()) {
            // 多类目的场景不使用大页，否则会造成内存浪费
            iter = _bufferMap.emplace(indexId, std::make_shared<EmbeddingBuffer<>>(indexId, _meta)).first;
        } else {
            using T = ailego::HugePageAlignedAllocator<char>;
            iter = _bufferMap.emplace(indexId, std::make_shared<EmbeddingBuffer<T>>(indexId, _meta)).first;
        }
    }
    auto buffer = iter->second;
    return buffer->Add(embedding, docId);
}

bool EmbeddingHolder::Add(const char* embedding, docid_t docId, const std::vector<index_id_t>& indexIds)
{
    for (index_id_t indexId : indexIds) {
        ANN_CHECK(Add(embedding, docId, indexId), "add failed");
    }
    return true;
}

bool EmbeddingHolder::Steal(EmbeddingHolder& rhs)
{
    if (HasMultiIndexId() != rhs.HasMultiIndexId()) {
        AUTIL_LOG(ERROR, "not consistency, steal failed");
        return false;
    }

    for (auto& [indexId, rhsBuffer] : rhs.GetMutableBufferMap()) {
        auto iter = _bufferMap.find(indexId);
        if (iter == _bufferMap.end()) {
            _bufferMap[indexId] = std::move(rhsBuffer);
            continue;
        } else {
            auto lhsBuffer = iter->second;
            ANN_CHECK(lhsBuffer->Steal(rhsBuffer.get()), "steal failed");
        }
    }
    rhs.Clear();
    return true;
}

bool EmbeddingHolder::Delete(docid_t docId, const std::vector<index_id_t>& indexIds)
{
    for (index_id_t indexId : indexIds) {
        auto iter = _bufferMap.find(indexId);
        if (iter == _bufferMap.end()) {
            continue;
        }
        auto buffer = iter->second;
        ANN_CHECK(buffer->Delete(docId), "delete failed");
    }
    return true;
}

const std::map<index_id_t, std::shared_ptr<EmbeddingBufferBase>>& EmbeddingHolder::GetBufferMap() const
{
    return _bufferMap;
}

std::map<index_id_t, std::shared_ptr<EmbeddingBufferBase>>& EmbeddingHolder::GetMutableBufferMap()
{
    return _bufferMap;
}

void EmbeddingHolder::SetDocIdMapper(const std::vector<docid_t>* old2NewDocId)
{
    for (auto& [_, buffer] : _bufferMap) {
        buffer->SetDocIdMapper(old2NewDocId);
    }
}

const aitheta2::IndexMeta& EmbeddingHolder::GetMeta() const { return _meta; }

bool EmbeddingHolder::HasMultiIndexId() const { return _hasMultiIndexId; }

void EmbeddingHolder::Clear() { _bufferMap.clear(); }

AUTIL_LOG_SETUP(indexlib.index, EmbeddingHolder);

} // namespace indexlibv2::index::ann