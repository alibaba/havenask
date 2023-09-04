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

#include "autil/NoCopyable.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"

namespace indexlibv2::index::ann {

struct EmbeddingDataHeader {
    int64_t indexId {kDefaultIndexId};
    uint64_t count {0ul};
};

class EmbeddingDataIterator : public aitheta2::IndexHolder::Iterator
{
public:
    EmbeddingDataIterator(const std::unordered_map<docid_t, embedding_t>::const_iterator& iter,
                          const std::unordered_map<docid_t, embedding_t>::const_iterator& end,
                          const std::vector<docid_t>* old2NewDocId = nullptr)
        : _iter(iter)
        , _end(end)
        , _old2NewDocId(old2NewDocId)
    {
    }
    ~EmbeddingDataIterator() = default;

public:
    const void* data(void) const override { return _iter->second.get(); }
    bool is_valid(void) const override { return _iter != _end; }
    uint64_t key(void) const override
    {
        return (nullptr != _old2NewDocId ? _old2NewDocId->at(_iter->first) : (uint64_t)_iter->first);
    }
    void next(void) override { ++_iter; }

private:
    std::unordered_map<docid_t, embedding_t>::const_iterator _iter;
    std::unordered_map<docid_t, embedding_t>::const_iterator _end;
    const std::vector<docid_t>* _old2NewDocId = nullptr;
};

class EmbeddingData : public aitheta2::IndexHolder
{
public:
    EmbeddingData(index_id_t indexId, const aitheta2::IndexMeta& meta)
        : _indexId(indexId)
        , _meta(meta)
        , _old2NewDocId(nullptr)
    {
    }
    ~EmbeddingData() = default;

public:
    size_t count(void) const override { return _dataMap.size(); }
    size_t dimension(void) const override { return _meta.dimension(); }
    aitheta2::IndexMeta::FeatureTypes type(void) const override { return _meta.type(); }
    size_t element_size(void) const override { return _meta.element_size(); }
    bool multipass(void) const override { return true; }
    aitheta2::IndexHolder::Iterator::Pointer create_iterator(void) override
    {
        return std::make_unique<EmbeddingDataIterator>(_dataMap.cbegin(), _dataMap.cend(), _old2NewDocId);
    }

public:
    index_id_t get_index_id() const { return _indexId; }
    bool add(embedding_t embedding, docid_t docId) { return _dataMap.emplace(docId, embedding).second; }
    bool remove(docid_t docId)
    {
        auto iterator = _dataMap.find(docId);
        if (iterator == _dataMap.end()) {
            return false;
        }
        _dataMap.erase(iterator);
        return true;
    }

    void merge(const EmbeddingData& other) { _dataMap.insert(other.get_data().begin(), other.get_data().end()); }
    const std::unordered_map<docid_t, embedding_t>& get_data() const { return _dataMap; }
    void clear() { _dataMap.clear(); }

    bool is_matched(const aitheta2::IndexMeta& meta) const
    {
        return (this->type() == meta.type() && this->dimension() == meta.dimension() &&
                this->element_size() == meta.element_size());
    }

    void set_doc_id_mapper(const std::vector<docid_t>* old2NewDocId) { _old2NewDocId = old2NewDocId; }

private:
    index_id_t _indexId;
    aitheta2::IndexMeta _meta;
    std::unordered_map<docid_t, embedding_t> _dataMap;
    const std::vector<docid_t>* _old2NewDocId = nullptr;
};

typedef std::shared_ptr<EmbeddingData> EmbeddingDataPtr;
class EmbeddingDataHolder : public autil::NoCopyable
{
public:
    EmbeddingDataHolder(const aitheta2::IndexMeta& meta) : _meta(meta) {}
    ~EmbeddingDataHolder() = default;

public:
    bool Add(embedding_t embedding, docid_t docId, index_id_t indexId);
    bool Add(embedding_t embedding, docid_t docId, const std::vector<index_id_t>& indexIds);
    void Merge(const EmbeddingDataHolder& other);
    bool Remove(docid_t docId, const std::vector<index_id_t>& indexIds);
    void Clear() { _embDataMap.clear(); }
    const std::map<index_id_t, EmbeddingDataPtr>& GetEmbeddingDataMap() const { return _embDataMap; }
    void SetDocIdMapper(const std::vector<docid_t>* old2NewDocId);

private:
    aitheta2::IndexMeta _meta;
    std::map<index_id_t, EmbeddingDataPtr> _embDataMap;
};

typedef std::shared_ptr<EmbeddingDataHolder> EmbeddingDataHolderPtr;

} // namespace indexlibv2::index::ann
