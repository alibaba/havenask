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

#include <ailego/memory_pool/huge_page_arena_pool.h>

#include "autil/NoCopyable.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingIterator.h"

namespace indexlibv2::index::ann {

struct EmbeddingFileHeader {
    int64_t indexId = kDefaultIndexId;
    uint64_t count = 0ul;
};

class EmbeddingBufferBase : public aitheta2::IndexHolder, autil::NoCopyable
{
public:
    EmbeddingBufferBase(int64_t indexId, const aitheta2::IndexMeta& meta)
        : _indexId(indexId)
        , _meta(meta)
        , _old2NewDocId(nullptr)
        , _isMultiPass(true)
        , _nodeSize(sizeof(docid_t) + _meta.element_size())
    {
    }
    virtual ~EmbeddingBufferBase() {}

public:
    virtual bool Add(const char* embedding, docid_t docId) = 0;
    virtual bool Delete(docid_t docId) = 0;
    virtual bool Steal(EmbeddingBufferBase* buffer) = 0;
    virtual void Clear() = 0;

public:
    void SetDocIdMapper(const std::vector<docid_t>* old2NewDocId) { _old2NewDocId = old2NewDocId; }
    // 是否可以多次遍历数据。 若为false，当遍历迭代器时，会依次删除遍历过的元素
    void SetMultiPass(bool flag) { _isMultiPass = flag; }
    index_id_t GetIndexId() const { return _indexId; }

protected:
    index_id_t _indexId;
    aitheta2::IndexMeta _meta;
    const std::vector<docid_t>* _old2NewDocId = nullptr;
    bool _isMultiPass = true;
    size_t _nodeSize = 0;
    bool _hasDelDocId {false};
    std::unordered_set<docid_t> _docIds;
    size_t _maxBufferSize {kMaxBufferSize};
    // 之所以减64,是因为调用string reserve时，实际allocate的大小略大。
    // 当使用大页分配时，会向上取整，导致内存浪费
    static constexpr size_t kMaxBufferSize = 8ul * 1024ul * 1024ul - 64ul; // 8MB - 64B
};

template <typename T = std::allocator<char>>
class EmbeddingBuffer : public EmbeddingBufferBase
{
    using Buffer = std::basic_string<char, std::char_traits<char>, T>;

public:
    EmbeddingBuffer(int64_t indexId, const aitheta2::IndexMeta& meta) : EmbeddingBufferBase(indexId, meta) {}
    ~EmbeddingBuffer() { Clear(); }

public:
    size_t count(void) const override { return _docIds.size(); }
    size_t dimension(void) const override { return _meta.dimension(); }
    aitheta2::IndexMeta::FeatureTypes type(void) const override { return _meta.type(); }
    size_t element_size(void) const override { return _meta.element_size(); }
    bool multipass(void) const override { return _isMultiPass; }
    aitheta2::IndexHolder::Iterator::Pointer create_iterator(void) override
    {
        Shrink(/*force*/ true);
        AUTIL_LOG(INFO, "create iterator with buffer count[%lu]", _buffers.size());
        return std::make_unique<EmbeddingIterator<T>>(_meta, _buffers, _isMultiPass, _old2NewDocId);
    }

public:
    bool Add(const char* embedding, docid_t docId) override;
    bool Delete(docid_t docId) override;
    bool Steal(EmbeddingBufferBase* buffer) override;
    void Clear() override;

private:
    void Shrink(bool force);
    size_t GetBufferSize() const;
    size_t GetUsedBytes() const { return _usedBytes; }

private:
    std::list<Buffer> _buffers;
    size_t _usedBytes = 0;
    AUTIL_LOG_DECLARE();
};

template <typename T>
bool EmbeddingBuffer<T>::Add(const char* embedding, docid_t docId)
{
    if (!_docIds.insert(docId).second) {
        return true;
    }

    if (_buffers.empty() || (_buffers.back().size() + _nodeSize) > _buffers.back().capacity()) {
        size_t bufferSize = GetBufferSize();
        _buffers.push_back(Buffer());
        _buffers.back().reserve(bufferSize);
    }
    _usedBytes += _nodeSize;
    _buffers.back().append(reinterpret_cast<const char*>(&docId), sizeof(docId));
    _buffers.back().append(embedding, element_size());
    return true;
}

template <typename T>
bool EmbeddingBuffer<T>::Delete(docid_t docId)
{
    auto iter = _docIds.find(docId);
    if (iter == _docIds.end()) {
        return false;
    }
    _docIds.erase(iter);
    _hasDelDocId = true;

    Shrink(/*force*/ false);
    return true;
}

template <typename T>
bool EmbeddingBuffer<T>::Steal(EmbeddingBufferBase* buffer)
{
    auto rhs = dynamic_cast<EmbeddingBuffer<T>*>(buffer);
    if (rhs == nullptr) {
        AUTIL_LOG(ERROR, "cast to EmbeddingBuffer failed");
        return false;
    }

    if (!is_matched(rhs->_meta) || GetIndexId() != rhs->GetIndexId()) {
        AUTIL_LOG(ERROR, "meta or index id mismatch, steal failed");
        return false;
    }

    for (auto& buffer : rhs->_buffers) {
        _buffers.push_back(std::move(buffer));
    }
    for (docid_t docId : rhs->_docIds) {
        if (!_docIds.insert(docId).second) {
            return false;
        }
    }
    _usedBytes += rhs->GetUsedBytes();

    rhs->Clear();
    return true;
}

template <typename T>
void EmbeddingBuffer<T>::Clear()
{
    _docIds.clear();
    _buffers.clear();
}

template <typename T>
void EmbeddingBuffer<T>::Shrink(bool force)
{
    if (!_hasDelDocId) {
        return;
    }

    if (!force) {
        size_t expectedTotalSize = _docIds.size() * _nodeSize;
        float threshold = 0.5f;
        float wasteRatio = 1.0 - (1.0 * expectedTotalSize / GetUsedBytes());
        if (wasteRatio < threshold) {
            return;
        }
        AUTIL_LOG(INFO, "waste ratio[%f] exceed[%f]", wasteRatio, threshold);
    }
    AUTIL_LOG(INFO, "begin shrink buffer with doc count[%lu]", _docIds.size());

    std::list<Buffer> buffers;
    std::unordered_set<docid_t> docIds;
    buffers.swap(_buffers);
    docIds.swap(_docIds);
    _usedBytes = 0;

    for (auto& buffer : buffers) {
        for (size_t localOff = 0; localOff < buffer.size(); localOff += _nodeSize) {
            const char* node = buffer.data() + localOff;
            docid_t docId = *reinterpret_cast<const docid_t*>(node);
            if (docIds.find(docId) != docIds.end()) {
                const char* embedding = node + sizeof(docid_t);
                Add(embedding, docId);
            }
        }
        buffer.clear();
        buffer.shrink_to_fit();
    }

    AUTIL_LOG(INFO, "end shrink buffer");
    _hasDelDocId = false;
}

template <>
inline size_t EmbeddingBuffer<>::GetBufferSize() const
{
    // 适用多类目场景,动态内存分配
    if (_buffers.empty()) {
        return _nodeSize;
    }

    return std::min(_maxBufferSize, _buffers.back().size() * 2);
}

template <>
inline size_t EmbeddingBuffer<ailego::HugePageAlignedAllocator<char>>::GetBufferSize() const
{
    // 大页的主要好处在于使用mmap/mumap管理内存，释放的内存能够立即归还操作系统
    return _maxBufferSize;
}

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, EmbeddingBuffer, T);

} // namespace indexlibv2::index::ann
