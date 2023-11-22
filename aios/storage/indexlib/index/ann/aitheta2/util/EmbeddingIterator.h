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

template <typename T = std::allocator<char>>
class EmbeddingIterator : public aitheta2::IndexHolder::Iterator
{
    using Buffer = std::basic_string<char, std::char_traits<char>, T>;

public:
    EmbeddingIterator(const aitheta2::IndexMeta& meta, std::list<Buffer>& buffers, bool isMultiPass,
                      const std::vector<docid_t>* old2NewDocId)
        : _meta(meta)
        , _buffers(buffers)
        , _iterator(_buffers.begin())
        , _isMultiPass(isMultiPass)
        , _old2NewDocId(old2NewDocId)
    {
    }
    ~EmbeddingIterator() {}

public:
    const void* data(void) const override
    {
        const char* node = GetNode();
        return reinterpret_cast<const void*>(node + sizeof(docid_t));
    }
    bool is_valid(void) const override { return _iterator != _buffers.end(); }
    uint64_t key(void) const override
    {
        docid_t docId = *reinterpret_cast<const docid_t*>(GetNode());
        return (nullptr != _old2NewDocId ? _old2NewDocId->at(docId) : (uint64_t)docId);
    }
    void next(void) override
    {
        _localOffset += sizeof(docid_t) + _meta.element_size();
        if (_localOffset < _iterator->size()) {
            return;
        }
        auto iter = _iterator++;
        if (!_isMultiPass) {
            _buffers.erase(iter);
        }
        _localOffset = 0;
    }

private:
    const char* GetNode() const { return _iterator->data() + _localOffset; }

private:
    aitheta2::IndexMeta _meta;
    std::list<Buffer>& _buffers;
    typename std::list<Buffer>::iterator _iterator;
    bool _isMultiPass = true;
    int64_t _localOffset = 0;
    const std::vector<docid_t>* _old2NewDocId = nullptr;
};
} // namespace indexlibv2::index::ann
