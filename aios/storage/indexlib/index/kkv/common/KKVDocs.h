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

#include <deque>

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/index/kkv/common/KKVDoc.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlibv2::index {
class KKVDocs
{
private:
    using Queue = std::deque<KKVDoc, autil::mem_pool::pool_allocator<KKVDoc>>;

public:
    using iterator = typename Queue::iterator;

public:
    KKVDocs(autil::mem_pool::Pool* pool)
    {
        assert(pool);
        autil::mem_pool::pool_allocator<KKVDoc> alloc(pool);
        auto docs = IE_POOL_COMPATIBLE_NEW_CLASS(pool, Queue, alloc);
        _docs.reset(docs, pool);
    }

    KKVDoc& operator[](size_t idx) { return (*_docs)[idx]; }
    const KKVDoc& operator[](size_t idx) const { return (*_docs)[idx]; }

    size_t size() const noexcept { return _docs->size(); }

    void push_back(const KKVDoc& doc) { return _docs->push_back(doc); }
    void push_back(KKVDoc&& doc) { return _docs->push_back(std::move(doc)); }
    iterator begin() { return _docs->begin(); }
    iterator end() { return _docs->end(); }
    void emplace_back(KKVDoc&& doc) { _docs->emplace_back(std::move(doc)); }
    void swap(KKVDocs& docs) { _docs = std::move(docs._docs); }
    KKVDoc& back() { return _docs->back(); }
    const KKVDoc& back() const { return _docs->back(); }
    void clear() { _docs->clear(); }

public:
    KKVDocs(const KKVDocs&) = delete;
    KKVDocs& operator=(const KKVDocs&) = delete;

    KKVDocs(KKVDocs&& other) : _docs(std::move(other._docs)) {}
    KKVDocs& operator=(KKVDocs&& other)
    {
        if (this != &other) {
            _docs = std::move(other._docs);
        }
        return *this;
    }

private:
    indexlib::util::PooledUniquePtr<Queue> _docs;
};

} // namespace indexlibv2::index
