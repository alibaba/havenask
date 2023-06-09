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
#ifndef __INDEXLIB_KKV_DOCS_H
#define __INDEXLIB_KKV_DOCS_H

#include <deque>

#include "autil/ConstString.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/on_disk_skey_node.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlib { namespace index {

struct KKVDoc {
    union {
        autil::StringView value;      // 128
        OnDiskSKeyOffset valueOffset; // 48
        uint64_t offset;              // 8
    };
    uint64_t skey = 0;      // 8
    uint32_t timestamp = 0; // 4
    uint32_t expireTime;
    bool skeyDeleted = false;
    bool duplicatedKey = false;
    bool inCache = false;

    KKVDoc()
        : skey(0)
        , timestamp(0)
        , expireTime(UNINITIALIZED_EXPIRE_TIME)
        , skeyDeleted(false)
        , duplicatedKey(false)
        , inCache(false)
    {
    }

    KKVDoc(uint32_t ts, uint64_t skey, uint64_t offset = 0)
        : offset(offset)
        , skey(skey)
        , timestamp(ts)
        , expireTime(UNINITIALIZED_EXPIRE_TIME)
        , skeyDeleted(false)
        , duplicatedKey(false)
        , inCache(false)
    {
    }

    ~KKVDoc() {}

    KKVDoc(const KKVDoc& rhs)
        : value(rhs.value)
        , skey(rhs.skey)
        , timestamp(rhs.timestamp)
        , expireTime(rhs.expireTime)
        , skeyDeleted(rhs.skeyDeleted)
        , duplicatedKey(rhs.duplicatedKey)
        , inCache(rhs.inCache)
    {
    }

    KKVDoc& operator=(const KKVDoc& rhs)
    {
        if (this != &rhs) {
            value = rhs.value;
            skey = rhs.skey;
            timestamp = rhs.timestamp;
            expireTime = rhs.expireTime;
            skeyDeleted = rhs.skeyDeleted;
            duplicatedKey = rhs.duplicatedKey;
            inCache = rhs.inCache;
        }
        return *this;
    }
    void SetValueOffset(OnDiskSKeyOffset offset) { valueOffset = offset; }
    void SetValue(autil::StringView v) { value = v; }
};

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
        mDocs.reset(docs, pool);
    }

    KKVDoc& operator[](size_t idx) { return (*mDocs)[idx]; }
    const KKVDoc& operator[](size_t idx) const { return (*mDocs)[idx]; }

    size_t size() const noexcept { return mDocs->size(); }

    void push_back(const KKVDoc& doc) { return mDocs->push_back(doc); }
    void push_back(KKVDoc&& doc) { return mDocs->push_back(std::move(doc)); }
    iterator begin() { return mDocs->begin(); }
    iterator end() { return mDocs->end(); }
    void emplace_back(KKVDoc&& doc) { mDocs->emplace_back(std::move(doc)); }
    void swap(KKVDocs& docs) { mDocs = std::move(docs.mDocs); }
    KKVDoc& back() { return mDocs->back(); }
    const KKVDoc& back() const { return mDocs->back(); }
    void clear() { mDocs->clear(); }

public:
    KKVDocs(const KKVDocs&) = delete;
    KKVDocs& operator=(const KKVDocs&) = delete;

    KKVDocs(KKVDocs&& other) : mDocs(std::move(other.mDocs)) {}
    KKVDocs& operator=(KKVDocs&& other)
    {
        if (this != &other) {
            mDocs = std::move(other.mDocs);
        }
        return *this;
    }

private:
    util::PooledUniquePtr<Queue> mDocs;
};
}} // namespace indexlib::index

#endif
