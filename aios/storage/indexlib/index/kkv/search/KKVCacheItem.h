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

#include <memory>

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/common/KKVDocs.h"

namespace indexlibv2::index {

template <typename SKeyType>
struct CachedSKeyNode {
    SKeyType skey;
    // TODO(xinfei.sxf) remove timestamp
    uint32_t timestamp;
    uint32_t expireTime;
    uint32_t valueOffset;

    CachedSKeyNode()
        : skey(SKeyType())
        , timestamp(INVALID_TIMESTAMP)
        , expireTime(indexlib::UNINITIALIZED_EXPIRE_TIME)
        , valueOffset(0)
    {
    }

} __attribute__((packed));

template <typename SKeyType>
struct KKVCacheItem {
    segmentid_t nextRtSegmentId;
    // TODO(xinfei.sxf) remove timestamp
    uint32_t timestamp;
    framework::Locator locator;
    uint32_t count;
    char* base;

    KKVCacheItem() : nextRtSegmentId(INVALID_SEGMENTID), timestamp(0), count(0), base(nullptr) {}

    CachedSKeyNode<SKeyType>* GetSKeyNodes() const { return (CachedSKeyNode<SKeyType>*)base; }

    char* GetValues() const { return (char*)(GetSKeyNodes() + count); }

    size_t Size() const
    {
        if (count == 0) {
            return sizeof(KKVCacheItem<SKeyType>);
        }
        size_t skeysSize = sizeof(CachedSKeyNode<SKeyType>) * count;
        CachedSKeyNode<SKeyType>* skeys = (CachedSKeyNode<SKeyType>*)base;
        return sizeof(KKVCacheItem<SKeyType>) + skeysSize + skeys[count - 1].valueOffset;
    }

    bool IsCacheItemValid() const
    {
        // TODO(xinfei.sxf) add judge
        return true;
    }

    static KKVCacheItem<SKeyType>* Create(typename KKVDocs::iterator beginIter, typename KKVDocs::iterator endIter)
    {
        KKVCacheItem<SKeyType>* cacheItem = new KKVCacheItem<SKeyType>();
        size_t bufferLen = 0;
        size_t skeyCount = 0;
        for (auto iter = beginIter; iter != endIter; ++iter) {
            auto& doc = *iter;
            if (!doc.skeyDeleted) {
                bufferLen += doc.value.size();
                ++skeyCount;
            }
        }
        size_t skeysSize = sizeof(CachedSKeyNode<SKeyType>) * skeyCount;
        bufferLen += skeysSize;
        cacheItem->base = new char[bufferLen];
        cacheItem->count = skeyCount;
        CachedSKeyNode<SKeyType>* skeys = (CachedSKeyNode<SKeyType>*)(cacheItem->base);
        char* valueBase = cacheItem->base + skeysSize;
        uint32_t cursor = 0;
        size_t index = 0;
        for (auto iter = beginIter; iter != endIter; ++iter) {
            auto& doc = *iter;
            if (doc.skeyDeleted) {
                continue;
            }
            skeys[index].skey = doc.skey;
            skeys[index].timestamp = doc.timestamp;
            skeys[index].expireTime = doc.expireTime;
            memcpy(valueBase + cursor, doc.value.data(), doc.value.length());
            cursor += doc.value.length();
            skeys[index].valueOffset = cursor;
            ++index;
        }
        assert(index == skeyCount);
        assert(cursor == bufferLen - skeysSize);
        return cacheItem;
    }

    static void Deleter(const autil::StringView& key, void* value, const autil::CacheAllocatorPtr& allocator)
    {
        if (value) {
            KKVCacheItem<SKeyType>* cacheItem = (KKVCacheItem<SKeyType>*)value;
            if (cacheItem->base) {
                delete[](char*) cacheItem->base;
                cacheItem->base = nullptr;
            }
            delete cacheItem;
        }
    }
};

} // namespace indexlibv2::index
