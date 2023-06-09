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
#ifndef __INDEXLIB_KV_CACHE_ITEM_H
#define __INDEXLIB_KV_CACHE_ITEM_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace autil {
class CacheAllocator;
using CacheAllocatorPtr = std::shared_ptr<CacheAllocator>;
} // namespace autil

namespace indexlib { namespace index {

struct KVCacheItem {
    segmentid_t nextRtSegmentId;
    uint32_t timestamp;
    autil::StringView value;

    KVCacheItem() : nextRtSegmentId(INVALID_SEGMENTID), timestamp(0) {}

    size_t Size() const { return sizeof(KVCacheItem) + value.size(); }

    bool HasValue() const { return !value.empty(); }

    void SetHasValue(bool hasValue)
    {
        if (!hasValue) {
            value = autil::StringView::empty_instance();
        }
    }

    bool IsCacheItemValid(segmentid_t oldestRtSegmentId, int64_t lastSkipIncTsInSecond) const
    {
        return nextRtSegmentId >= oldestRtSegmentId && (int64_t)timestamp >= lastSkipIncTsInSecond;
    }

    static KVCacheItem* Create(segmentid_t nextRtSegmentId, uint32_t timestamp, const autil::StringView& value)
    {
        KVCacheItem* ret = new KVCacheItem();
        ret->nextRtSegmentId = nextRtSegmentId;
        ret->timestamp = timestamp;
        if (value.size() > 0) {
            char* newValue = new char[value.size()];
            memcpy(newValue, value.data(), value.size());
            ret->value = autil::StringView(newValue, value.size());
        }
        return ret;
    }

    static void Deleter(const autil::StringView& key, void* value, const autil::CacheAllocatorPtr& allocator)
    {
        if (value) {
            KVCacheItem* cacheItem = (KVCacheItem*)value;
            if (cacheItem->value.size() > 0) {
                delete[](char*) cacheItem->value.data();
                cacheItem->value = autil::StringView::empty_instance();
            }
            delete cacheItem;
        }
    }
};
}} // namespace indexlib::index

#endif //__INDEXLIB_KV_CACHE_ITEM_H
