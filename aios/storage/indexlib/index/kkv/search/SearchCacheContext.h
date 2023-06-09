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

#include "autil/MurmurHash.h"
#include "autil/cache/cache.h"
#include "indexlib/index/kkv/common/KKVMetricsCollector.h"
#include "indexlib/index/kkv/common/SKeySearchContext.h"
#include "indexlib/index/kkv/search/KKVCacheItem.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

namespace indexlibv2::index {
// TODO(xinfei.sxf) move to kkv/common dir
class SearchCacheContext
{
public:
    inline SearchCacheContext(const indexlib::util::SearchCachePartitionWrapperPtr& searchCache, bool onlyCache,
                              autil::CacheBase::Priority cachePriority)
        : mSearchCache(searchCache)
        , mCacheKey(0)
        , mOnlyCache(onlyCache)
        , mCachePriority(cachePriority)
    {
    }

    ~SearchCacheContext() {}

public:
    template <typename SKeyType>
    inline void Init(PKeyType pkey, SKeySearchContext<SKeyType>* skeyContext, KVMetricsCollector* metricCollector,
                     autil::mem_pool::Pool* pool);

    uint64_t GetCacheKey() const { return mCacheKey; }
    bool IsOnlyCache() const { return mOnlyCache; }
    autil::CacheBase::Priority GetPriority() { return mCachePriority; }
    const indexlib::util::SearchCachePartitionWrapperPtr& GetSearchCache() const { return mSearchCache; }
    bool HasCacheItem() const noexcept { return mCacheItemGuard.operator bool(); }

    template <typename SKeyType>
    inline KKVCacheItem<SKeyType>* GetCacheItem() const
    {
        if (mCacheItemGuard) {
            return mCacheItemGuard->GetCacheItem<KKVCacheItem<SKeyType>>();
        }
        return nullptr;
    }

    template <typename SKeyType>
    void PutCacheItem(KKVCacheItem<SKeyType>* item, autil::CacheBase::Priority priority)
    {
        mSearchCache->Put(mCacheKey, 0, item, priority);
    }

private:
    template <typename SKeyType>
    static uint64_t GetCacheKey(const PKeyType& pkey, SKeySearchContext<SKeyType>* skeyContext,
                                autil::mem_pool::Pool* pool)
    {
        if (!skeyContext || skeyContext->GetSortedRequiredSKeys().empty()) {
            return autil::MurmurHash::MurmurHash64A(&pkey, sizeof(pkey), 0);
        }
        auto& sortedRequiredSkeys = skeyContext->GetSortedRequiredSKeys();
        // FIXME: skey set size too large; extra skey space
        size_t len = sizeof(pkey) + sortedRequiredSkeys.size() * sizeof(SKeyType);
        PKeyType* pkeyAddr = static_cast<PKeyType*>(pool->allocate(len));
        *pkeyAddr = pkey;
        SKeyType* skeyAddr = reinterpret_cast<SKeyType*>(pkeyAddr + 1);
        std::copy(sortedRequiredSkeys.cbegin(), sortedRequiredSkeys.cend(), skeyAddr);
        return autil::MurmurHash::MurmurHash64A((void*)pkeyAddr, len, 0);
    }

private:
    const indexlib::util::SearchCachePartitionWrapperPtr& mSearchCache;
    std::unique_ptr<indexlib::util::CacheItemGuard> mCacheItemGuard;
    uint64_t mCacheKey;
    bool mOnlyCache;
    autil::CacheBase::Priority mCachePriority;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////

template <typename SKeyType>
inline void SearchCacheContext::Init(PKeyType pkey, SKeySearchContext<SKeyType>* skeyContext,
                                     KVMetricsCollector* metricsCollector, autil::mem_pool::Pool* pool)
{
    auto* searchCacheCounter = metricsCollector ? metricsCollector->GetSearchCacheCounter() : nullptr;
    mCacheKey = GetCacheKey(pkey, skeyContext, pool);
    mCacheItemGuard = mSearchCache->Get(mCacheKey, 0, searchCacheCounter);
    KKVCacheItem<SKeyType>* kkvCacheItem = nullptr;
    if (mCacheItemGuard && (kkvCacheItem = mCacheItemGuard->GetCacheItem<KKVCacheItem<SKeyType>>()) &&
        kkvCacheItem->IsCacheItemValid()) {
        // do nothing, remain valid cache item
    } else {
        mCacheItemGuard.reset();
    }
}

} // namespace indexlibv2::index
