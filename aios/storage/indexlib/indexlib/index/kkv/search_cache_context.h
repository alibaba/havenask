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
#ifndef __INDEXLIB_SEARCH_CACHE_CONTEXT_H
#define __INDEXLIB_SEARCH_CACHE_CONTEXT_H

#include <memory>

#include "autil/MurmurHash.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_cache_item.h"
#include "indexlib/index/kkv/kkv_index_options.h"
#include "indexlib/index/kkv/skey_search_context.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

namespace indexlib { namespace index {

class SearchCacheContext
{
public:
    inline SearchCacheContext(const util::SearchCachePartitionWrapperPtr& searchCache, bool onlyCache)
        : mSearchCache(searchCache)
        , mCacheKey(0)
        , mOnlyCache(onlyCache)
    {
    }

    ~SearchCacheContext() {}

public:
    template <typename SKeyType>
    inline void Init(PKeyType pkey, KKVIndexOptions* indexOptions, SKeySearchContext<SKeyType>* skeyContext,
                     KVMetricsCollector* metricCollector, autil::mem_pool::Pool* pool);

    bool IsOnlyCache() const { return mOnlyCache; }
    uint64_t GetCacheKey() const { return mCacheKey; }
    const util::SearchCachePartitionWrapperPtr& GetSearchCache() const { return mSearchCache; }
    bool HasCacheItem() const noexcept { return mCacheItemGuard.operator bool(); }

    template <typename SKeyType>
    inline KKVCacheItem<SKeyType>* GetCacheItem() const
    {
        if (mCacheItemGuard) {
            return mCacheItemGuard->GetCacheItem<KKVCacheItem<SKeyType>>();
        }
        return NULL;
    }

    template <typename SKeyType>
    void PutCacheItem(regionid_t regionId, KKVCacheItem<SKeyType>* item, autil::CacheBase::Priority priority)
    {
        mSearchCache->Put(mCacheKey, regionId, item, priority);
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
    const util::SearchCachePartitionWrapperPtr& mSearchCache;
    std::unique_ptr<util::CacheItemGuard> mCacheItemGuard;
    uint64_t mCacheKey;
    bool mOnlyCache;

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
template <typename SKeyType>
inline void SearchCacheContext::Init(PKeyType pkey, KKVIndexOptions* indexOptions,
                                     SKeySearchContext<SKeyType>* skeyContext, KVMetricsCollector* metricsCollector,
                                     autil::mem_pool::Pool* pool)
{
    assert(indexOptions);
    auto* searchCacheCounter = metricsCollector ? metricsCollector->GetSearchCacheCounter() : nullptr;
    mCacheKey = GetCacheKey(pkey, skeyContext, pool);
    mCacheItemGuard = mSearchCache->Get(mCacheKey, indexOptions->GetRegionId(), searchCacheCounter);
    KKVCacheItem<SKeyType>* kkvCacheItem = NULL;
    if (mCacheItemGuard && (kkvCacheItem = mCacheItemGuard->GetCacheItem<KKVCacheItem<SKeyType>>()) &&
        kkvCacheItem->IsCacheItemValid(indexOptions->oldestRtSegmentId, indexOptions->lastSkipIncTsInSecond)) {
        // do nothing, remain valid cache item
    } else {
        mCacheItemGuard.reset();
    }
}
}} // namespace indexlib::index

#endif //__INDEXLIB_SEARCH_CACHE_CONTEXT_H
