#ifndef __INDEXLIB_SEARCH_CACHE_PARTITION_WRAPPER_H
#define __INDEXLIB_SEARCH_CACHE_PARTITION_WRAPPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/cache/search_cache.h"

IE_NAMESPACE_BEGIN(util);

class SearchCachePartitionWrapper
{
private:
    struct CacheKey
    {
        uint64_t key;
        uint64_t partitionId;
    };
public:
    SearchCachePartitionWrapper(const SearchCachePtr& searchCache, uint64_t partitionId);
    ~SearchCachePartitionWrapper();
public:

    template<typename CacheItem>
    bool Put(uint64_t key, regionid_t regionId, CacheItem* cacheItem)
    {
        assert(mSearchCache);
        CacheKey cacheKey = MakeKey(key, regionId);
        autil::ConstString constStringKey((const char*)&cacheKey, sizeof(CacheKey));
        return mSearchCache->Put(constStringKey, cacheItem);
    }

    std::unique_ptr<CacheItemGuard> Get(uint64_t key, regionid_t regionId)
    {
        assert(mSearchCache);
        CacheKey cacheKey = MakeKey(key, regionId);
        autil::ConstString constStringKey((const char*)&cacheKey, sizeof(CacheKey));
        return mSearchCache->Get(constStringKey);
    }

    size_t GetUsage() const
    {
        assert(mSearchCache);
        return mSearchCache->GetUsage();
    }

    void ResetCounter() const { mSearchCache->ResetCounter(); }
    int64_t GetHitCount() const { return mSearchCache->GetHitCount(); }
    int64_t GetMissCount() const { return mSearchCache->GetMissCount(); }

    SearchCachePtr GetSearchCache() const { return mSearchCache; }

private:
    CacheKey MakeKey(uint64_t key, regionid_t regionId)
    {
        CacheKey cacheKey;
        cacheKey.key = key;
        cacheKey.partitionId = mPartitionId ^ regionId;
        return cacheKey;
    }

private:
    SearchCachePtr mSearchCache;
    uint64_t mPartitionId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SearchCachePartitionWrapper);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_SEARCH_CACHE_PARTITION_WRAPPER_H
