#ifndef ISEARCH_SEARCHERCACHE_H
#define ISEARCH_SEARCHERCACHE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/SearcherCacheConfig.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/util/memcache/MemCache.h>
#include <ha3/search/SearcherCacheStrategy.h>
#include <ha3/search/CacheResult.h>
#include <ha3/util/ChainedFixedSizePool.h>

BEGIN_HA3_NAMESPACE(search);

enum CacheMissType {
    CMT_NONE,                // hit
    CMT_NOT_IN_CACHE,        // not in cache
    CMT_EMPTY_RESULT,        // empty result
    CMT_EXPIRE,              // expire
    CMT_DELETE_TOO_MUCH,     // delete too much
    CMT_TRUNCATE,            // truncate
    CMT_TRUNCATE_OPTIMIZER,  // truncate optimizer
    CMT_UNKNOWN
};

class SearcherCache {
public:
    SearcherCache();
    ~SearcherCache();
private:
    SearcherCache(const SearcherCache &);
    SearcherCache& operator=(const SearcherCache &);
public:
    bool init(const config::SearcherCacheConfig &config);
    bool get(uint64_t key, uint32_t curTime,
             CacheResult *&cacheResult, autil::mem_pool::Pool *pool,
             CacheMissType &missType);
    bool put(uint64_t key, uint32_t curTime,
             const SearcherCacheStrategyPtr &searcherCacheStrategyPtr,
             CacheResult *cacheResult, autil::mem_pool::Pool *pool);

    bool validateCacheResult(
            uint64_t key, bool useTruncate,
            const uint32_t requiredTopk,
            const SearcherCacheStrategyPtr &searcherCacheStrategyPtr,
            CacheResult *cacheResult, CacheMissType &missType);

    bool exceedIncDocLimit(uint32_t totalMatchDocs) {
        return totalMatchDocs >= _config.incDocLimit;
    }

    void deleteCacheItem(uint64_t key);

    const config::SearcherCacheConfig& getConfig() const {
        return _config;
    }

    uint32_t getCacheLatencyLimit() const {
        return (uint32_t)(_config.latencyLimitInMs * 1000);
    }

    void getCacheStat(uint64_t &memUse, uint32_t &itemNum);
public:
    inline void increaseCacheGetNum() { ++_cacheGetNum; }
    inline void increaseCacheHitNum() { ++_cacheHitNum; }
    inline uint32_t getAndResetCacheGetNum() {
        return _cacheGetNum.exchange(0);
    }
    inline uint32_t getAndResetCacheHitNum() {
        return _cacheHitNum.exchange(0);
    }
private:
    bool doPut(uint64_t key, const CacheResult *cacheResult,
               autil::mem_pool::Pool *pool = NULL);
    bool doGet(uint64_t key, uint32_t curTime,
               CacheResult *&cacheResult,
               autil::mem_pool::Pool *pool,
               CacheMissType &missType);
    bool isExpire(uint32_t curTime, const CacheHeader *header);
    bool delTooMuch(uint32_t delCount, uint32_t matchDocCount);
    bool validateCachedDocNum(const CacheResult *cacheResult) const;

public:
    // for test
    util::MemCache* getMemCache() {
        return _memCache;
    }
    friend class SearcherCacheTest;
private:
    static const uint32_t CACHE_ITEM_BLOCK_SIZE;
private:
    config::SearcherCacheConfig _config;
    util::ChainedFixedSizePool *_pool;
    util::MemCache *_memCache;
    std::atomic_uint _cacheGetNum;
    std::atomic_uint _cacheHitNum;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SearcherCache);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SEARCHERCACHE_H
