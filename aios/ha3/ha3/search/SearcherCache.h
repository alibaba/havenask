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

#include <stddef.h>
#include <stdint.h>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/RangeUtil.h"
#include "ha3/config/SearcherCacheConfig.h"
#include "ha3/search/SearcherCacheStrategy.h"
#include "ha3/util/memcache/atomic.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace search {
class CacheHeader;
class CacheResult;
}  // namespace search
namespace util {
class ChainedFixedSizePool;
class MemCache;
}  // namespace util
}  // namespace isearch

namespace isearch {
namespace search {

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
    bool get(uint64_t key, autil::PartitionRange partRange, uint32_t curTime,
             CacheResult *&cacheResult, autil::mem_pool::Pool *pool,
             CacheMissType &missType);
    bool put(uint64_t key, autil::PartitionRange partRange, uint32_t curTime,
             const SearcherCacheStrategyPtr &searcherCacheStrategyPtr,
             CacheResult *cacheResult, autil::mem_pool::Pool *pool);

    bool validateCacheResult(
            uint64_t key, autil::PartitionRange partRange, bool useTruncate,
            const uint32_t requiredTopk,
            const SearcherCacheStrategyPtr &searcherCacheStrategyPtr,
            CacheResult *cacheResult, CacheMissType &missType);

    bool exceedIncDocLimit(uint32_t totalMatchDocs) {
        return totalMatchDocs >= _config.incDocLimit;
    }

    void deleteCacheItem(uint64_t key, autil::PartitionRange partRange);

    const config::SearcherCacheConfig& getConfig() const {
        return _config;
    }

    uint32_t getCacheLatencyLimit() const {
        return (uint32_t)(_config.latencyLimitInMs * 1000);
    }

    void getCacheStat(uint64_t &memUse, uint32_t &itemNum);
public:
    inline void increaseCacheGetNum() {atomic32_inc(&_cacheGetNum);}
    inline void increaseCacheHitNum() {atomic32_inc(&_cacheHitNum);}
    inline uint32_t getAndResetCacheGetNum() {
        uint32_t cacheGetNum = atomic32_read(&_cacheGetNum);
        atomic32_sub(cacheGetNum, &_cacheGetNum);
        return cacheGetNum;
    }
    inline uint32_t getAndResetCacheHitNum() {
        uint32_t cacheHitNum = atomic32_read(&_cacheHitNum);
        atomic32_sub(cacheHitNum, &_cacheHitNum);
        return cacheHitNum;
    }
private:
    bool doPut(uint64_t key, autil::PartitionRange partRange,
               const CacheResult *cacheResult,
               autil::mem_pool::Pool *pool = NULL);
    bool doGet(uint64_t key, autil::PartitionRange partRange,
               uint32_t curTime, CacheResult *&cacheResult,
               autil::mem_pool::Pool *pool, CacheMissType &missType);
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
    util::atomic32_t _cacheGetNum;
    util::atomic32_t _cacheHitNum;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearcherCache> SearcherCachePtr;

} // namespace search
} // namespace isearch
