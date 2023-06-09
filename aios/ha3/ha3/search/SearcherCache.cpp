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
#include "ha3/search/SearcherCache.h"

#include <assert.h>
#include <string.h>
#include <memory>

#include "alog/Logger.h"
#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Result.h"
#include "ha3/common/SearcherCacheClause.h"
#include "ha3/config/SearcherCacheConfig.h"
#include "ha3/search/CacheResult.h"
#include "ha3/search/SearcherCacheStrategy.h"
#include "ha3/util/memcache/ChainedFixedSizePool.h"
#include "ha3/util/memcache/MemCache.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace autil;
using namespace autil::mem_pool;
using namespace isearch::util;
using namespace isearch::config;
using namespace isearch::common;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, SearcherCache);

const uint32_t SearcherCache::CACHE_ITEM_BLOCK_SIZE = 1024 - sizeof(void*);

SearcherCache::SearcherCache() {
    _memCache = NULL;
    _pool = NULL;
    atomic32_set(&_cacheGetNum, 0);
    atomic32_set(&_cacheHitNum, 0);
}

SearcherCache::~SearcherCache() {
    delete _memCache;
    delete _pool;
}

bool SearcherCache::init(const SearcherCacheConfig &config) {
    _config = config;
    _pool = new util::ChainedFixedSizePool;
    _memCache = new util::MemCache;

    _pool->init(CACHE_ITEM_BLOCK_SIZE);
    return _memCache->init(((uint64_t)config.maxSize) << 20,
                           config.maxItemNum, _pool);
}

bool SearcherCache::put(
        uint64_t key, autil::PartitionRange partRange, uint32_t curTime,
        const SearcherCacheStrategyPtr &searcherCacheStrategyPtr,
        CacheResult *cacheResult, Pool *pool)
{
    assert(searcherCacheStrategyPtr.get() && cacheResult);
    if (!searcherCacheStrategyPtr->beforePut(curTime, cacheResult)) {
        return false;
    }
    if (!validateCachedDocNum(cacheResult)) {
        return false;
    }
    return doPut(key, partRange, cacheResult, pool);
}

bool SearcherCache::doPut(uint64_t key, autil::PartitionRange partRange,
                          const CacheResult *cacheResult, Pool *pool)
{
    autil::DataBuffer dataBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, pool);
    cacheResult->serialize(dataBuffer);

    int dataLen = dataBuffer.getDataLen();
    char *src = dataBuffer.getData();
    ChainedBlockItem item = _pool->createItem(src, dataLen);

    static const size_t keyLen = sizeof(key) + sizeof(partRange);
    char keyBuf[keyLen];
    memcpy(keyBuf, (char*)&key, sizeof(key));
    memcpy(keyBuf + sizeof(key), (char*)&partRange, sizeof(partRange));
    MCElem mcKey(keyBuf, keyLen);
    MCValueElem mcVaue(item, dataLen);

    int ret = _memCache->put(mcKey, mcVaue);
    if (ret != 0) {
        AUTIL_LOG(WARN, "put item failed, key:[%ld], errorCode:[%d]",
                key, ret);
        _pool->deallocate(item);
        return false;
    }

    return true;
}

bool SearcherCache::get(
        uint64_t key, autil::PartitionRange partRange, uint32_t curTime,
        CacheResult *&cacheResult, Pool *pool, CacheMissType &missType)
{
    CacheResult *tmpCacheResult = NULL;
    if (!doGet(key, partRange, curTime, tmpCacheResult, pool, missType)) {
        cacheResult = NULL;
        return false;
    }

    cacheResult = tmpCacheResult;
    missType = CMT_NONE;
    return true;
}

bool SearcherCache::doGet(uint64_t key, autil::PartitionRange partRange, uint32_t curTime,
                          CacheResult *&cacheResult, Pool *pool,
                          CacheMissType &missType)
{
    static const size_t keyLen = sizeof(key) + sizeof(partRange);
    char keyBuf[keyLen];
    memcpy(keyBuf, (char*)&key, sizeof(key));
    memcpy(keyBuf + sizeof(key), (char*)&partRange, sizeof(partRange));
    MCElem mcKey(keyBuf, keyLen);
    MemCacheItem *item = _memCache->get(mcKey);
    if (item == NULL) {
        AUTIL_LOG(DEBUG, "get from bottom cache faild, key:[%ld]", key);
        missType = CMT_NOT_IN_CACHE;
        return false;
    }

    autil::DataBuffer dataBuffer(item->value.dataSize, pool);
    _pool->read(item->value, dataBuffer.getStart());
    item->decref();

    CacheResult *tmpCacheResult = new CacheResult;
    tmpCacheResult->deserializeHeader(dataBuffer);
    CacheHeader *header = tmpCacheResult->getHeader();
    if (isExpire(curTime, header)) {
        AUTIL_LOG(DEBUG, "cache result expire");
        missType = CMT_EXPIRE;
        deleteCacheItem(key, partRange);
        delete tmpCacheResult;
	return false;
    }

    tmpCacheResult->deserializeResult(dataBuffer, pool);
    cacheResult = tmpCacheResult;
    missType = CMT_NONE;
    return true;
}

bool SearcherCache::isExpire(uint32_t curTime,
                             const CacheHeader *header)
{
    if (curTime == SearcherCacheClause::INVALID_TIME) {
        curTime = (uint32_t)TimeUtility::currentTimeInSeconds();
    }
    AUTIL_LOG(DEBUG, "expireTime: %d, curTime:%d",
            header->expireTime, curTime);
    return (header->expireTime < curTime);
}

bool SearcherCache::validateCacheResult(
        uint64_t key, PartitionRange partRange, bool useTruncate, uint32_t requiredTopk,
        const SearcherCacheStrategyPtr &strategyPtr,
        CacheResult *cacheResult, CacheMissType &missType)
{
    // use truncate disabled, cache result is truncate, cache miss.
    if (!useTruncate && cacheResult->useTruncateOptimizer()) {
        missType = CMT_TRUNCATE_OPTIMIZER;
        return false;
    }
    common::Result *result = cacheResult->getResult();
    if (result == NULL) {
        AUTIL_LOG(DEBUG, "empty result need delete");
        missType = CMT_EMPTY_RESULT;
        deleteCacheItem(key, partRange);
        return false;
    }
    MatchDocs *matchDocs = result->getMatchDocs();
    assert(matchDocs);

    uint32_t matchDocCount = matchDocs->size();
    uint32_t delCount = strategyPtr->filterCacheResult(cacheResult);
    assert(matchDocs->actualMatchDocs() >= delCount);
    matchDocs->setActualMatchDocs(matchDocs->actualMatchDocs() - delCount);
    matchDocs->setTotalMatchDocs(matchDocs->totalMatchDocs() - delCount);

    if (delTooMuch(delCount, matchDocCount)) {
        AUTIL_LOG(DEBUG, "delete too much, delCount:%d, matchDocCount:%d",
                delCount, matchDocCount);
        missType = CMT_DELETE_TOO_MUCH;
        deleteCacheItem(key, partRange);
        return false;
    }

    if (cacheResult->isTruncated() &&
        matchDocCount - delCount < requiredTopk)
    {
        AUTIL_LOG(DEBUG, "not enough doc, requiredTopk:%d, actualCount:%d",
                requiredTopk, matchDocCount - delCount);
        missType = CMT_TRUNCATE;
	return false;
    }

    return true;
}

bool SearcherCache::delTooMuch(uint32_t delCount, uint32_t matchDocCount) {
    if (delCount > 0 &&
        (uint32_t)((float)delCount / matchDocCount * 100) >= _config.incDeletionPercent)
    {
        return true;
    }
    return false;
}

void SearcherCache::deleteCacheItem(uint64_t key, autil::PartitionRange partRange) {
    static const size_t keyLen = sizeof(key) + sizeof(partRange);
    char keyBuf[keyLen];
    memcpy(keyBuf, (char*)&key, sizeof(key));
    memcpy(keyBuf + sizeof(key), (char*)&partRange, sizeof(partRange));
    MCElem mcKey(keyBuf, keyLen);
    _memCache->del(mcKey);
}

void SearcherCache::getCacheStat(uint64_t &memUse, uint32_t &itemNum) {
    _memCache->getStat(memUse, itemNum);
}

bool SearcherCache::validateCachedDocNum(const CacheResult *cacheResult) const {
    if (!cacheResult->getResult()) {
        return true;
    }
    uint32_t docNum = cacheResult->getResult()->getTotalMatchDocs();
    return (docNum >= _config.minAllowedCacheDocNum)
        && (docNum <= _config.maxAllowedCacheDocNum);
}

} // namespace search
} // namespace isearch
