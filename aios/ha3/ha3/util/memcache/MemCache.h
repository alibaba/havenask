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
/*
 *  Memory Cache for C++, use LRU algorithm.
 *
 *  Author : Michael Ni/Terry Wei, report bug to hao.nih@alibaba-inc.com or terry.wei@alibaba-inc.com
 *
 *  ChangeLog :
 *    1. 2007-12-01 : Michael Ni created this class and tested all  interfaces.
 *    2. 2009-11-17 : Terry Wei reconstructed it by using the double linked list and thread-safe hashmap
 */

#pragma once

#include <stdint.h>
#include <stdio.h>
#include <map>
#include <sstream>
#include <string>
#include <utility>

#include "autil/Lock.h"

#include "autil/Log.h"
#include "ha3/util/memcache/CacheItem.h"
#include "ha3/util/memcache/ChainedFixedSizePool.h"
#include "ha3/util/memcache/atomic.h"

namespace isearch {
namespace util {
class CCacheMap;
}  // namespace util
}  // namespace isearch


namespace isearch {
namespace util {
const static u_n8_t CACHE_QUEUES = 16;

//add by wuzhu for simon monitoring,20080603
struct MemCacheStat
{
    MemCacheStat():m_MAXSIZE(0),m_nTotalSize(0),m_nFetechCount(0), m_nHitCount(0),
                   m_nPutCount(0),m_nHitMiss(0), m_nHitRatio(0), m_MAXELEM(0),
                   m_nItemSize(0), m_nSwapCount(0){}
    u_n64_t m_MAXSIZE;
    u_n64_t m_nTotalSize;
    u_n64_t m_nFetechCount;
    u_n64_t m_nHitCount;
    u_n64_t m_nPutCount;
    u_n64_t m_nHitMiss;
    double m_nHitRatio;
    u_n32_t m_MAXELEM;
    u_n32_t m_nItemSize;
    u_n64_t m_nSwapCount;
};

struct MCElem
{
    MCElem():data(NULL), len(0){}
    MCElem(char *buf, u_n32_t n)
    {
        data = buf;
        len = n;
    }
    char *data;
    u_n32_t len;
};

struct MCValueElem
{
    MCValueElem()
    {
        len = 0;
    }
    MCValueElem(ChainedBlockItem buf, u_n32_t n)
    {
        data = buf;
        len = n;
    }
    ChainedBlockItem data;
    u_n32_t len;
};

struct MemCacheItem : public CacheItem
{
    MemCacheItem(u_n64_t key1 = 0, u_n8_t id1 = 0, ChainedBlockItem value1 = ChainedBlockItem())
    {
        key = key1;
        next = NULL;
        prev = NULL;
        id = id1;
        setref(0);
        value = value1;
    }

    MemCacheItem(const MemCacheItem * item)
    {
        init(item);
    }

    void init(const MemCacheItem * item)
    {
        key = item->key;
        next = item->next;
        prev = item->prev;
        id = item->id;
        refcount = item->refcount;
        value = item->value;
    }

    MemCacheItem  *next;
    MemCacheItem  *prev;
    u_n8_t id;
    atomic16_t refcount;
    ChainedBlockItem value;

    void incref() { atomic16_inc(&refcount); }
    void decref() { atomic16_dec(&refcount); }
    void setref(u_n16_t i) { atomic16_set(&refcount, i); }
    u_n16_t getref() { return atomic16_read(&refcount); }

    void print()
    {
        fprintf(stderr, "id = %d, refcount = %d, key = %lu\n", id, getref(), key);
    }
};

class MemCacheItemList
{
public:
    MemCacheItemList(){}
    virtual ~MemCacheItemList()
    {
        clear(_heads, _tails);
        clear(_freeheads, _freetails);
    }
    bool init(u_n64_t limit, u_n32_t maxitems, ChainedFixedSizePool *pool);
    int add(MemCacheItem *it, CCacheMap *cachemap);
    int del(MemCacheItem *it);
    int update_r(MemCacheItem *it);
    int update(MemCacheItem *it);
    void stat(std::stringstream &output);
    void stat(MemCacheStat &stat);
    void getStat(uint64_t &memUse, uint32_t &itemNum);
    void print();
    void reset_maxsize(u_n32_t nSize) { atomic64_sub(nSize, &_maxsize); }
    bool consistencyCheck(
            std::map<void *, std::pair<size_t, std::string>> &retMap);

private:
    void link(MemCacheItem *it, MemCacheItem **heads, MemCacheItem **tails);
    void unlink(MemCacheItem *it, MemCacheItem **heads, MemCacheItem **tails);
    bool remove_lru_node(u_n8_t id, n64_t maxsize, CCacheMap *cachemap);
    u_n32_t get_item_len(const MemCacheItem *it)
    {
        const static u_n32_t ITEM_SIZE = sizeof(MemCacheItem);
        return (it->value.dataSize + ITEM_SIZE);
    }
    void clear(MemCacheItem **heads, MemCacheItem **tails);
    MemCacheItem *pop(u_n8_t id, MemCacheItem **heads,  MemCacheItem **tails);

    atomic64_t _maxsize;
    atomic64_t _malloced;
    atomic64_t _swap;
    atomic64_t _totalNode;
    atomic32_t _itemCount;
    MemCacheItem *_heads[CACHE_QUEUES];
    MemCacheItem *_tails[CACHE_QUEUES];
    MemCacheItem *_freeheads[CACHE_QUEUES];
    MemCacheItem *_freetails[CACHE_QUEUES];
    MemCacheItem *_delheads[CACHE_QUEUES];
    autil::ThreadMutex _locks[CACHE_QUEUES];
    ChainedFixedSizePool *_pool;
    AUTIL_LOG_DECLARE();
};

class MemCache
{
public:
    MemCache();
    ~MemCache();
    bool init(u_n64_t maxsize, u_n32_t maxitems, ChainedFixedSizePool *pool);
    int put(const MCElem &mckey, const MCValueElem &mcvalue);
    MemCacheItem* get(const MCElem &mckey);
    int del(const MCElem &mckey);
    void getStat(std::stringstream &output, atomic64_t &miss);
    void getStatValue(MemCacheStat &stat, atomic64_t& miss);
    void getStat(uint64_t &memUse, uint32_t &itemNum);
    void print();
    bool consistencyCheck();

private:
    CCacheMap *_cachemap;
    MemCacheItemList *_MCItemList;
    u_n32_t _maxitems;
    u_n64_t _maxsize;
    atomic64_t _fetch;
    atomic64_t _hit;
    atomic64_t _put;
    atomic64_t _random;
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch
