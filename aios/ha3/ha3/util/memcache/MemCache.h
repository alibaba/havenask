/*
 *  Memory Cache for C++, use LRU algorithm.
 *
 *  Author : Michael Ni/Terry Wei, report bug to hao.nih@alibaba-inc.com or terry.wei@alibaba-inc.com
 *
 *  ChangeLog :
 *    1. 2007-12-01 : Michael Ni created this class and tested all  interfaces.
 *    2. 2009-11-17 : Terry Wei reconstructed it by using the double linked list and thread-safe hashmap
 */

#ifndef KINGSO_CACHE_MEMCACHE_H
#define KINGSO_CACHE_MEMCACHE_H

#include <sstream>
#include "autil/Lock.h"
#include "CacheMap.h"
#include <ha3/util/ChainedFixedSizePool.h>
#include <set>

BEGIN_HA3_NAMESPACE(util);

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
        refcount = item->refcount.load();
        value = item->value;
    }

    MemCacheItem  *next;
    MemCacheItem  *prev;
    u_n8_t id;
    std::atomic_ushort refcount;
    ChainedBlockItem value;

    void incref() { ++refcount; }
    void decref() { --refcount; }
    void setref(u_n16_t i) { refcount = i; }
    u_n16_t getref() { return refcount.load(); }

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

        int  update_r(MemCacheItem *it);
        int  update(MemCacheItem *it);

        void stat(std::stringstream &output);
        void stat(MemCacheStat &stat);
        void getStat(uint64_t &memUse, uint32_t &itemNum);

        void print();

        void reset_maxsize(u_n32_t nSize) {
            _maxsize -= nSize;
        }
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

        std::atomic_ulong _maxsize;
        std::atomic_ulong _malloced;
        std::atomic_ulong _swap;
        std::atomic_ulong _totalNode;
        std::atomic_uint _itemCount;
        MemCacheItem *_heads[CACHE_QUEUES];
        MemCacheItem *_tails[CACHE_QUEUES];
        MemCacheItem *_freeheads[CACHE_QUEUES];
        MemCacheItem *_freetails[CACHE_QUEUES];
        MemCacheItem *_delheads[CACHE_QUEUES];
        autil::ThreadMutex _locks[CACHE_QUEUES];
        ChainedFixedSizePool *_pool;
        HA3_LOG_DECLARE();
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

        void getStat(std::stringstream &output, std::atomic_ulong &miss);

        void getStatValue(MemCacheStat &stat, std::atomic_ulong& miss);
        void getStat(uint64_t &memUse, uint32_t &itemNum);

        void print();
        bool consistencyCheck();

    private:
        CCacheMap *_cachemap;
        MemCacheItemList *_MCItemList;
        u_n32_t _maxitems;
        u_n64_t _maxsize;
        std::atomic_ulong _fetch;
        std::atomic_ulong _hit;
        std::atomic_ulong _put;
        std::atomic_ulong _random;
        HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(util);

#endif
