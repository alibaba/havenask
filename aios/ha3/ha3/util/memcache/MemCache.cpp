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
#include "ha3/util/memcache/MemCache.h"

#include <type_traits>

#include "alog/Logger.h"
#include "ha3/util/memcache/CacheMap.h"
#include "ha3/util/memcache/crc.h"

using namespace std;
namespace isearch {
namespace util {

AUTIL_LOG_SETUP(ha3, MemCache);
AUTIL_LOG_SETUP(ha3, MemCacheItemList);
bool MemCacheItemList::init(u_n64_t limit, u_n32_t maxitems, ChainedFixedSizePool *pool)
{
    atomic64_set(&_totalNode, 0);
    _pool = pool;
    u_n32_t nPreLoadItems = maxitems / CACHE_QUEUES;
    for (u_n8_t i = 0; i < CACHE_QUEUES; i++) {
        _heads[i] = NULL;
        _tails[i] = NULL;
        _freeheads[i] = NULL;
        _freetails[i] = NULL;
        _delheads[i] = NULL;
        for (u_n32_t j = 0; j < nPreLoadItems; j++) {
            MemCacheItem *item = new MemCacheItem(0, i);
            link(item, _freeheads, _freetails);
            atomic64_inc(&_totalNode);
        }
    }
    u_n32_t nManagerSize = (sizeof(MemCacheItem) * 2 + sizeof(autil::ThreadMutex)) * CACHE_QUEUES;
    nManagerSize += sizeof(MemCacheItem) * nPreLoadItems * CACHE_QUEUES;
    if (nManagerSize >= limit)
        return false;
    limit -= nManagerSize;
    atomic64_set(&_maxsize, limit);
    atomic64_set(&_malloced, 0);
    atomic64_set(&_swap, 0);
    atomic32_set(&_itemCount, 0);
    return true;
}

bool MemCacheItemList::remove_lru_node(u_n8_t id, n64_t maxsize, CCacheMap *cachemap)
{
    MemCacheItem* del = _delheads[id];
    MemCacheItem* head = NULL;
    while(del) {
        MemCacheItem* tmp = del;
        if(del->getref() == 0) {
            unlink(del, _heads, _tails);
            if(!del->value.empty()) {
                uint32_t size = del->value.dataSize;
                _pool->deallocate(del->value);
                atomic64_sub(size, &_malloced);
            }
            del = (MemCacheItem*)del->hash_next;
            link(tmp, _freeheads, _freetails);
        } else {
            del = (MemCacheItem*)del->hash_next;
            tmp->hash_next = head;
            head = tmp;
        }
    }
    _delheads[id] = head;
    if(atomic16_read(&_malloced) < maxsize) {
        return true;
    }
    if(_delheads[id]) { // 删除队列非空，淘汰可能会出错
        return false;
    }

    bool bRet = false;
    //插入是在队列头，扫描从队尾开始，队尾插入的时间肯定比队列头的要早
    MemCacheItem * search = _tails[id];
    while (search != NULL) {
        if (cachemap->tryLockWrite(search->key) == 0) {
            bool bTmp = false;
            if (search->getref() == 0) {
                MemCacheItem *removed = (MemCacheItem *)cachemap->remove(search->key, search);
                if (removed == search) {
                    bTmp = true;
                    atomic64_inc(&_swap); //插入次数++
                    atomic32_dec(&_itemCount);
                }
            }
            cachemap->unlock(search->key);
            if (bTmp) {
                MemCacheItem * search2 = search;
                search = search->prev;
                unlink(search2, _heads, _tails);
                link(search2, _freeheads, _freetails);
                if (!search2->value.empty()) {
                    uint32_t size = search2->value.dataSize;
                    _pool->deallocate(search2->value);
                    if (atomic64_sub_return(size, &_malloced) < maxsize) { //获取到足够的空间就结束
                        bRet = true;
                        break;
                    }
                }
                continue;
            } //end if bTmp
        }//end if trylock
        search = search->prev;
    }//end for
    return bRet;
}

//将新的item添加到it->id对应的队列中，如果当时已经使用的内存超过限制，则先进行释放
inline int MemCacheItemList::add(MemCacheItem *it, CCacheMap *cachemap)
{
    bool bTmp = false;
    u_n32_t size = it->value.dataSize;
    n64_t leftsize = atomic64_read(&_maxsize) - size;

    u_n8_t id = it->id;
    if (_locks[id].lock() == 0) {
        if (atomic64_read(&_malloced) < leftsize || remove_lru_node(id, leftsize, cachemap)) {
            MemCacheItem *it2 = pop(id, _freeheads, _freetails);
            if (it2 != NULL) {
                it2->init(it);
            } else {
                if ((it2 = new MemCacheItem(it)) == NULL) {
                    _locks[id].unlock();
                    return -1;
                }
                size += sizeof(MemCacheItem);
                leftsize -= sizeof(MemCacheItem);
                atomic64_inc(&_totalNode);
            }
            if (cachemap->insert(it2)) {
                link(it2, _heads, _tails);
                bTmp = true;
            }
        }
        _locks[id].unlock();
    }
    if (bTmp) {
        atomic64_add(size, &_malloced);
        atomic32_inc(&_itemCount);
        return 0;
    } else
        return -2;
}

int MemCacheItemList::del(MemCacheItem *it)
{
    atomic64_inc(&_swap); //插入次数++

    u_n8_t id = it->id;
    if (_locks[id].lock()) {
        return -2;
    }
    if(it->getref() == 0) {
        unlink(it, _heads, _tails);
        if(!it->value.empty()) {
            uint32_t size = it->value.dataSize;
            _pool->deallocate(it->value);
            atomic64_sub(size, &_malloced);
        }
        link(it, _freeheads, _freetails);
    } else {
        it->hash_next = _delheads[id];
        //AUTIL_LOG(INFO, "insert %p, id: %u, key %lu, list %s", it, id, it->key, "delhead");
        _delheads[id] = it;
    }
    _locks[id].unlock();
    atomic32_dec(&_itemCount);
    return 0;
}
//每访问一次进行更新（插入到队列头）
inline int MemCacheItemList::update_r(MemCacheItem *it)
{
    u_n8_t id = it->id;
    if (_locks[id].lock() != 0)
        return -1;
    //AUTIL_LOG(INFO, "update_r %p, %u, %s", it, id, "head");
    if (_heads[id] != it) {
        //要更新的节点不是head节点，而且队列最起码有2个node
        //unlink
        if (_tails[id] == it)
            _tails[id] = it->prev;
        if (it->next) it->next->prev = it->prev;
        it->prev->next = it->next;
        //move to head
        it->prev = NULL;
        it->next = _heads[id];
        it->next->prev = it;
        _heads[id] = it;
    }
    _locks[id].unlock();
    return 0;
}

inline int MemCacheItemList::update(MemCacheItem *it)
{
    u_n8_t id = it->id;
    if (_heads[id] != it) {
        //要更新的节点不是head节点，而且队列最起码有2个node
        //unlink
        if (_tails[id] == it)
            _tails[id] = it->prev;
        if (it->next) it->next->prev = it->prev;
        it->prev->next = it->next;
        //move to head
        it->prev = NULL;
        it->next = _heads[id];
        it->next->prev = it;
        _heads[id] = it;
    }
    return 0;
}

//清空全部已经分配的内存
void MemCacheItemList::clear(MemCacheItem **heads, MemCacheItem **tails)
{
    MemCacheItem *it = NULL, *it2 = NULL;
    for (u_n8_t i = 0; i < CACHE_QUEUES; i++) {
        if(_locks[i].lock() == 0) {
            it = tails[i];
            while(it != NULL) {
                it2 = it;
                it = it->prev;
                unlink(it2, heads, tails);
                if (!it2->value.empty()) {
                    _pool->deallocate(it2->value);
                }
                delete it2;
            }
            heads[i] = NULL;
            tails[i] = NULL;
            _locks[i].unlock();
        }
    }
}

void MemCacheItemList::print()
{
    MemCacheItem *it = NULL;
    for(u_n8_t i = 0; i < CACHE_QUEUES; i++) {
        if(_locks[i].lock() == 0) {
            printf("queue %d ------------------------------ \n", i);
            for (it = _tails[i]; it != NULL; it = it->prev)
                it->print();
            printf("\n");
            _locks[i].unlock();
        }
    }
}

//将item插入到队列中
inline void MemCacheItemList::link(MemCacheItem *it, MemCacheItem **heads,  MemCacheItem **tails)
{
    //assert(it != heads[it->id]);
    //assert((heads[it->id] && tails[it->id]) || (heads[it->id] == NULL && tails[it->id] == NULL));
    u_n8_t id = it->id;
    // if (heads == _heads) {
    //     AUTIL_LOG(INFO, "link %p, %u, %s", it, id, "head");
    // }
    it->prev = NULL;
    it->next = heads[id];
    if (it->next)
        it->next->prev = it;
    heads[id] = it;
    if (tails[id] == NULL)
        tails[id] = it;
}

//将item从队列删除
inline void MemCacheItemList::unlink(MemCacheItem *it, MemCacheItem **heads,  MemCacheItem **tails)
{
    u_n8_t id = it->id;
    // if (heads == _heads) {
    //     AUTIL_LOG(INFO, "unlink %p, %u, %s", it, id, "head");
    // }
    if (heads[id] == it)
        heads[id] = it->next;
    if (tails[id] == it)
        tails[id] = it->prev;
    //assert(it->next != it);
    //assert(it->prev != it);
    if (it->next) it->next->prev = it->prev;
    if (it->prev) it->prev->next = it->next;
}

//将item从队列删除
inline MemCacheItem * MemCacheItemList::pop(u_n8_t id, MemCacheItem **heads,  MemCacheItem **tails)
{
    MemCacheItem *it = heads[id];
    // if (heads == _heads) {
    //     AUTIL_LOG(INFO, "pop %p, %u, %s", it, id, "head");
    // }
    if (it == NULL) return NULL;
    heads[id] = it->next;
    if (tails[id] == it)
        tails[id] = NULL;
    if (it->next)
        it->next->prev = NULL;
    return it;
}

bool MemCacheItemList::consistencyCheck(std::map<void *, std::pair<size_t, string>> &retMap) {
    std::map<void *, std::pair<size_t, string>> map;
    for (size_t i = 0; i < CACHE_QUEUES; ++i) {
        MemCacheItem *it = _heads[i];
        while (it) {
            if (it->id != i) {
                AUTIL_LOG(ERROR, "item id [%u] do not match, list [%s] index [%zu]", it->id, "head", i);
            }
            auto iter = map.find(it);
            if (iter != map.end()) {
                AUTIL_LOG(ERROR, "duplicate item exist in lru, item1 %p, id1: %zu, list1: %s,"
                        "item2 %p, id2: %zu, list2: %s", iter->first, iter->second.first,
                        iter->second.second.c_str(), it, i, "head");
                return false;
            }
            map.insert(make_pair(it, make_pair(i, "head")));
            it = it->next;
        }
    }
    AUTIL_LOG(INFO, "map.size(): %zu", map.size());
    retMap = map;
    uint64_t delCount = 0;
    for (size_t i = 0; i < CACHE_QUEUES; ++i) {
        MemCacheItem *it = _delheads[i];
        while (it) {
            if (it->id != i) {
                AUTIL_LOG(ERROR, "item id [%u] do not match, list [%s] index [%zu]", it->id, "delhead", i);
                return false;
            }
            auto iter = map.find(it);
            if (iter == map.end()) {
                AUTIL_LOG(ERROR, "lru: item exist in delhead but not in head, item %p, id: %u", it, it->id);
                return false;
            }
            retMap.erase(it);
            it = (MemCacheItem *)it->hash_next;
            delCount++;
        }
    }
    AUTIL_LOG(INFO, "delhead size: %lu", delCount);
    if (retMap.size() != atomic32_read(&_itemCount)) {
        AUTIL_LOG(ERROR, "size not equal, retMap.size(): %zu, _itemCount: %u", retMap.size(), atomic32_read(&_itemCount));
        return false;
    }
    for (size_t i = 0; i < CACHE_QUEUES; ++i) {
        MemCacheItem *it = _freeheads[i];
        while (it) {
            if (it->id != i) {
                AUTIL_LOG(ERROR, "item id [%u] do not match, list [%s] index [%zu]", it->id, "freehead", i);
            }
            auto iter = map.find(it);
            if (iter != map.end()) {
                AUTIL_LOG(ERROR, "duplicate item exist in lru, item1 %p, id1: %zu, list1: %s,"
                        "item2 %p, id2: %zu, list2: %s", iter->first, iter->second.first,
                        iter->second.second.c_str(), it, i, "freehead");
                return false;
            }
            map.insert(make_pair(it, make_pair(i, "freehead")));
            it = it->next;
        }
    }
    AUTIL_LOG(INFO, "map.size(): %zu", map.size());
    uint64_t totalNode = atomic64_read(&_totalNode);
    if (totalNode != map.size()) {
        AUTIL_LOG(ERROR, "totalNode %lu does not equal map.size() %zu", totalNode, map.size());
        return false;
    }
    return true;
}

//两种统计方式，给监控程序（比如simon）使用
inline void MemCacheItemList::stat(stringstream &output)
{
    output<<"  total data size: "<<atomic64_read(&_malloced)<<endl;
    output<<"  total swap count: "<<atomic64_read(&_swap)<<endl;
}

inline void MemCacheItemList::stat(MemCacheStat &stat)
{
    stat.m_nTotalSize = atomic64_read(&_malloced);
    stat.m_nSwapCount = atomic64_read(&_swap);
}

inline void MemCacheItemList::getStat(uint64_t &memUse, uint32_t &itemNum)
{
    memUse = atomic64_read(&_malloced);
    itemNum = atomic32_read(&_itemCount);
}


/************************************************************
 * MemCache class
 ************************************************************/
MemCache::MemCache()
{
    _cachemap = NULL;
    _MCItemList = NULL;
    _maxitems = 0;
    _maxsize = 0;
    atomic64_set(&_fetch, 0);
    atomic64_set(&_hit, 0);
    atomic64_set(&_put, 0);
    atomic64_set(&_random, 0);
}

MemCache::~MemCache()
{
    if(_cachemap) {
        delete _cachemap;
        _cachemap = NULL;
    }
    if(_MCItemList) {
        delete _MCItemList;
        _MCItemList = NULL;
    }
}

//初始化操作，主要是各个对象的初始化
bool MemCache::init(u_n64_t maxsize, u_n32_t maxitems, ChainedFixedSizePool *pool)
{
    _maxsize = maxsize;
    _maxitems = maxitems;
    u_n32_t nMapSize = 0;
    if((_cachemap = new CCacheMap(maxitems)) == NULL || (nMapSize =_cachemap->init()) == 0)
        return false;
    nMapSize += (sizeof(CCacheMap) + sizeof(MemCacheItemList));
    if (nMapSize >= maxsize)
        return false;
    maxsize -= nMapSize;
    if((_MCItemList = new MemCacheItemList()) == NULL || !_MCItemList->init(maxsize, maxitems, pool))
        return false;
    return true;
}

//将查询的key及返回的结果放到memcache中, 0代表操作成功，其它失败
int MemCache::put(const MCElem &mckey, const MCValueElem &mcvalue)
{
    atomic64_inc(&_put); //插入次数++
    u_n64_t hash_val;
    if(mckey.len == 8) {
        hash_val = *(uint64_t*)mckey.data;
    } else {
        hash_val = get_crc64(mckey.data, mckey.len);
    }
    u_n8_t id = atomic64_inc_return(&_random) % CACHE_QUEUES;
    MemCacheItem it(hash_val, id, mcvalue.data);

    //首先看是否已经在memcache了，如果在直接返回
    CacheItem *cache_item = NULL;
    if(_cachemap->lockWrite(hash_val) != 0)
        return -2;
    cache_item = _cachemap->find(hash_val, cache_item);
    if(cache_item) { // 更新
        _cachemap->remove(hash_val, cache_item);
    }
    int nRes = _MCItemList->add(&it, _cachemap);
    _cachemap->unlock(hash_val);
    if(cache_item) { // 删除掉老的
        _MCItemList->del((MemCacheItem*)cache_item);
    }
    if(nRes == 0) {
        u_n32_t nSize = _cachemap->rehash(); //如果add成功，可能需要对map做rehash操作
        if(nSize > 0) _MCItemList->reset_maxsize(nSize);
        return 0;
    } else {
        return -4;
    }
}

//从memcache查找mckey对应的结果，返回NULL代表失败,  由调用者负责释放内存(MemCacheItem)
MemCacheItem* MemCache::get(const MCElem &mckey)
{
    atomic64_inc(&_fetch); //查找次数++
    CacheItem *cache_item = NULL;
    u_n64_t hash_val;
    if(mckey.len == 8) {
        hash_val = *(uint64_t*)mckey.data;
    } else {
        hash_val = get_crc64(mckey.data, mckey.len);
    }
    if(_cachemap->lockRead(hash_val) != 0)
        return NULL;
    if((cache_item = _cachemap->find(hash_val, cache_item)) == NULL) { //如果map查找不到，则直接返回无结果
        _cachemap->unlock(hash_val);
        return NULL;
    }
    MemCacheItem *it = (MemCacheItem *) cache_item;
    it->incref();  //查找成功，引用计数++，引用计数不为0代表该内存不能被释放
    _cachemap->unlock(hash_val);
    atomic64_inc(&_hit);  //命中次数++
    _MCItemList->update_r(it);  //更新该item在队列中的位置
    return it;//成功
}

int MemCache::del(const MCElem &mckey)
{
    u_n64_t hash_val;
    if(mckey.len == 8) {
        hash_val = *(uint64_t*)mckey.data;
    } else {
        hash_val = get_crc64(mckey.data, mckey.len);
    }

    //首先看是否已经在memcache了，如果在直接返回
    CacheItem *cache_item = NULL;
    if(_cachemap->lockWrite(hash_val) != 0)
        return -2;
    cache_item = _cachemap->find(hash_val, cache_item);
    if(NULL == cache_item) {
        _cachemap->unlock(hash_val);
        return 0;
    }
    _cachemap->remove(hash_val, cache_item);
    _cachemap->unlock(hash_val);

    if(_MCItemList->del((MemCacheItem*)cache_item)) {
        return -3;
    }

    return 1;
}

bool MemCache::consistencyCheck() {
    bool ret = true;
    std::map<void *, std::pair<size_t, string>> lruMap;
    std::map<void *, size_t> hashMap;
    if (!_MCItemList->consistencyCheck(lruMap)) {
        ret = false;
    }
    if (!_cachemap->consistencyCheck(hashMap)) {
        ret = false;
    }
    if (lruMap.size() != hashMap.size()) {
        AUTIL_LOG(ERROR, "size not equal, lruMap: %zu, hashMap: %zu", lruMap.size(), hashMap.size());
        ret = false;
    }
    for (const auto &itLru : lruMap) {
        auto itHash = hashMap.find(itLru.first);
        if (hashMap.end() == itHash) {
            AUTIL_LOG(ERROR, "lru item %p not exist in hashMap, id: %zu, list: %s",
                    itLru.first, itLru.second.first, itLru.second.second.c_str());
            ret = false;
        }
    }
    return ret;
}

//以下是2个统计函数，主要统计查询次数，命中率和插入次数
void MemCache::getStat(stringstream &output, atomic64_t& miss)
{
    MemCacheStat stat;
    getStatValue(stat, miss);

    output<<"  max item size: "<< stat.m_MAXELEM <<endl;
    output<<"  max cache size: "<< stat.m_MAXSIZE <<endl;
    output<<"  total fetch count: "<< stat.m_nFetechCount <<endl;
    output<<"  total mis count: "<< stat.m_nHitMiss <<endl;
    output<<"  total hit count: "<< stat.m_nHitCount <<endl;
    output<<"  total put count: "<< stat.m_nPutCount <<endl;
    output<<"  cache hit ratio: "<< stat.m_nHitRatio <<"%"<<endl;
    _MCItemList->stat(output);
}

void MemCache::getStatValue(MemCacheStat &stat, atomic64_t& miss)
{
    _MCItemList->stat(stat);
    stat.m_MAXELEM = _maxitems;
    stat.m_MAXSIZE = _maxsize;
    stat.m_nFetechCount = atomic64_read(&_fetch);
    stat.m_nHitMiss  = atomic64_read(&miss);
    stat.m_nHitCount = atomic64_read(&_hit) - atomic64_read(&miss);
    stat.m_nPutCount = atomic64_read(&_put);
    n64_t fetch = stat.m_nFetechCount > 0 ? stat.m_nFetechCount : 1;
    stat.m_nHitRatio = 100 * ((double)stat.m_nHitCount/fetch);
}

void MemCache::print()
{
    atomic64_t miss;
    atomic64_set(&miss, 0);

    stringstream output;

    getStat(output, miss);
    printf("%s", output.str().c_str());
    _MCItemList->print();
}

void MemCache::getStat(uint64_t &memUse, uint32_t &itemNum) {
    _MCItemList->getStat(memUse, itemNum);
}

} // namespace util
} // namespace isearch
