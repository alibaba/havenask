#include "MemCache.h"
#include "crc.h"

using namespace std;
BEGIN_HA3_NAMESPACE(util);

HA3_LOG_SETUP(util, MemCache);
HA3_LOG_SETUP(util, MemCacheItemList);

bool MemCacheItemList::init(u_n64_t limit, u_n32_t maxitems, ChainedFixedSizePool *pool)
{
    _totalNode = 0;
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
            ++_totalNode;
        }
    }

    u_n32_t nManagerSize = (sizeof(MemCacheItem) * 2 + sizeof(autil::ThreadMutex)) * CACHE_QUEUES;
    nManagerSize += sizeof(MemCacheItem) * nPreLoadItems * CACHE_QUEUES;
    if (nManagerSize >= limit)
        return false;
    limit -= nManagerSize;
    _maxsize.store(limit);
    _malloced.store(0);
    _swap.store(0);
    _itemCount.store(0);
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
                _malloced -= size;
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
    if(_malloced.load() < maxsize) {
        return true;
    }
    if(_delheads[id]) {
        return false;
    }

    bool bRet = false;

    MemCacheItem * search = _tails[id];
    while (search != NULL) {
        if (cachemap->tryLockWrite(search->key) == 0) {
            bool bTmp = false;
            if (search->getref() == 0) {
                MemCacheItem *removed = (MemCacheItem *)cachemap->remove(search->key, search);
                if (removed == search) {
                    bTmp = true;
                    ++_swap;
                    --_itemCount;
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
                    if (_malloced.fetch_sub(size) < maxsize) {
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

inline int MemCacheItemList::add(MemCacheItem *it, CCacheMap *cachemap)
{
    bool bTmp = false;
    u_n32_t size = it->value.dataSize;
    n64_t leftsize = _maxsize.load() - size;

    u_n8_t id = it->id;
    if (_locks[id].lock() == 0) {
        if (_malloced.load() < leftsize || remove_lru_node(id, leftsize, cachemap)) {
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
                ++_totalNode;
            }
            if (cachemap->insert(it2)) {
                link(it2, _heads, _tails);
                bTmp = true;
            }
        }
        _locks[id].unlock();
    }
    if (bTmp) {
        _malloced += size;
        ++_itemCount;
        return 0;
    } else
        return -2;
}

int MemCacheItemList::del(MemCacheItem *it)
{
    ++_swap;

    u_n8_t id = it->id;
    if (_locks[id].lock()) {
        return -2;
    }
    if(it->getref() == 0) {
        unlink(it, _heads, _tails);
        if(!it->value.empty()) {
            uint32_t size = it->value.dataSize;
            _pool->deallocate(it->value);
            _malloced -= size;
        }
        link(it, _freeheads, _freetails);
    } else {
        it->hash_next = _delheads[id];
        //HA3_LOG(INFO, "insert %p, id: %u, key %lu, list %s", it, id, it->key, "delhead");
        _delheads[id] = it;
    }
    _locks[id].unlock();
    --_itemCount;
    return 0;
}

inline int MemCacheItemList::update_r(MemCacheItem *it)
{
    u_n8_t id = it->id;
    if (_locks[id].lock() != 0)
        return -1;
    //HA3_LOG(INFO, "update_r %p, %u, %s", it, id, "head");
    if (_heads[id] != it) {
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

inline void MemCacheItemList::link(MemCacheItem *it, MemCacheItem **heads,  MemCacheItem **tails)
{
    //assert(it != heads[it->id]);
    //assert((heads[it->id] && tails[it->id]) || (heads[it->id] == NULL && tails[it->id] == NULL));
    u_n8_t id = it->id;
    // if (heads == _heads) {
    //     HA3_LOG(INFO, "link %p, %u, %s", it, id, "head");
    // }
    it->prev = NULL;
    it->next = heads[id];
    if (it->next)
        it->next->prev = it;
    heads[id] = it;
    if (tails[id] == NULL)
        tails[id] = it;
}

inline void MemCacheItemList::unlink(MemCacheItem *it, MemCacheItem **heads,  MemCacheItem **tails)
{
    u_n8_t id = it->id;
    // if (heads == _heads) {
    //     HA3_LOG(INFO, "unlink %p, %u, %s", it, id, "head");
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

inline MemCacheItem * MemCacheItemList::pop(u_n8_t id, MemCacheItem **heads,  MemCacheItem **tails)
{
    MemCacheItem *it = heads[id];
    // if (heads == _heads) {
    //     HA3_LOG(INFO, "pop %p, %u, %s", it, id, "head");
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
                HA3_LOG(ERROR, "item id [%u] do not match, list [%s] index [%zu]", it->id, "head", i);
            }
            auto iter = map.find(it);
            if (iter != map.end()) {
                HA3_LOG(ERROR, "duplicate item exist in lru, item1 %p, id1: %zu, list1: %s,"
                        "item2 %p, id2: %zu, list2: %s", iter->first, iter->second.first,
                        iter->second.second.c_str(), it, i, "head");
                return false;
            }
            map.insert(make_pair(it, make_pair(i, "head")));
            it = it->next;
        }
    }
    HA3_LOG(INFO, "map.size(): %zu", map.size());
    retMap = map;
    uint64_t delCount = 0;
    for (size_t i = 0; i < CACHE_QUEUES; ++i) {
        MemCacheItem *it = _delheads[i];
        while (it) {
            if (it->id != i) {
                HA3_LOG(ERROR, "item id [%u] do not match, list [%s] index [%zu]", it->id, "delhead", i);
                return false;
            }
            auto iter = map.find(it);
            if (iter == map.end()) {
                HA3_LOG(ERROR, "lru: item exist in delhead but not in head, item %p, id: %u", it, it->id);
                return false;
            }
            retMap.erase(it);
            it = (MemCacheItem *)it->hash_next;
            delCount++;
        }
    }
    HA3_LOG(INFO, "delhead size: %lu", delCount);
    if (retMap.size() != _itemCount.load()) {
        HA3_LOG(ERROR, "size not equal, retMap.size(): %zu, _itemCount: %u", retMap.size(), _itemCount.load());
        return false;
    }
    for (size_t i = 0; i < CACHE_QUEUES; ++i) {
        MemCacheItem *it = _freeheads[i];
        while (it) {
            if (it->id != i) {
                HA3_LOG(ERROR, "item id [%u] do not match, list [%s] index [%zu]", it->id, "freehead", i);
            }
            auto iter = map.find(it);
            if (iter != map.end()) {
                HA3_LOG(ERROR, "duplicate item exist in lru, item1 %p, id1: %zu, list1: %s,"
                        "item2 %p, id2: %zu, list2: %s", iter->first, iter->second.first,
                        iter->second.second.c_str(), it, i, "freehead");
                return false;
            }
            map.insert(make_pair(it, make_pair(i, "freehead")));
            it = it->next;
        }
    }
    HA3_LOG(INFO, "map.size(): %zu", map.size());
    uint64_t totalNode = _totalNode.load();
    if (totalNode != map.size()) {
        HA3_LOG(ERROR, "totalNode %lu does not equal map.size() %zu", totalNode, map.size());
        return false;
    }
    return true;
}

inline void MemCacheItemList::stat(stringstream &output)
{
    output<<"  total data size: "<<_malloced.load()<<endl;
    output<<"  total swap count: "<<_swap.load()<<endl;
}

inline void MemCacheItemList::stat(MemCacheStat &stat)
{
    stat.m_nTotalSize = _malloced.load();
    stat.m_nSwapCount = _swap.load();
}

inline void MemCacheItemList::getStat(uint64_t &memUse, uint32_t &itemNum)
{
    memUse = _malloced.load();
    itemNum = _itemCount.load();
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
    _fetch = 0;
    _hit = 0;
    _put = 0;
    _random = 0;
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


int MemCache::put(const MCElem &mckey, const MCValueElem &mcvalue)
{
    ++_put;
    u_n64_t hash_val;
    if(mckey.len == 8) {
        hash_val = *(uint64_t*)mckey.data;
    } else {
        hash_val = get_crc64(mckey.data, mckey.len);
    }
    u_n8_t id = (++_random) % CACHE_QUEUES;
    MemCacheItem it(hash_val, id, mcvalue.data);

    CacheItem *cache_item = NULL;
    if(_cachemap->lockWrite(hash_val) != 0)
        return -2;
    cache_item = _cachemap->find(hash_val, cache_item);
    if(cache_item) {
        _cachemap->remove(hash_val, cache_item);
    }
    int nRes = _MCItemList->add(&it, _cachemap);
    _cachemap->unlock(hash_val);
    if(cache_item) {
        _MCItemList->del((MemCacheItem*)cache_item);
    }
    if(nRes == 0) {
        u_n32_t nSize = _cachemap->rehash();
        if(nSize > 0) _MCItemList->reset_maxsize(nSize);
        return 0;
    } else {
        return -4;
    }
}

MemCacheItem* MemCache::get(const MCElem &mckey)
{
    ++_fetch;
    CacheItem *cache_item = NULL;
    u_n64_t hash_val;
    if(mckey.len == 8) {
        hash_val = *(uint64_t*)mckey.data;
    } else {
        hash_val = get_crc64(mckey.data, mckey.len);
    }
    if(_cachemap->lockRead(hash_val) != 0)
        return NULL;
    if((cache_item = _cachemap->find(hash_val, cache_item)) == NULL) {
        _cachemap->unlock(hash_val);
        return NULL;
    }
    MemCacheItem *it = (MemCacheItem *) cache_item;
    it->incref();
    _cachemap->unlock(hash_val);
    ++_hit;
    _MCItemList->update_r(it);
    return it;
}

int MemCache::del(const MCElem &mckey)
{
    u_n64_t hash_val;
    if(mckey.len == 8) {
        hash_val = *(uint64_t*)mckey.data;
    } else {
        hash_val = get_crc64(mckey.data, mckey.len);
    }

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
        HA3_LOG(ERROR, "size not equal, lruMap: %zu, hashMap: %zu", lruMap.size(), hashMap.size());
        ret = false;
    }
    for (const auto &itLru : lruMap) {
        auto itHash = hashMap.find(itLru.first);
        if (hashMap.end() == itHash) {
            HA3_LOG(ERROR, "lru item %p not exist in hashMap, id: %zu, list: %s",
                    itLru.first, itLru.second.first, itLru.second.second.c_str());
            ret = false;
        }
    }
    return ret;
}

void MemCache::getStat(stringstream &output, atomic_ulong& miss)
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

void MemCache::getStatValue(MemCacheStat &stat, atomic_ulong& miss)
{
    _MCItemList->stat(stat);
    stat.m_MAXELEM = _maxitems;
    stat.m_MAXSIZE = _maxsize;
    stat.m_nFetechCount = _fetch.load();
    stat.m_nHitMiss  = miss.load();
    stat.m_nHitCount = _hit.load() - miss.load();
    stat.m_nPutCount = _put.load();
    n64_t fetch = stat.m_nFetechCount > 0 ? stat.m_nFetechCount : 1;
    stat.m_nHitRatio = 100 * ((double)stat.m_nHitCount/fetch);
}

void MemCache::print()
{
    atomic_ulong miss(0);
    stringstream output;

    getStat(output, miss);
    printf("%s", output.str().c_str());
    _MCItemList->print();
}

void MemCache::getStat(uint64_t &memUse, uint32_t &itemNum) {
    _MCItemList->getStat(memUse, itemNum);
}

END_HA3_NAMESPACE(util);
