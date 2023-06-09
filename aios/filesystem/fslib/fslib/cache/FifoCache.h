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
#ifndef FSLIB_FIFOCACHE_H
#define FSLIB_FIFOCACHE_H

#include <map>
#include <deque>
#include <cassert>
#include <iostream>
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "autil/Log.h"
#include "fslib/fslib.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/common/common_define.h"
#include "fslib/cache/FileBlockPool.h"

FSLIB_BEGIN_NAMESPACE(cache);

template<class K, class V, class GetSizeCallBack>
class FifoCache
{
private:
    typedef std::deque<K>  ActiveKeys;
    typedef std::pair<int32_t, V> CacheValue;
    typedef std::map<K, CacheValue> CacheMap;

public:
    FifoCache(uint64_t cacheSize,
              FileBlockPool *pool = NULL,
              GetSizeCallBack getSizeCallBack = GetSizeCallBack());
    ~FifoCache();

public:
    void setCacheSize(uint64_t cacheSize);
    void warmUp(const std::vector<K>& keyList, 
                             const std::vector<V>& valueList) {
        assert(false);
    }
    bool invalidate(const K& key);
    void invalidate(const std::vector<K>& keyList) {
        assert(false);
    }
    bool put(const K& key, const V& val);
    bool get(const K& key, V& val);
    bool update(const K& key, const V& newVal) {
        assert(false);
        return false;
    }
    float getHitRatio() const;
    bool isInCache(const K& key);

    int64_t getTotalQueryTimes() const {
        return _numGets;
    }

    int64_t getHitQueryTimes() const {
        return _numHits;
    }

    GetSizeCallBack& getGetSizeCallBack()
    {
        return _getSizeCallBack;
    }
    FileBlockPool* getFileBlockPool() const {
        return _pool;
    }

private:
    void removeHead();
    friend class FifoCacheTest;
    
private:
    GetSizeCallBack _getSizeCallBack;

    mutable autil::ReadWriteLock _rwLock;
    uint64_t _cacheSize;
    uint64_t _cacheSpaceUsed;
    ActiveKeys _activeKeys;
    CacheMap _cacheMap;

    int64_t _numHits;
    int64_t _numGets;
    FileBlockPool *_pool;

private:
    static alog::Logger *_logger;

};

template<class K, class V,  class GetSizeCallBack>
FifoCache<K, V, GetSizeCallBack>::FifoCache(
        uint64_t cacheSize, FileBlockPool *pool, GetSizeCallBack getSizeCallBack)
{
    _cacheSpaceUsed = 0;
    _cacheSize = cacheSize;
    _numHits = 0;
    _numGets = 0;
    _getSizeCallBack = getSizeCallBack;
    _pool = pool;

    AUTIL_LOG(INFO, "cache size: %s!", 
              autil::StringUtil::toString(cacheSize).c_str());
}

template<class K, class V,  class GetSizeCallBack>
FifoCache<K, V, GetSizeCallBack>::~FifoCache() {
}

template<class K, class V,  class GetSizeCallBack>
void FifoCache<K, V, GetSizeCallBack>::setCacheSize(uint64_t cacheSize){
    autil::ScopedWriteLock scopedWriteLock(_rwLock);
    _cacheSize = cacheSize;
    AUTIL_LOG(INFO, "cache size: %s!", 
              autil::StringUtil::toString(cacheSize).c_str());
}

template<class K, class V,  class GetSizeCallBack>
bool FifoCache<K, V, GetSizeCallBack>::put(const K& key, const V& val) {
    autil::ScopedWriteLock scopedWriteLock(_rwLock);
    CacheValue cacheValue = std::make_pair(1, val);

    typename CacheMap::iterator it = _cacheMap.find(key);
    if (it == _cacheMap.end()) {
        while (_cacheSpaceUsed + _getSizeCallBack(val) > _cacheSize && 
               !_activeKeys.empty()) {
            removeHead();
        }
        if (_cacheSpaceUsed + _getSizeCallBack(val) > _cacheSize) {
            AUTIL_LOG(WARN, "fail to put size: %s!", 
                      autil::StringUtil::toString(_getSizeCallBack(val)).c_str());
            return false;
        }
        _cacheMap[key] = cacheValue;
        _cacheSpaceUsed += _getSizeCallBack(val);
    } else {
        it->second.first++;
        AUTIL_LOG(DEBUG, "put same key %d times!", it->second.first);
    }
    _activeKeys.push_back(key);

    return true;
}

template<class K, class V,  class GetSizeCallBack>
bool FifoCache<K, V, GetSizeCallBack>::get(const K& key, V& val) {
    autil::ScopedReadLock scopedReadLock(_rwLock);
    _numGets += 1;
    typename CacheMap::const_iterator it = _cacheMap.find(key);
    if (it == _cacheMap.end()) {
        return false;
    }
    _numHits += 1;
    val = it->second.second;
    return true;
}

template<class K, class V,  class GetSizeCallBack>
float FifoCache<K, V, GetSizeCallBack>::getHitRatio() const {
    autil::ScopedReadLock scopedReadLock(_rwLock);
    if (_numGets == 0) {
        return 0;
    }
    return (float)_numHits / _numGets;
};

template<class K, class V,  class GetSizeCallBack>
void FifoCache<K, V, GetSizeCallBack>::removeHead() {
    K poppedCache = _activeKeys.front();
    _activeKeys.pop_front();
    typename CacheMap::iterator it = _cacheMap.find(poppedCache);
    if (it == _cacheMap.end()) {
        return ;
    }
    if (it->second.first <= 1) {
        _cacheSpaceUsed -= _getSizeCallBack(it->second.second);
        _cacheMap.erase(it);
    } else {
        it->second.first--;
    }
}

template<class K, class V,  class GetSizeCallBack>
bool FifoCache<K, V, GetSizeCallBack>::invalidate(const K& key) {
    autil::ScopedWriteLock scopedWriteLock(_rwLock);
    typename CacheMap::iterator it = _cacheMap.find(key);
    if (it == _cacheMap.end()) {
        return false;
    }
    _cacheSpaceUsed -= _getSizeCallBack(it->second.second);
    _cacheMap.erase(it);
    return true;
}

template<class K, class V, class GetSizeCallBack> 
bool FifoCache<K, V, GetSizeCallBack>::isInCache(const K& key) {
    autil::ScopedReadLock scopedReadLock(_rwLock);
    typename CacheMap::const_iterator it = _cacheMap.find(key);
    return it != _cacheMap.end();
}

template<class K, class V, class GetSizeCallBack>
alog::Logger *FifoCache<K, V, GetSizeCallBack>::_logger
= alog::Logger::getLogger("fslib.FiFoCache");

FSLIB_END_NAMESPACE(cache);

#endif //FSLIB_FIFOCACHE_H



