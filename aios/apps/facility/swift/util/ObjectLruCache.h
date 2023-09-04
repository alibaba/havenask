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

#include <iterator>
#include <list>
#include <stddef.h>
#include <stdint.h>
#include <utility>

#include "autil/Lock.h"
#include "autil/Log.h"

namespace swift {
namespace util {
template <class K, class V>
class ObjectLruCache {
public:
    ObjectLruCache() : _totalHit(0), _totalGet(0) {}
    virtual ~ObjectLruCache() { clear(); }

private:
    ObjectLruCache(const ObjectLruCache &);
    ObjectLruCache &operator=(const ObjectLruCache &);

public:
    bool get(const K &key, V &value);
    void put(const K &key, const V &value);
    void remove(const K &key);
    bool pop();
    bool back(K &key, V &value);
    size_t size();
    bool empty();
    void clear();
    double hitRatio();

protected:
    autil::ThreadMutex _mutex;
    std::list<std::pair<K, V>> _objList; // most freq item in the end of list
    int64_t _totalHit;
    int64_t _totalGet;
    AUTIL_LOG_DECLARE();
};

template <class K, class V>
size_t ObjectLruCache<K, V>::size() {
    autil::ScopedLock lock(_mutex);
    return _objList.size();
}

template <class K, class V>
bool ObjectLruCache<K, V>::empty() {
    autil::ScopedLock lock(_mutex);
    return _objList.empty();
}

template <class K, class V>
void ObjectLruCache<K, V>::clear() {
    autil::ScopedLock lock(_mutex);
    return _objList.clear();
}

template <class K, class V>
bool ObjectLruCache<K, V>::get(const K &key, V &value) {
    autil::ScopedLock lock(_mutex);
    _totalGet++;
    bool found = false;
    for (typename std::list<std::pair<K, V>>::iterator it = _objList.begin(); it != _objList.end(); ++it) {
        if (it->first == key) {
            value = it->second;
            _objList.erase(it);
            found = true;
            _totalHit++;
            break;
        }
    }
    if (found) {
        _objList.push_back(std::make_pair(key, value));
    }
    return found;
}

template <class K, class V>
void ObjectLruCache<K, V>::put(const K &key, const V &value) {
    autil::ScopedLock lock(_mutex);
    for (typename std::list<std::pair<K, V>>::iterator it = _objList.begin(); it != _objList.end(); ++it) {
        if (it->first == key) {
            _objList.erase(it);
            break;
        }
    }
    _objList.push_back(std::make_pair(key, value));
}

template <class K, class V>
void ObjectLruCache<K, V>::remove(const K &key) {
    autil::ScopedLock lock(_mutex);
    for (typename std::list<std::pair<K, V>>::iterator it = _objList.begin(); it != _objList.end(); ++it) {
        if (it->first == key) {
            _objList.erase(it);
            break;
        }
    }
}

template <class K, class V>
bool ObjectLruCache<K, V>::pop() {
    autil::ScopedLock lock(_mutex);
    if (!_objList.empty()) {
        _objList.pop_front();
        return true;
    } else {
        return false;
    }
}

template <class K, class V>
bool ObjectLruCache<K, V>::back(K &key, V &value) {
    autil::ScopedLock lock(_mutex);
    if (!_objList.empty()) {
        std::pair<K, V> &obj = _objList.front();
        key = obj.first;
        value = obj.second;
        return true;
    } else {
        return false;
    }
}

template <class K, class V>
double ObjectLruCache<K, V>::hitRatio() {
    autil::ScopedLock lock(_mutex);
    if (_totalGet != 0) {
        return (double)_totalHit / _totalGet;
    } else {
        return 0;
    }
}

} // namespace util
} // namespace swift
