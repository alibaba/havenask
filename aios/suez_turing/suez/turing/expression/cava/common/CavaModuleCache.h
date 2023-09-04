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

#include <list>
#include <unordered_map>

#include "autil/Lock.h"
#include "cava/codegen/CavaJitModule.h"

namespace suez {
namespace turing {

template <class KeyType, class ValType>
class ObjLRUCache {
public:
    ObjLRUCache(size_t maxSize) {
        _maxSize = maxSize;
        _totalSize = 0;
    }
    ~ObjLRUCache() {}

private:
    ObjLRUCache(const ObjLRUCache &);
    ObjLRUCache &operator=(const ObjLRUCache &);

public:
    typedef typename std::shared_ptr<ValType> ValTypePtr;
    typedef typename std::pair<KeyType, ValTypePtr> KeyValType;
    typedef typename std::list<KeyValType>::iterator ListIterType;

    // if found key not update val
    void put(const KeyType &key, const ValTypePtr &value) {
        ValTypePtr tmp;
        {
            size_t id = key % SLICE_SIZE;
            auto &itemMap = _itemMap[id];
            auto &lruList = _lruList[id];
            autil::ScopedLock lock(_mutex[id]);
            auto it = itemMap.find(key);
            if (it != itemMap.end()) {
                lruList.splice(lruList.begin(), lruList, it->second);
                return;
            }
            lruList.push_front(KeyValType(key, value));
            itemMap[key] = lruList.begin();
            ++_totalSize;
            if (_totalSize > _maxSize) {
                auto last = lruList.end();
                --last;
                tmp = last->second;
                itemMap.erase(last->first);
                lruList.pop_back();
                --_totalSize;
            }
        }
    }
    const ValTypePtr get(const KeyType &key) {
        size_t id = key % SLICE_SIZE;
        auto &itemMap = _itemMap[id];
        auto &lruList = _lruList[id];
        autil::ScopedLock lock(_mutex[id]);
        auto it = itemMap.find(key);
        if (it == itemMap.end()) {
            return ValTypePtr();
        } else {
            lruList.splice(lruList.begin(), lruList, it->second);
            return it->second->second;
        }
    }
    size_t size() const { return _totalSize; }

private:
    static const size_t SLICE_SIZE = 16;
    autil::ThreadMutex _mutex[SLICE_SIZE];
    std::list<KeyValType> _lruList[SLICE_SIZE];
    std::unordered_map<KeyType, ListIterType> _itemMap[SLICE_SIZE];
    size_t _maxSize;
    std::atomic_size_t _totalSize;
};

typedef ObjLRUCache<size_t, ::cava::CavaJitModule> CavaModuleCache;
typedef typename std::shared_ptr<CavaModuleCache> CavaModuleCachePtr;
} // namespace turing
} // namespace suez
