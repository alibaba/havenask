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
#include <unordered_set>

#include "autil/Lock.h"

namespace isearch {
namespace turing {

template <class KeyType>
class ObjKeyCache {
public:
    ObjKeyCache(size_t maxSize = 10000)
        : _maxSize(maxSize)
    {
    }
    ~ObjKeyCache() {}

private:
    ObjKeyCache(const ObjKeyCache &);
    ObjKeyCache &operator=(const ObjKeyCache &);

public:
    bool getAndPut(const KeyType &key) {
        size_t id = key % SLICE_SIZE;
        auto &itemSet = _itemSet[id];
        autil::ScopedLock lock(_mutex[id]);
        auto it = itemSet.find(key);
        if (it == itemSet.end()) {
            if (itemSet.size() >= _maxSize) {
                itemSet.clear();
            }
            itemSet.insert(key);
            return true;
        } else {
            return false;
        }
    }

private:
    static const size_t SLICE_SIZE = 16;
    autil::ThreadMutex _mutex[SLICE_SIZE];
    std::unordered_set<KeyType> _itemSet[SLICE_SIZE];
    size_t _maxSize;
};

typedef ObjKeyCache<size_t> SqlQueryCache;
typedef typename std::shared_ptr<SqlQueryCache> SqlQueryCachePtr;
}
}
