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

#include <memory>
#include <stack>
#include <stddef.h>

#include "autil/Lock.h"
#include "autil/mem_pool/Pool.h"

namespace autil {
namespace mem_pool {

template <class T>
class MemPoolFactory {
public:
    static constexpr size_t DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024; // 5M
    static constexpr size_t MAX_ALLOCATE_SIZE = 10 * 1024 * 1024; // 10M
    static constexpr size_t MAX_RESERVE_POOL_SIZE = 400;

public:
    MemPoolFactory(size_t chunkSize = DEFAULT_CHUNK_SIZE,
                   size_t maxAllocSize = MAX_ALLOCATE_SIZE,
                   size_t maxAllocPoolNum = MAX_RESERVE_POOL_SIZE)
        : _chunkSize(chunkSize), _maxAllocSize(maxAllocSize), _maxAllocPoolNum(maxAllocPoolNum) {}
    ~MemPoolFactory();

public:
    Pool *getPool();
    void recycle(Pool *pool);
    void getPoolVec(std::vector<Pool *> &poolVec, size_t expectedCount);
    void recyclePoolVec(std::vector<Pool *> &poolVec);
    size_t getPoolStackSize() const { return _poolStack.size(); }

    size_t getChunkSize() const { return _chunkSize; }
    size_t getMaxAllocSize() const { return _maxAllocSize; }
    size_t getMaxAllocPoolNum() const { return _maxAllocPoolNum; }

private:
    autil::ThreadMutex _mutex;
    std::stack<Pool *> _poolStack;

    size_t _chunkSize;
    size_t _maxAllocSize;
    size_t _maxAllocPoolNum;
};

template <class T>
MemPoolFactory<T>::~MemPoolFactory() {
    Pool *pool = NULL;
    autil::ScopedLock lock(_mutex);
    while (_poolStack.size() > 0) {
        pool = _poolStack.top();
        _poolStack.pop();
        if (pool) {
            pool->reset();
            delete pool;
        }
    }
}

template <class T>
inline Pool *MemPoolFactory<T>::getPool() {
    {
        Pool *pool = NULL;
        autil::ScopedLock lock(_mutex);
        while (_poolStack.size() > 0) {
            pool = _poolStack.top();
            _poolStack.pop();
            if (pool) {
                return pool;
            }
        }
    }
    return new (std::nothrow) T(_chunkSize);
}

template <class T>
inline void MemPoolFactory<T>::recycle(Pool *pool) {
    if (!pool)
        return;
    if (pool->getTotalBytes() > _maxAllocSize) {
        pool->release();
    } else {
        pool->reset();
    }
    {
        autil::ScopedLock lock(_mutex);
        if (_poolStack.size() > _maxAllocPoolNum) {
            delete pool;
            return;
        }
        _poolStack.push(pool);
    }
}

template <class T>
inline void MemPoolFactory<T>::getPoolVec(std::vector<Pool *> &poolVec, size_t expectedCount) {
    size_t count = 0;
    {
        autil::ScopedLock lock(_mutex);
        while (_poolStack.size() > 0 && count < expectedCount) {
            auto pool = _poolStack.top();
            _poolStack.pop();
            if (pool) {
                count++;
                poolVec.push_back(pool);
            }
        }
    }
    while (count < expectedCount) {
        poolVec.push_back(new (std::nothrow) T(_chunkSize));
        count++;
    }
}

template <class T>
inline void MemPoolFactory<T>::recyclePoolVec(std::vector<Pool *> &poolVec) {
    if (poolVec.empty())
        return;
    for (auto &pool : poolVec) {
        if (!pool)
            return;
        if (pool->getTotalBytes() > _maxAllocSize) {
            pool->release();
        } else {
            pool->reset();
        }
    }
    {
        autil::ScopedLock lock(_mutex);
        for (auto &pool : poolVec) {
            if (_poolStack.size() > _maxAllocPoolNum) {
                delete pool;
                continue;
            }
            _poolStack.push(pool);
        }
        poolVec.clear();
    }
}

} // namespace mem_pool
} // namespace autil
