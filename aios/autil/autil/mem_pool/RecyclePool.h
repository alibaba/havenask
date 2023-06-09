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

#include <stdlib.h>
#include <utility>
#if __cplusplus >= 201103L
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"


namespace autil { namespace mem_pool {

class RecyclePool : public Pool
{
public:
#if __cplusplus >= 201103L
    typedef std::unordered_map<size_t, void *> HashMap;
#else
    typedef std::tr1::unordered_map<size_t, void *> HashMap;
#endif
public:
    RecyclePool(ChunkAllocatorBase* allocator, size_t chunkSize,
                size_t alignSize = 1);
    RecyclePool(size_t chunkSize, size_t alignSize = 1);
    ~RecyclePool();

private:
    RecyclePool(const RecyclePool&);
    void operator=(const RecyclePool&);

public:
    /**
     * numBytes >= 8
     */
    virtual void* allocate(size_t numBytes) override;

    /*
     * only less than 64k memory block can be deallocated
     */
    virtual void deallocate(void* ptr, size_t size) override;
    virtual void release() override;
    virtual size_t reset() override;

    size_t getFreeSize() const;

public:
    //for test
    HashMap& getFreeList();

private:
    static void*& nextOf(void* ptr);

private:
    size_t _freeSize;
    HashMap _freeList;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////
//implementation

inline void*& RecyclePool::nextOf(void *ptr)
{
    return *(static_cast<void **>(ptr));
}

inline void* RecyclePool::allocate(size_t numBytes)
{
    size_t allocSize = alignBytes(numBytes, _alignSize);
    if (allocSize < 8) {
        allocSize = 8;
    }
    HashMap::iterator it = _freeList.find(allocSize);
    if (it != _freeList.end()) {
        void* ptr = it->second;
        if (ptr != NULL) {
            SanitizerUtil::UnpoisonMemoryRegion(ptr, allocSize);
            _freeList[allocSize] = nextOf(ptr);
            _freeSize -= allocSize;
            _allocSize += allocSize;
            return ptr;
        }
    }
    return Pool::allocate(allocSize);
}

inline void RecyclePool::deallocate(void *ptr, size_t size)
{
    if (!ptr || size == 0) {
        return;
    }
    size_t allocSize = alignBytes(size, _alignSize);
    if (allocSize < 8) {
        allocSize = 8;
    }

    HashMap::iterator it = _freeList.find(allocSize);
    if (it == _freeList.end()) {
        _freeList[allocSize] = NULL;
    }
    nextOf(ptr) = _freeList[allocSize];
    _freeList[allocSize] = ptr;
    _allocSize -= allocSize;
    _freeSize += allocSize;
    SanitizerUtil::PoisonMemoryRegion(ptr, allocSize);
}

inline void RecyclePool::release()
{
    _freeSize = 0;
    _freeList.clear();

    Pool::release();
}

inline size_t RecyclePool::reset()
{
    _freeSize = 0;
    _freeList.clear();

    return Pool::reset();
}

inline size_t RecyclePool::getFreeSize() const
{
    return _freeSize;
}

inline RecyclePool::HashMap& RecyclePool::getFreeList()
{
    return _freeList;
}

}}

