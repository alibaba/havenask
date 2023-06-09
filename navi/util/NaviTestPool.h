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
#ifndef NAVI_NAVITESTPOOL_H
#define NAVI_NAVITESTPOOL_H

#include "autil/mem_pool/Pool.h"
#include <vector>

namespace autil {
namespace mem_pool {

class PoolAsan : public Pool
{
public:
    PoolAsan(ChunkAllocatorBase *allocator, size_t chunkSize,
             size_t alignSize = DEFAULT_ALIGN_SIZE)
        : Pool(allocator, chunkSize, alignSize)
    {
    }

    PoolAsan(size_t chunkSize = DEFAULT_CHUNK_SIZE,
             size_t alignSize = DEFAULT_ALIGN_SIZE)
        : Pool(chunkSize, alignSize)
    {
    }
    ~PoolAsan() {
        reset();
    }
public:
    void* allocate(size_t numBytes) override {
        ScopedSpinLock lock(_mutex);
        auto ptr = malloc(numBytes);
        _chunks.push_back(ptr);
        _allocSize += numBytes;
        return ptr;
    }
    void deallocate(void *ptr, size_t size) override {
        return;
    }
    void release() override {
        reset();
    }
    size_t reset() override {
        ScopedSpinLock lock(_mutex);
        for (auto chunk : _chunks) {
            free(chunk);
        }
        _chunks.clear();
        _allocSize = 0;
        return 0;
    }
    size_t getUsedBytes() const override {
        ScopedSpinLock lock(_mutex);
        return _allocSize;
    }
    size_t getTotalBytes() const override {
        ScopedSpinLock lock(_mutex);
        return _allocSize;
    }

private:
    std::vector<void *> _chunks;
};

}
}

#endif //NAVI_NAVITESTPOOL_H
