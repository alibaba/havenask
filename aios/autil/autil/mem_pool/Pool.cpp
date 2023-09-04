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
#include "autil/mem_pool/Pool.h"

#include <stdint.h>

#include "autil/mem_pool/ChunkAllocatorBase.h"
#include "autil/mem_pool/SimpleAllocatePolicy.h"

namespace autil {
namespace mem_pool {

AUTIL_LOG_SETUP(autil, Pool);

MemoryChunk Pool::DUMMY_CHUNK;

Pool::Pool(AllocatePolicy *allocatePolicy, size_t alignSize)
    : _alignSize(alignSize), _memChunk(&(Pool::DUMMY_CHUNK)), _allocSize(0), _allocPolicy(allocatePolicy) {}

Pool::Pool(ChunkAllocatorBase *allocator, size_t chunkSize, size_t alignSize)
    : Pool(new SimpleAllocatePolicy(allocator, chunkSize), alignSize) {}

Pool::Pool(size_t chunkSize, size_t alignSize) : Pool(new SimpleAllocatePolicy(chunkSize), alignSize) {}

Pool::~Pool() {
    _memChunk = NULL;
    delete _allocPolicy;
    _allocPolicy = NULL;
}

bool Pool::isInPool(const void *ptr) const { return _allocPolicy->isInChunk(ptr); }

void *Pool::allocate(size_t numBytes) {
    ScopedSpinLock lock(_mutex);
    return allocateUnsafe(numBytes);
}

void *Pool::allocate(size_t numBytes, size_t alignment) {
    ScopedSpinLock lock(_mutex);
    return allocateUnsafe(numBytes, alignment);
}

void *Pool::allocateUnsafe(size_t numBytes) {
    size_t allocSize = alignBytes(numBytes, _alignSize);
    void *ptr = _memChunk->allocate(allocSize);
    if (!ptr) {
        MemoryChunk *chunk = _allocPolicy->allocate(allocSize);
        if (!chunk) {
            AUTIL_LOG(ERROR,
                      "Allocate too large memory chunk: %ld, "
                      "max available chunk size: %ld",
                      (int64_t)numBytes,
                      (int64_t)_allocPolicy->getAvailableChunkSize());
            return NULL;
        }
        _memChunk = chunk;
        ptr = _memChunk->allocate(allocSize);
    }
    _allocSize += allocSize;
    SanitizerUtil::UnpoisonMemoryRegion(ptr, allocSize);
    return ptr;
}

void *Pool::allocateUnsafe(size_t numBytes, size_t alignment) {
    size_t allocSize = alignBytes(numBytes, _alignSize);
    void *ptr = _memChunk->allocate(allocSize, alignment);
    if (!ptr) {
        MemoryChunk *chunk = _allocPolicy->allocate(allocSize);
        if (!chunk) {
            AUTIL_LOG(ERROR,
                      "Allocate memory chunk of size %ld failed, "
                      "available chunk size: %ld",
                      (int64_t)numBytes,
                      (int64_t)_allocPolicy->getAvailableChunkSize());
            return NULL;
        }
        _memChunk = chunk;
        ptr = _memChunk->allocate(allocSize, alignment);
    }
    _allocSize += allocSize;
    SanitizerUtil::UnpoisonMemoryRegion(ptr, allocSize);
    return ptr;
}

void Pool::clear() {
    ScopedSpinLock lock(_mutex);
    _allocPolicy->clear();
}

namespace {
class SharedPoolWrapper final : public Pool {
public:
    explicit SharedPoolWrapper(Pool *impl) : _impl(impl) {}
    ~SharedPoolWrapper() = default;

public:
    void *allocate(size_t numBytes) override { return _impl->allocate(numBytes); }
    void *allocate(size_t numBytes, size_t alignment) override { return _impl->allocate(numBytes, alignment); }

    void deallocate(void *ptr, size_t size) override { _impl->deallocate(ptr, size); }

    void release() override { _impl->release(); }
    size_t reset() override { return _impl->reset(); }

    size_t getUsedBytes() const override { return _impl->getUsedBytes(); }
    size_t getTotalBytes() const override { return _impl->getTotalBytes(); }

    bool isInPool(const void *ptr) const override { return _impl->isInPool(ptr); }

private:
    Pool *_impl = nullptr;
};
} // namespace

std::shared_ptr<Pool> Pool::maybeShared(Pool *pool) {
    if (pool == nullptr) {
        return {};
    }
    try {
        auto ptr = pool->shared_from_this();
        return ptr;
    } catch (std::bad_weak_ptr &e) { return std::make_shared<SharedPoolWrapper>(pool); }
}

// UnsafePool
UnsafePool::UnsafePool(ChunkAllocatorBase *allocator, size_t chunkSize, size_t alignSize)
    : Pool(allocator, chunkSize, alignSize) {}

UnsafePool::UnsafePool(size_t chunkSize, size_t alignSize) : Pool(chunkSize, alignSize) {}

} // namespace mem_pool
} // namespace autil
