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

#include <algorithm>
#include <assert.h>
#include <cstdio>
#include <memory>
#include <set>
#include <stdexcept>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "autil/mem_pool/PoolBase.h"

namespace indexlibv2 { namespace util {

inline constexpr size_t HUGE_PAGE_SIZE = 2 * 1024 * 1024; // 2M

template <typename T, typename U>
static inline T AlignUp(T val, U alignment)
{
    assert((alignment & (alignment - 1)) == 0);
    return (val + alignment - 1) & ~(alignment - 1);
}

inline void* AllocMmapPage(size_t nBytes)
{
    if (nBytes % HUGE_PAGE_SIZE) {
        printf("invalid nBytes [%zu]", nBytes);
        return nullptr;
    }
    int mflags = MAP_PRIVATE | MAP_ANONYMOUS;
    size_t totalAllocatedSize = nBytes + HUGE_PAGE_SIZE;
    void* p = mmap(0, totalAllocatedSize, PROT_NONE, mflags, -1, 0);
    if (unlikely(p == MAP_FAILED)) {
        printf("mmap failed!");
        return nullptr;
    }
    uintptr_t firstPage = AlignUp((uintptr_t)p, HUGE_PAGE_SIZE);
    size_t excessHead = firstPage - (uintptr_t)p;
    size_t excessTail = HUGE_PAGE_SIZE - excessHead;
    if (excessHead) {
        munmap(p, excessHead);
    }
    if (excessTail) {
        munmap((void*)(firstPage + nBytes), excessTail);
    }
    if (madvise((void*)firstPage, nBytes, MADV_HUGEPAGE) != 0) {
        printf("madvise %p failed!", (void*)firstPage);
        return nullptr;
    }
    if (mprotect((void*)firstPage, nBytes, PROT_READ | PROT_WRITE) != 0) {
        printf("mprotect %p failed!", (void*)firstPage);
        return nullptr;
    }
    memset((void*)firstPage, 0, nBytes);
    return (void*)firstPage;
}

inline void DeallocMmapPage(void* p, size_t n)
{
    if (n % HUGE_PAGE_SIZE) {
        printf("invalid nBytes [%zu]", n);
        throw std::runtime_error("DeallocMmappage invalid size");
    }

    for (size_t i = 0; i < n; i += HUGE_PAGE_SIZE) {
        munmap((void*)((size_t)p + i), HUGE_PAGE_SIZE);
        usleep(100);
    }
}

template <typename T>
class HugePageAlignedAllocator
{
public:
    HugePageAlignedAllocator() throw() {}
    HugePageAlignedAllocator(const HugePageAlignedAllocator&) throw() {}
    template <typename U>
    HugePageAlignedAllocator(const HugePageAlignedAllocator<U>&) throw()
    {
    }

    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = size_t;
    using difference_type = ptrdiff_t;

    template <typename U>
    struct rebind {
        using other = HugePageAlignedAllocator<U>;
    };

    pointer allocate(size_type n, const void* hint = 0) { return (pointer)AllocMmapPage(AlignUp(n, HUGE_PAGE_SIZE)); }

    void deallocate(pointer p, size_type n)
    {
        size_type alignNbyte = AlignUp(n, HUGE_PAGE_SIZE);
        DeallocMmapPage(p, alignNbyte);
    }
};

struct ChunkCmp {
    bool operator()(const autil::mem_pool::MemoryChunk* lhs, const autil::mem_pool::MemoryChunk* rhs) const
    {
        if (lhs->getFreeSize() != rhs->getFreeSize()) {
            return lhs->getFreeSize() < rhs->getFreeSize();
        }
        return lhs < rhs;
    }
};

class HugePageArenaPool : public autil::mem_pool::PoolBase
{
public:
    static constexpr size_t DEFAULT_CHUNK_SIZE = 10 * 1024 * 1024; // 10M
    static constexpr size_t DEFAULT_ALIGN_SIZE = sizeof(char*);
    static constexpr size_t RELEASE_RATE = 10;

    using MemoryChunk = autil::mem_pool::MemoryChunk;

    HugePageArenaPool(size_t chunkSize = DEFAULT_CHUNK_SIZE) : _chunkSize(chunkSize), _allocSize(0), _totalChunkSize(0)
    {
        _memChunk = allocateChunk(0);
    }

    ~HugePageArenaPool() { release(); }

    void* allocate(size_t numBytes) override
    {
        size_t allocSize = AlignUp(numBytes, DEFAULT_ALIGN_SIZE);
        void* ptr = _memChunk->allocate(allocSize);
        if (!ptr) {
            MemoryChunk* chunk = allocateChunk(allocSize);
            if (!chunk) {
                return NULL;
            }
            _memChunk = chunk;
            ptr = _memChunk->allocate(allocSize);
        }
        _allocSize += allocSize;
        return ptr;
    }

    void deallocate(void*, size_t) override
    {
        // deallocate never
    }

    void release() override
    {
        for (auto memChunk : _usedChunk) {
            releaseChunk(memChunk);
        }
        for (auto memChunk : _freeChunk) {
            releaseChunk(memChunk);
        }
        _allocSize = 0;
    }

    size_t reset() override
    {
        for (auto memChunk : _usedChunk) {
            memChunk->reset();
            _freeChunk.insert(memChunk);
        }
        _usedChunk.clear();
        std::vector<MemoryChunk*> toReleaseChunk;
        for (auto memChunk : _freeChunk) {
            if (rand() % 100 < RELEASE_RATE) {
                toReleaseChunk.emplace_back(memChunk);
            }
        }
        for (auto memChunk : toReleaseChunk) {
            _totalChunkSize -= memChunk->getTotalBytes();
            _freeChunk.erase(memChunk);
            releaseChunk(memChunk);
        }
        size_t ret = _allocSize;
        _allocSize = 0;
        _memChunk = allocateChunk(_chunkSize);
        return ret;
    }

    void CleanUpToReuse() { (void)reset(); }

    bool isInPool(const void* ptr) const override
    {
        for (auto memChunk : _usedChunk) {
            if (memChunk->isInChunk(ptr)) {
                return true;
            }
        }
        return false;
    }

    size_t getUsedBytes() const { return _allocSize; }

    size_t getTotalBytes() const { return _totalChunkSize; }

private:
    void releaseChunk(MemoryChunk* memChunk) { DeallocMmapPage(memChunk, memChunk->getTotalBytes()); }

    MemoryChunk* allocateChunk(size_t nBytes)
    {
        auto cmp = [](const autil::mem_pool::MemoryChunk* lhs, size_t rhs) -> bool { return lhs->getFreeSize() < rhs; };

        auto iter = std::lower_bound(_freeChunk.begin(), _freeChunk.end(), nBytes, cmp);
        if (iter != _freeChunk.end()) {
            MemoryChunk* chunk = *iter;
            _freeChunk.erase(iter);
            _usedChunk.emplace_back(chunk);
            return chunk;
        }
        size_t nAllocSize = std::max(nBytes + sizeof(MemoryChunk), _chunkSize);
        nAllocSize = AlignUp(nAllocSize, HUGE_PAGE_SIZE);
        MemoryChunk* chunk = (MemoryChunk*)AllocMmapPage(nAllocSize);
        ::new (chunk) MemoryChunk(nAllocSize);
        _usedChunk.emplace_back(chunk);
        _totalChunkSize += nAllocSize;
        return chunk;
    }

    size_t _chunkSize;
    size_t _allocSize;
    size_t _totalChunkSize;
    std::vector<MemoryChunk*> _usedChunk;
    std::set<MemoryChunk*, ChunkCmp> _freeChunk;
    MemoryChunk* _memChunk;
};

}} // namespace indexlibv2::util
