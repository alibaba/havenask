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

#include "autil/Allocators.h"
#include "autil/FixedSizeAllocator.h"
#include "autil/FixedSizeRecyclePool.h"
#include "autil/Lock.h"
#include "autil/MemUtil.h"
#include "autil/cache/cache_allocator.h"
#include "indexlib/util/cache/Block.h"

namespace indexlib { namespace util {

class BlockAllocator final : public autil::CacheAllocator
{
public:
    BlockAllocator(size_t cacheSize, uint32_t blockSize) noexcept
        : _blockHeaderAllocator(sizeof(Block))
        , _blockDataPool(blockSize, GetNumPerAlloc(cacheSize, blockSize))
        , _blockSize(blockSize)
    {
    }
    ~BlockAllocator() noexcept = default;

public:
    void* Allocate() noexcept override { return AllocBlock(); }
    void Deallocate(void* addr) noexcept override { FreeBlock((Block*)addr); }

public:
    Block* AllocBlock() noexcept
    {
        autil::ScopedLock lock(_lock);
        Block* block = new (_blockHeaderAllocator.allocate()) Block();
        block->data = (uint8_t*)_blockDataPool.allocate();
        return block;
    }

    void FreeBlock(Block* block) noexcept
    {
        autil::ScopedLock lock(_lock);
        autil::MemUtil::unmarkReadOnlyForDebug(block->data, _blockSize);
        _blockDataPool.free(block->data);
        block->~Block();
        _blockHeaderAllocator.free(block);
    }

public:
    size_t TEST_GetAllocatedCount() const noexcept { return _blockHeaderAllocator.getCount(); }

private:
    static size_t GetNumPerAlloc(size_t cacheSize, uint32_t blockSize) noexcept
    {
        const size_t DEFAULT_SIZE_PER_ALLOC = 1024 * 1024 * 1024; // 1G
        size_t sizePerAlloc = std::min(cacheSize + 64 * 1024 * 1024, DEFAULT_SIZE_PER_ALLOC);
        return (sizePerAlloc + blockSize - 1) / blockSize;
    }

private:
    autil::ThreadMutex _lock;
    autil::FixedSizeAllocator _blockHeaderAllocator;
    autil::FixedSizeRecyclePool<autil::MmapAllocator> _blockDataPool;
    size_t _blockSize;
};

typedef std::shared_ptr<BlockAllocator> BlockAllocatorPtr;
}} // namespace indexlib::util
