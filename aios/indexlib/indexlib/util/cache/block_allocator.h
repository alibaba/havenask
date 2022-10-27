#ifndef __INDEXLIB_BLOCK_ALLOCATOR_H
#define __INDEXLIB_BLOCK_ALLOCATOR_H

#include <tr1/memory>
#include <autil/Lock.h>
#include <autil/Allocators.h>
#include <autil/FixedSizeAllocator.h>
#include <autil/FixedSizeRecyclePool.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/cache/cache_allocator.h"
#include "indexlib/util/cache/block.h"

IE_NAMESPACE_BEGIN(util);

class BlockAllocator final : public CacheAllocator
{
public:
    BlockAllocator(size_t cacheSize, uint32_t blockSize)
        : mBlockHeaderAllocator(sizeof(Block))
        , mBlockDataPool(blockSize, GetNumPerAlloc(cacheSize, blockSize))
    { }
    ~BlockAllocator() = default;

public:
    void* Allocate() override { return AllocBlock(); }
    void Deallocate(void* addr) override { FreeBlock((Block*)addr); }

public:
    Block* AllocBlock()
    {
        autil::ScopedLock lock(mLock);
        Block *block = new (mBlockHeaderAllocator.allocate())Block();
        block->data = (uint8_t *)mBlockDataPool.allocate();
        return block;
    }

    void FreeBlock(Block* block)
    {
        autil::ScopedLock lock(mLock);
        mBlockDataPool.free(block->data);
        block->~Block();
        mBlockHeaderAllocator.free(block);    
    }

public:
    size_t TEST_GetAllocatedCount() const
    { return mBlockHeaderAllocator.getCount(); }

private:
    static size_t GetNumPerAlloc(size_t cacheSize, uint32_t blockSize)
    {
        const size_t DEFAULT_SIZE_PER_ALLOC = 1024 * 1024 * 1024; // 1G
        size_t sizePerAlloc = std::min(cacheSize + 64 * 1024 * 1024,
                DEFAULT_SIZE_PER_ALLOC);
        return (sizePerAlloc + blockSize - 1) / blockSize;
    }

private:
    autil::ThreadMutex mLock;
    autil::FixedSizeAllocator mBlockHeaderAllocator;
    autil::FixedSizeRecyclePool<autil::MmapAllocator> mBlockDataPool;
};

DEFINE_SHARED_PTR(BlockAllocator);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BLOCK_ALLOCATOR_H
