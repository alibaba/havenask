#ifndef __INDEXLIB_BLOCK_HANDLE_H
#define __INDEXLIB_BLOCK_HANDLE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class BlockHandle
{
public:
    BlockHandle()
        : mBlockCache(NULL)
        , mCacheHandle(NULL)
        , mBlock(NULL)
        , mBlockDataSize(0)
    {
    }

    BlockHandle(util::BlockCache* blockCache, util::Cache::Handle* handle,
               util::Block* block, size_t blockDataSize)
        : mBlockCache(blockCache)
        , mCacheHandle(handle)
        , mBlock(block)
        , mBlockDataSize(blockDataSize)
    {}

    ~BlockHandle()
    {
        Release();
    }

    BlockHandle(BlockHandle&& rhs)
        : mBlockCache(rhs.mBlockCache)
        , mCacheHandle(rhs.mCacheHandle)
        , mBlock(rhs.mBlock)
        , mBlockDataSize(rhs.mBlockDataSize)
    {
        rhs.mBlockCache = nullptr;
        rhs.mCacheHandle = nullptr;
        rhs.mBlock = nullptr;
        rhs.mBlockDataSize = 0;
    }

    BlockHandle& operator=(BlockHandle&& other)
    {
        if (this != &other)
        {
            std::swap(mBlockCache, other.mBlockCache);
            std::swap(mCacheHandle, other.mCacheHandle);
            std::swap(mBlock, other.mBlock);
            std::swap(mBlockDataSize, other.mBlockDataSize);
        }
        return *this;
    }

public:
    void Reset(util::BlockCache* blockCache,
               util::Cache::Handle* handle,
               util::Block* block, size_t blockDataSize)
    {
        Release();
        assert(blockCache);
        assert(block);
        assert(handle);
        assert(blockDataSize <= blockCache->GetBlockSize());
        
        mBlockCache = blockCache;
        mBlock = block;
        mCacheHandle = handle;
        mBlockDataSize = blockDataSize;
    }

    uint64_t GetOffset() const
    { return mBlock == NULL ? 0 : (mBlockCache->GetBlockSize() * mBlock->id.inFileIdx); }
    uint8_t* GetData() const { return mBlock == NULL ? NULL : mBlock->data; }
    uint32_t GetDataSize() const { return mBlockDataSize; }
    uint64_t GetFileId() const { return mBlock == NULL ? 0 : mBlock->id.fileId; }
    util::Block* GetBlock() const { return mBlock; }

    util::Cache::Handle* StealHandle()
    {
        util::Cache::Handle* ret = mCacheHandle;
        mCacheHandle = NULL;
        return ret;
    }
    
    void Release()
    {
        if (!mCacheHandle)
        {
            return;
        }

        if (mBlockCache)
        {
            mBlockCache->ReleaseHandle(mCacheHandle);
        }
        mCacheHandle = NULL;
        mBlock = NULL;
        mBlockDataSize = 0;
        mBlockCache = NULL;
    }

private:
    BlockHandle(const BlockHandle &);
    BlockHandle& operator=(const BlockHandle &);
    
private:
    util::BlockCache* mBlockCache;
    util::Cache::Handle* mCacheHandle;
    util::Block* mBlock;
    uint32_t mBlockDataSize;
};

DEFINE_SHARED_PTR(BlockHandle);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BLOCK_HANDLE_H
