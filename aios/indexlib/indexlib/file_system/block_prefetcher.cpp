#include "indexlib/file_system/block_prefetcher.h"

using namespace std;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BlockPrefetcher);

BlockPrefetcher::BlockPrefetcher(BlockFileAccessor* accessor)
    : mStartBlockIdx(INVALID_BLOCK_IDX)
    , mPrefetchingBlockEnd(INVALID_BLOCK_IDX)
    , mAccessor(accessor)
    , mBlockCache(accessor->GetBlockCache())
    , mUsePrefetcher(mBlockCache->IsUsePrefetcher())
    , mPrefetchFuture(future_lite::makeReadyFuture(vector<BlockHandle>()))      
{
}


Block* BlockPrefetcher::ReadAhead(size_t offset, size_t maxOffset, util::Cache::Handle** handle)
{
    if (!mUsePrefetcher)
    {
        return GetBlock(offset, handle);
    }
    size_t blockIdx = mAccessor->GetBlockIdx(offset);
    if (mStartBlockIdx == INVALID_BLOCK_IDX)
    {
        mStartBlockIdx = blockIdx + 1;
        mPrefetchingBlockEnd = mStartBlockIdx;
        return GetBlock(offset, handle);
    }
    else if (blockIdx < mStartBlockIdx || blockIdx >= mPrefetchingBlockEnd)
    {
        mStartBlockIdx = blockIdx + 1;
        mHandles.clear();
        Block* block = GetBlock(offset, handle);
        Prefetch(offset, maxOffset, 1);
        return block;
    }
    else if (blockIdx < mStartBlockIdx + mHandles.size())
    {
        *handle = mHandles[blockIdx - mStartBlockIdx].StealHandle();
        if (mPrefetchFuture.hasResult())
        {
            WriteBack();
            if (blockIdx + 1 == mStartBlockIdx + mHandles.size())
            {
                Prefetch(offset, maxOffset, 2);                
            }
        }
        return mHandles[blockIdx - mStartBlockIdx].GetBlock();
    }
    else
    {
        assert(blockIdx < mPrefetchingBlockEnd);
        WriteBack();
        *handle = mHandles[blockIdx - mStartBlockIdx].StealHandle();
        if (blockIdx + 1 == mStartBlockIdx + mHandles.size())
        {
            Prefetch(offset, maxOffset, 2);
        }        
        return mHandles[blockIdx - mStartBlockIdx].GetBlock();
    }
    return nullptr;
}

void BlockPrefetcher::ReadAhead(size_t offset, size_t maxOffset)
{
    if (!mUsePrefetcher)
    {
        return;
    }
    size_t blockIdx = mAccessor->GetBlockIdx(offset);
    if (mStartBlockIdx == INVALID_BLOCK_IDX ||
        blockIdx < mStartBlockIdx || blockIdx >= mPrefetchingBlockEnd)
    {
        return;
    }
    if (!mPrefetchFuture.hasResult())
    {
        return;
    }
    WriteBack();
    if (blockIdx + 1 == mStartBlockIdx + mHandles.size())
    {
        Prefetch(offset, maxOffset, 2);
    }
}

Block* BlockPrefetcher::GetBlock(size_t offset, util::Cache::Handle** handle)
{
    Block* block = mAccessor->GetBlock(offset, handle);
    return block;
}

void BlockPrefetcher::WriteBack()
{
    mPrefetchFuture.wait();
    vector<BlockHandle>& handles = mPrefetchFuture.value();
    if (handles.empty())
    {
        return;
    }
    for (size_t i = 0; i < handles.size(); i ++)
    {
        mHandles.emplace_back(std::move(handles[i]));
    }
    mPrefetchFuture = future_lite::makeReadyFuture(vector<BlockHandle>());
}

void BlockPrefetcher::Prefetch(size_t fileOffset, size_t maxOffset, size_t prefetchBlockCount)
{
    BlockFileAccessor::FileBlockMeta blockMeta;
    mAccessor->GetBlockMeta(fileOffset, blockMeta);
    size_t blockOffset = blockMeta.blockOffset;
    size_t blockSize = mAccessor->GetBlockSize();
    size_t blockEndIdx = (maxOffset + blockSize - 1) / blockSize;
    size_t blockIdx = mAccessor->GetBlockIdx(blockOffset);
    if (blockIdx + 1 >= blockEndIdx)
    {
        mPrefetchingBlockEnd = blockIdx + 1;
        return;
    }
    prefetchBlockCount = std::min(prefetchBlockCount, blockEndIdx - blockIdx - 1);
    ReadOption option;
    option.advice = storage::IO_ADVICE_LOW_LATENCY;
    mPrefetchFuture = mAccessor->GetBlocks(blockIdx + 1, prefetchBlockCount, option);
    mPrefetchingBlockEnd = blockIdx + 1 + prefetchBlockCount;
    //mPrefetchingBlockEnd = blockIdx + 1 + FIRST_PRETCH_NUM;
}

std::pair<size_t, size_t> BlockPrefetcher::TEST_GetCacheRange()
{
    return std::make_pair(mStartBlockIdx, mStartBlockIdx + mHandles.size());
}

std::pair<size_t, size_t> BlockPrefetcher::TEST_GetFutureRange()
{
    return std::make_pair(mStartBlockIdx, mPrefetchingBlockEnd);
}

IE_NAMESPACE_END(file_system);

