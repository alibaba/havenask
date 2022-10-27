#ifndef __INDEXLIB_BLOCK_PREFETCHER_H
#define __INDEXLIB_BLOCK_PREFETCHER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/cache/cache.h"
#include "indexlib/file_system/block_file_accessor.h"
#include <future_lite/Future.h>

IE_NAMESPACE_BEGIN(file_system);

class BlockPrefetcher
{
public:
    explicit BlockPrefetcher(BlockFileAccessor* accessor);
    //~BlockPrefetcher();

    util::Block* ReadAhead(size_t offset, size_t maxOffset, util::Cache::Handle**);
    void ReadAhead(size_t offset, size_t maxOffset);

private:
    void WriteBack();
    void Prefetch(size_t fileOffset, size_t maxOffset, size_t prefetchBlockCount);
    util::Block* GetBlock(size_t offset, util::Cache::Handle** handle);
public:
    std::pair<size_t, size_t> TEST_GetCacheRange();
    std::pair<size_t, size_t> TEST_GetFutureRange();
private:
    static constexpr size_t INVALID_BLOCK_IDX = std::numeric_limits<size_t>::max();
    size_t mStartBlockIdx;
    size_t mPrefetchingBlockEnd;
    BlockFileAccessor* mAccessor;
    util::BlockCache* mBlockCache;
    bool mUsePrefetcher;
    future_lite::Future<std::vector<util::BlockHandle>> mPrefetchFuture;
    std::vector<util::BlockHandle> mHandles;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BlockPrefetcher);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BLOCK_PREFETCHER_H
