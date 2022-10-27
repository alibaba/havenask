#ifndef __INDEXLIB_BLOCK_BYTE_SLICE_LIST_H
#define __INDEXLIB_BLOCK_BYTE_SLICE_LIST_H

#include <vector>
#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/byte_slice_list/byte_slice_list.h"
#include "indexlib/util/cache/cache.h"
#include "indexlib/file_system/block_file_accessor.h"
#include "indexlib/file_system/block_prefetcher.h"

DECLARE_REFERENCE_CLASS(file_system, BlockByteSliceList);

IE_NAMESPACE_BEGIN(util);
struct Block;
class BlockCache;
IE_SUB_CLASS_TYPE_DECLARE_WITH_NS(ByteSliceList, BlockByteSliceList, file_system);
IE_NAMESPACE_END(util);

IE_NAMESPACE_BEGIN(file_system);

class BlockByteSliceList final : public util::ByteSliceList 
{
public:
    BlockByteSliceList(BlockFileAccessor* accessor);
    ~BlockByteSliceList();

public:
    void AddBlock(size_t fileOffset, size_t dataSize);
    void Clear(autil::mem_pool::Pool* pool) override;
    util::ByteSlice* GetNextSlice(util::ByteSlice* slice);
    util::ByteSlice* GetSlice(size_t offset, util::ByteSlice* slice);
private:
    void ReleaseBlocks();
private:
    BlockFileAccessor* mAccessor;
    util::BlockCache* mBlockCache;
    BlockPrefetcher mPrefetcher;
    std::vector<util::Cache::Handle*> mHandles;
private:
    friend class BlockByteSliceListTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BlockByteSliceList);


IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BLOCK_BYTE_SLICE_LIST_H
