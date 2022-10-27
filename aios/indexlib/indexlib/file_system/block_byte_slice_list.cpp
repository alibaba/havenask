#include <algorithm>
#include "indexlib/file_system/block_byte_slice_list.h"
#include "indexlib/util/cache/block_cache.h"
#include "indexlib/util/cache/block.h"
#include "indexlib/util/cache/block_handle.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BlockByteSliceList);

BlockByteSliceList::BlockByteSliceList(BlockFileAccessor* accessor) 
    : mAccessor(accessor)
    , mBlockCache(accessor->GetBlockCache())
    , mPrefetcher(accessor)
{
    IE_SUB_CLASS_TYPE_SETUP(ByteSliceList, BlockByteSliceList);
}

BlockByteSliceList::~BlockByteSliceList()
{
    Clear(NULL);
}

void BlockByteSliceList::AddBlock(size_t fileOffset, size_t dataSize)
{
    ByteSlice *byteSlice = ByteSlice::CreateObject(0);
    byteSlice->next = NULL;
    byteSlice->data = NULL;
    byteSlice->size = dataSize;
    byteSlice->dataSize = 0;
    byteSlice->offset = fileOffset;
    Add(byteSlice);
}

void BlockByteSliceList::Clear(autil::mem_pool::Pool* pool)
{
    ReleaseBlocks();
    ByteSlice* slice = GetHead();
    ByteSlice* next = NULL;
    while(slice)
    {
        //set size = 0 to make the slice 
        //             be destroyed via base class
        slice->size = 0;
        slice->data = NULL;
        slice->dataSize = 0;
        slice->offset = 0;
        next = slice->next;

        ByteSlice::DestroyObject(slice, pool);
        slice = next;
    }
    
    mHead = mTail = NULL;
    mTotalSize = 0;    
}

util::ByteSlice* BlockByteSliceList::GetNextSlice(util::ByteSlice* slice) {
    if (!slice)
    {
        return nullptr;
    }
    if (slice->dataSize == slice->size)
    {
        return slice->next;
    }
    return GetSlice(slice->offset + slice->dataSize, slice);
}

ByteSlice* BlockByteSliceList::GetSlice(size_t fileOffset, ByteSlice* slice)
{
    if (!slice)
    {
        return nullptr;
    }
    assert(fileOffset >= slice->offset && fileOffset < slice->offset + slice->size);
    assert(slice->dataSize <= slice->size);

    if (fileOffset >= slice->offset + slice->dataSize)
    {
        BlockFileAccessor::FileBlockMeta blockMeta;
        mAccessor->GetBlockMeta(fileOffset, blockMeta);
        Cache::Handle* handle = nullptr;
        Block* block = mPrefetcher.ReadAhead(blockMeta.blockOffset, slice->offset + slice->size, &handle);
        assert(block);
        mHandles.push_back(handle);

        ByteSlice* retSlice = nullptr;
        if (blockMeta.blockOffset <= slice->offset)
        {
            auto dataOffset = slice->offset - blockMeta.blockOffset;
            slice->data = block->data + dataOffset;
            slice->dataSize = blockMeta.blockDataSize - dataOffset;
            retSlice = slice;
        }
        else
        {
            auto newSlice = ByteSlice::CreateObject(0);
            newSlice->offset = blockMeta.blockOffset;
            newSlice->size = slice->offset + slice->size - newSlice->offset;
            newSlice->data = block->data;
            newSlice->dataSize = blockMeta.blockDataSize;
            newSlice->next = slice->next;
            slice->size = slice->size - newSlice->size;
            slice->next = newSlice;
            if (mTail == slice)
            {
                mTail = newSlice;
            }
            retSlice = newSlice;
        }
        retSlice->dataSize = std::min(
            static_cast<size_t>(retSlice->dataSize), static_cast<size_t>(retSlice->size));
        return retSlice;
    }
    mPrefetcher.ReadAhead(fileOffset, slice->offset + slice->size);
    return slice;
}

void BlockByteSliceList::ReleaseBlocks()
{
    for (size_t i = 0; i < mHandles.size(); ++i)
    {
        mBlockCache->ReleaseHandle(mHandles[i]);
    }
    mHandles.clear();
}


IE_NAMESPACE_END(file_system);

