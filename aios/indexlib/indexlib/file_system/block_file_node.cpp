#include "autil/TimeUtility.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/block_byte_slice_list.h"
#include "indexlib/file_system/block_file_node.h"
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/util/cache/block_cache.h"
#include "indexlib/util/cache/block_allocator.h"

using namespace std;
using namespace fslib;
using namespace autil;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BlockFileNode);

BlockFileNode::BlockFileNode(util::BlockCache* blockCache,
                             bool useDirectIO, bool cacheDecompressFile)
    : mAccessor(blockCache, useDirectIO)
    , mCacheDecompressFile(cacheDecompressFile)
{
}

BlockFileNode::~BlockFileNode() 
{
}

void BlockFileNode::DoOpen(const string& path, FSOpenType openType)
{
    // TODO: check GetPath()/path in mAccessor
    mAccessor.Open(path);
}

void BlockFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType)
{
    mAccessor.Open(GetPath(), packageOpenMeta);
}

void BlockFileNode::DoOpen(const string& path, const FileMeta& fileMeta,
                           FSOpenType openType)
{
    mAccessor.Open(path, fileMeta);
}

FSFileType BlockFileNode::GetType() const
{
    return FSFT_BLOCK;
}

size_t BlockFileNode::GetLength() const
{
    return mAccessor.GetFileLength();
}

void* BlockFileNode::GetBaseAddress() const
{
    IE_LOG(DEBUG, "not supported GetBaseAddress");
    return NULL;
}

size_t BlockFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option) {
    if ((offset + length) > mAccessor.GetFileLength())
    {
        INDEXLIB_FATAL_ERROR(OutOfRange,
            "read file out of range, offset: [%lu], "
            "read length: [%lu], file length: [%lu]",
            offset, length, mAccessor.GetFileLength());
    }
    option.advice = storage::IO_ADVICE_LOW_LATENCY;    
    return mAccessor.Read(buffer, length, offset, option);    
}

future_lite::Future<size_t>
BlockFileNode::ReadAsync(void* buffer, size_t length, size_t offset, ReadOption option)
{
    if ((offset + length) > mAccessor.GetFileLength())
    {
        try
        {
            INDEXLIB_FATAL_ERROR(OutOfRange, "read file out of range, offset: [%lu], "
                                 "read length: [%lu], file length: [%lu]",
                                 offset, length, mAccessor.GetFileLength());
        }
        catch (...)
        {
            return future_lite::makeReadyFuture<size_t>(std::current_exception());
        }

    }

    return mAccessor.ReadAsync(buffer, length, offset, option);
}

ByteSliceList* BlockFileNode::Read(size_t length, size_t offset, ReadOption option)
{
    if (offset + length > mAccessor.GetFileLength())
    {
        INDEXLIB_FATAL_ERROR(OutOfRange, "read file out of range, offset: [%lu], "
                             "read length: [%lu], file length: [%lu]",
                             offset, length, mAccessor.GetFileLength());
    }

    BlockByteSliceList* sliceList = new BlockByteSliceList(&mAccessor);
    auto sliceLen = std::min(length, mAccessor.GetFileLength() - offset);
    sliceList->AddBlock(offset, sliceLen);
    return sliceList;
}

size_t BlockFileNode::Write(const void* buffer, size_t length)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "not supported Write");
    return 0;
}

void BlockFileNode::Close()
{
    mAccessor.Close();
}

size_t BlockFileNode::Prefetch(size_t length, size_t offset, ReadOption option)
{
    size_t fileLength = mAccessor.GetFileLength();
    if (offset + length > fileLength)
    {
        if (unlikely(offset >= fileLength))
        {
            return 0;
        }
        length = fileLength - offset;
    }
    return mAccessor.Prefetch(length, offset, option);
}

future_lite::Future<uint32_t> BlockFileNode::DoReadUInt32Async(
    size_t offset,  size_t leftBytes, uint32_t currentValue, ReadOption option)
{
    assert(leftBytes <= sizeof(uint32_t));
    return mAccessor.GetBlockAsync(offset, option)
        .thenValue([this, offset, leftBytes, currentValue, option](BlockHandle handle) mutable {
            auto data = handle.GetData();
            size_t blockSize = handle.GetDataSize();
            size_t inBlockOffset = offset - handle.GetOffset();
            assert(inBlockOffset < blockSize);
            size_t copySize = std::min(blockSize - inBlockOffset, leftBytes);
            memcpy(reinterpret_cast<char*>(&currentValue) + sizeof(uint32_t) - leftBytes,
                data + inBlockOffset, copySize);
            if (copySize != leftBytes)
            {
                return DoReadUInt32Async(offset + copySize, leftBytes - copySize, currentValue, option);
            }
            return future_lite::makeReadyFuture<uint32_t>(currentValue);
        });
}

future_lite::Future<uint32_t> BlockFileNode::ReadUInt32Async(size_t offset, ReadOption option)
{
    if (unlikely((offset + sizeof(uint32_t)) > mAccessor.GetFileLength()))
    {
        try
        {
            INDEXLIB_FATAL_ERROR(OutOfRange, "read file out of range, offset: [%lu], "
                                 "read length: 4, file length: [%lu]",
                                 offset, mAccessor.GetFileLength());
        }
        catch (...)
        {
            return future_lite::makeReadyFuture<uint32_t>(std::current_exception());
        }
    }
    return DoReadUInt32Async(offset, sizeof(uint32_t), 0, option);
}

IE_NAMESPACE_END(file_system);

