#ifndef __INDEXLIB_BLOCK_FILE_ACCESSOR_H
#define __INDEXLIB_BLOCK_FILE_ACCESSOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/cache/block_cache.h"
#include "indexlib/util/cache/block_handle.h"
#include "indexlib/util/cache/block_allocator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/package_file_meta.h"
#include "indexlib/file_system/package_open_meta.h"
#include "indexlib/storage/file_wrapper.h"
#include <autil/TimeUtility.h>
#include <future_lite/Future.h>
#include "indexlib/util/future_executor.h"

IE_NAMESPACE_BEGIN(file_system);

class BlockFileAccessor
{
private:
    template<typename T>
    using Future = future_lite::Future<T>;
private:
    struct ReadContext
    {
        uint8_t* curBuffer = nullptr;
        size_t leftLen = 0;
        size_t curOffset = 0;
        bool async = false;

        ReadContext(uint8_t *buffer, size_t len, size_t offset, bool asyncMode)
            : curBuffer(buffer)
            , leftLen(len)
            , curOffset(offset)
            , async(asyncMode)
        {
        }
    };

    using ReadContextPtr = std::shared_ptr<ReadContext>;

public:
    struct FileBlockMeta
    {
        size_t blockOffset;
        size_t inBlockOffset;
        size_t blockDataSize;

        FileBlockMeta(size_t _blockOffset, size_t _inBlockOffset, size_t _blockDataSize)
            : blockOffset(_blockOffset)
            , inBlockOffset(_inBlockOffset)
            , blockDataSize(_blockDataSize)
        {}
        FileBlockMeta()
            : blockOffset(0)
            , inBlockOffset(0)
            , blockDataSize(0)
        {}
    };
    
public:
    BlockFileAccessor(util::BlockCache* blockCache, bool useDirectIO)
        : mBlockCache(blockCache)
        , mFileId(0)
        , mFileLength(0)
        , mFileBeginOffset(0)
        , mBlockSize(0)
        , mUseDirectIO(useDirectIO)
        , mExecutor(util::FutureExecutor::GetInternalExecutor())
    {
        assert(mBlockCache);
        mBlockAllocatorPtr = blockCache->GetBlockAllocator();
        assert(mBlockAllocatorPtr);
        mBlockSize = mBlockCache->GetBlockSize();
    }
    
    ~BlockFileAccessor()
    {
        try
        {
            Close(); 
        }
        catch(...)
        {}
    }
    
public:
    void Open(const std::string& path);
    
    void Open(const std::string& path, const PackageOpenMeta& packageOpenMeta);

    void Open(const std::string& path, const fslib::FileMeta& fileMeta);

    void Close();

    uint64_t GetFileLength() const { return mFileLength; }
    util::BlockCache* GetBlockCache() const { return mBlockCache; }

    size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption());
    Future<size_t> ReadAsync(void* buffer, size_t length, size_t offset, ReadOption option);
    Future<std::vector<util::BlockHandle>> GetBlocks(size_t blockIdx, size_t blockCount, ReadOption option);
    size_t Prefetch(size_t length, size_t offset, ReadOption option = ReadOption());

    bool GetBlockMeta(size_t offset, FileBlockMeta& meta);

    bool GetBlock(size_t offset, util::BlockHandle& handle, ReadOption* option = nullptr);
    util::Block* GetBlock(size_t offset, util::Cache::Handle** handle);
    Future<util::BlockHandle> GetBlockAsync(size_t offset, ReadOption option);

    size_t GetBlockCount(size_t offset, size_t length) const
    {
        size_t blockOffset = GetBlockOffset(offset);
        return ((length + offset - blockOffset) + mBlockSize - 1) / mBlockSize;
    }

    size_t GetBlockOffset(size_t offset) const
    { return offset - (offset % mBlockSize); }

    size_t GetBlockIdx(size_t offset) const
    { return (offset / mBlockSize); }

    size_t GetInBlockOffset(size_t offset) const
    { return offset % mBlockSize; }

    size_t GetBlockSize() const
    { return mBlockSize; }
    
    const char* GetFilePath() const
    { return mFilePtr ? mFilePtr->GetFileName() : NULL; }

    uint64_t GetFileId() const { return mFileId; }

    void ReadBlockFromFileToCache(util::Block* block, uint64_t blockOffset, util::Cache::Handle** handle)
    {
        ReadOption option;
        option.advice = storage::IO_ADVICE_LOW_LATENCY;
        *handle = ReadBlockFromFileToCache(block, blockOffset, false, option).get();
    }


public:
    // for test
    void TEST_SetFile(storage::FileWrapperPtr file)
    { mFilePtr = file; }

private:
    size_t DoRead(void* buffer, size_t length, size_t offset, ReadOption option);
    Future<std::pair<util::Block*, util::Cache::Handle*>>
    DoGetBlock(const util::blockid_t& blockID, uint64_t offset, bool async, ReadOption option);

    Future<util::BlockHandle> DoGetBlock(size_t offset, bool async, ReadOption option);

    Future<std::vector<util::BlockHandle>> DoGetBlocks(
        size_t blockIdx, size_t endBlockIdx,
        std::vector<util::BlockHandle>&& handles, ReadOption option);
    Future<std::vector<util::BlockHandle>> DoGetBlocksCallback(
        size_t startMissBlock, size_t endMissBlock, size_t endBlockIdx,
        util::Block* block, util::Cache::Handle* cacheHandle,
        std::vector<util::BlockHandle>&& handles, ReadOption option);    
    Future<std::vector<util::BlockHandle>>
        GetBlockHandles(size_t blockInFileIdx, size_t blockCount, bool async, ReadOption option);    

    void FillBuffer(const util::BlockHandle& handle,
                    const ReadContextPtr& ctx);
    Future<future_lite::Unit> FillBuffer(size_t startBlockIdx, size_t cnt,
                                           const ReadContextPtr& ctx, ReadOption option);

    Future<size_t> ReadFromBlock(const ReadContextPtr& ctx, ReadOption option);

    Future<util::Cache::Handle*> ReadBlockFromFileToCache(util::Block* block, uint64_t blockOffset, bool async,
                                                          ReadOption option);

private:
    static const size_t BATCH_READ_BLOCKS = 4;

private:
    util::BlockCache* mBlockCache;
    util::BlockAllocatorPtr mBlockAllocatorPtr;
    storage::FileWrapperPtr mFilePtr;
    uint64_t mFileId;
    uint64_t mFileLength;
    size_t mFileBeginOffset;
    uint32_t mBlockSize;
    bool mUseDirectIO;

    future_lite::Executor* mExecutor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BlockFileAccessor);
///////////////////////////////////////////////////////////////////////

// to guarantee no memory leak when exception
struct FreeBlockWhenException
{
    FreeBlockWhenException(util::Block* block_, util::BlockAllocator* allocator_)
        : allocator(allocator_)
        , block(block_)
    {}
    ~FreeBlockWhenException()
    {
        if (unlikely(block != NULL))
        {
            allocator->FreeBlock(block); // meeting a exception
        }
    }
    util::BlockAllocator* allocator;
    util::Block* block;

    FreeBlockWhenException(const FreeBlockWhenException&) = delete;
    FreeBlockWhenException& operator=(const FreeBlockWhenException&) = delete;

    FreeBlockWhenException(FreeBlockWhenException&& other)
        : allocator(other.allocator)
        , block(other.block)
    {
        other.allocator = nullptr;
        other.block = nullptr;
    }
    FreeBlockWhenException& operator=(FreeBlockWhenException&& other)
    {
        std::swap(allocator, other.allocator);
        std::swap(block, other.block);
        return *this;
    }
};

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BLOCK_FILE_ACCESSOR_H
