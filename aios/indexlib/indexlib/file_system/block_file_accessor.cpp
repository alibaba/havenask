#include "indexlib/file_system/block_file_accessor.h"
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/file_system/read_option.h"

using namespace std;
using namespace autil;

using future_lite::Future;
using future_lite::Promise;
using future_lite::Try;
using future_lite::MoveWrapper;
using future_lite::Unit;
using future_lite::makeReadyFuture;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BlockFileAccessor);

void BlockFileAccessor::Open(const string& path)
{
    mFileId = FileBlockCache::GetFileId(path);
    mFileLength = FileSystemWrapper::GetFileLength(path);
    mFilePtr.reset(FileSystemWrapper::OpenFile(path, fslib::READ, mUseDirectIO, mFileLength));
    mFileBeginOffset = 0;
}
    
void BlockFileAccessor::Open(const string& path,
                             const PackageOpenMeta& packageOpenMeta)
{
    mFileId = FileBlockCache::GetFileId(path);
    mFilePtr.reset(FileSystemWrapper::OpenFile(
                       packageOpenMeta.GetPhysicalFilePath(), fslib::READ, mUseDirectIO,
                       packageOpenMeta.GetPhysicalFileLength()));
    mFileLength = packageOpenMeta.GetLength();
    mFileBeginOffset = packageOpenMeta.GetOffset();
}

void BlockFileAccessor::Open(const string& path,
                             const fslib::FileMeta& fileMeta)
{
    mFileId = FileBlockCache::GetFileId(path);
    mFileLength = fileMeta.fileLength;
    mFilePtr.reset(FileSystemWrapper::OpenFile(path, fslib::READ, mUseDirectIO, mFileLength));
    mFileBeginOffset = 0;
}

void BlockFileAccessor::Close()
{
    if (mFilePtr)
    {
        mFilePtr->Close();
        mFilePtr.reset();
    }
}

void BlockFileAccessor::FillBuffer(const util::BlockHandle& handle,
    const ReadContextPtr& ctx)
{
    assert(ctx);
    size_t inBlockOffset = GetInBlockOffset(ctx->curOffset);
    size_t blockDataSize = handle.GetDataSize();
    size_t curReadBytes = inBlockOffset + ctx->leftLen > blockDataSize ?
                          blockDataSize - inBlockOffset : ctx->leftLen;
    if (ctx->curBuffer)
    {
        memcpy(ctx->curBuffer, handle.GetData() + inBlockOffset, curReadBytes);
        ctx->curBuffer += curReadBytes;
    }
    ctx->curOffset += curReadBytes;
    ctx->leftLen -= curReadBytes;
}

Future<Unit> BlockFileAccessor::FillBuffer(size_t startBlockIdx, size_t cnt,
                                           const ReadContextPtr& ctx, ReadOption option)
{
    if (cnt == 0)
    {
        return makeReadyFuture(Unit());
    }

    assert(ctx->curOffset / mBlockSize == startBlockIdx);

    return GetBlockHandles(startBlockIdx, cnt, ctx->async, option)
        .thenValue([ctx, this](vector<BlockHandle>&& handles) mutable {
            for (const auto& handle : handles)
            {
                FillBuffer(handle, ctx);
            }
        });
}
size_t BlockFileAccessor::Read(void* buffer, size_t length, size_t offset, ReadOption option)
{
    size_t batchSize = mBlockSize * BATCH_READ_BLOCKS;
    size_t readed = 0;
    while (length > 0)
    {
        size_t readLen = std::min(length, batchSize);
        auto ret = DoRead(buffer, readLen, offset, option);

        readed += ret;
        if (buffer)
        {
            buffer = static_cast<char*>(buffer) + ret;
        }
        length -= ret;
        offset += ret;

        if (ret != readLen)
        {
            return readed;
        }
    }
    return readed;
}

size_t BlockFileAccessor::DoRead(void* buffer, size_t length, size_t offset, ReadOption option)
{
    auto ctx = make_shared<ReadContext>(static_cast<uint8_t*>(buffer), length, offset, false);
    return ReadFromBlock(ctx, option).get();
}

Future<size_t> BlockFileAccessor::ReadAsync(void* buffer, size_t length, size_t offset, ReadOption option)
{
    auto ctx = make_shared<ReadContext>(static_cast<uint8_t*>(buffer), length, offset, true);
    return ReadFromBlock(ctx, option);
}

Future<vector<BlockHandle>> BlockFileAccessor::GetBlocks(size_t blockIdx, size_t blockCount, ReadOption option)
{
    size_t endBlockIdx = std::min((mFileLength + mBlockSize - 1) / mBlockSize, blockIdx + blockCount);
    if (blockCount == 0 || blockIdx >= endBlockIdx)
    {
        return makeReadyFuture(vector<BlockHandle>());
    }
    blockCount = endBlockIdx - blockIdx;
    vector<BlockHandle> handles;
    handles.reserve(blockCount);
    mBlockCache->ReportPrefetchCount(blockCount);
    return DoGetBlocks(blockIdx, endBlockIdx, std::move(handles), option);
}

Future<vector<BlockHandle>> BlockFileAccessor::DoGetBlocks(size_t blockIdx, size_t endBlockIdx,
        vector<BlockHandle>&& handles, ReadOption option)
{
    size_t curBlockIdx = blockIdx;
    size_t nxtMissBlock = curBlockIdx;
    for (; curBlockIdx < endBlockIdx; ++curBlockIdx)
    {
        blockid_t blockId(mFileId, curBlockIdx);
        Cache::Handle* cacheHandle = NULL;
        Block* block = mBlockCache->Get(blockId, &cacheHandle);
        if (block && curBlockIdx > nxtMissBlock)
        {
            return DoGetBlocksCallback(nxtMissBlock, curBlockIdx, endBlockIdx, block, cacheHandle, std::move(handles), option);
        }
        else if (!block && curBlockIdx >= nxtMissBlock + BATCH_READ_BLOCKS - 1)
        {
            return DoGetBlocksCallback(nxtMissBlock, curBlockIdx + 1, endBlockIdx, nullptr, nullptr, std::move(handles), option);
        }
        if (block) {
            nxtMissBlock ++;
            size_t blockDataSize = std::min((size_t)mBlockSize, mFileLength - curBlockIdx * mBlockSize);
            handles.emplace_back(mBlockCache, cacheHandle, block, blockDataSize);
        }
    }
    if (curBlockIdx > nxtMissBlock)
    {
        return DoGetBlocksCallback(nxtMissBlock, curBlockIdx, endBlockIdx, nullptr, nullptr, std::move(handles), option);
    }
    return makeReadyFuture(std::move(handles));
}

Future<vector<BlockHandle>> BlockFileAccessor::DoGetBlocksCallback(
        size_t startMissBlock, size_t endMissBlock, size_t endBlockIdx,
        Block* block, Cache::Handle* cacheHandle,
        vector<BlockHandle>&& handles, ReadOption option)
{
    return GetBlockHandles(startMissBlock, endMissBlock - startMissBlock, true, option)
        .thenValue([this, startMissBlock, endMissBlock, endBlockIdx, option, block, cacheHandle, handles = std::move(handles)]
                   (vector<BlockHandle>&& results) mutable {
                    for (size_t i = 0; i < results.size(); i ++)
                    {
                        handles.push_back(std::move(results[i]));
                    }
                    if (block && cacheHandle)
                    {
                        size_t blockDataSize = std::min((size_t)mBlockSize,
                                mFileLength - endMissBlock * mBlockSize);
                        handles.emplace_back(mBlockCache, cacheHandle, block, blockDataSize);
                        endMissBlock ++;
                    }
                    if (endBlockIdx > endMissBlock)
                    {
                        return DoGetBlocks(endMissBlock, endBlockIdx, std::move(handles), option);
                    }
                    return makeReadyFuture(std::move(handles));
                });    
}

Future<size_t> BlockFileAccessor::ReadFromBlock(const ReadContextPtr& ctx, ReadOption option)
{
    if (ctx->leftLen == 0)
    {
        return makeReadyFuture((size_t)0);
    }

    size_t startOffset = ctx->curOffset;

    // [start, end)
    size_t startBlockIdx = ctx->curOffset / mBlockSize;
    size_t endBlockIdx = (ctx->curOffset + ctx->leftLen + mBlockSize - 1) / mBlockSize;

    std::function<void(const ReadContextPtr)> deferWork;

    size_t curBlockIdx = startBlockIdx;
    size_t blockCount = endBlockIdx - curBlockIdx;

    for (; curBlockIdx < endBlockIdx; ++curBlockIdx)
    {
        blockid_t blockId(mFileId, curBlockIdx);
        Cache::Handle* cacheHandle = NULL;
        Block* block = mBlockCache->Get(blockId, &cacheHandle);
        if (block)
        {
            mBlockCache->ReportHit(option.blockCounter);

            deferWork = [this, curBlockIdx, cacheHandle, block, option](const ReadContextPtr& ctx) mutable {
                auto blockDataSize = min(mFileLength - curBlockIdx * mBlockSize, (size_t)mBlockSize);
                FillBuffer(BlockHandle(mBlockCache, cacheHandle, block, blockDataSize), ctx);
            };
            blockCount = curBlockIdx - startBlockIdx;
            break;
        }
        else
        {
            mBlockCache->ReportMiss(option.blockCounter);
            blockCount = curBlockIdx - startBlockIdx + 1; // +1 for current
            if (blockCount >= BATCH_READ_BLOCKS)
            {
                break;
            }
        }
    }
    return FillBuffer(startBlockIdx, blockCount, ctx, option)
        .thenValue([this, option, ctx, startOffset, defer = std::move(deferWork)](Unit u) mutable {
            if (defer)
            {
                defer(ctx);
            }
            return ReadFromBlock(ctx, option).thenValue([ctx, startOffset] (size_t readSize) {
                return ctx->curOffset - startOffset;
            });
        });
}

size_t BlockFileAccessor::Prefetch(size_t length, size_t offset, ReadOption option)
{
    return Read(NULL, length, offset, option);
}

bool BlockFileAccessor::GetBlockMeta(size_t offset, BlockFileAccessor::FileBlockMeta& meta)
{
    if (offset >= mFileLength)
    {
        return false;
    }
    meta.blockOffset = GetBlockOffset(offset);
    meta.inBlockOffset = GetInBlockOffset(offset);
    meta.blockDataSize = min(mFileLength - meta.blockOffset, (size_t)mBlockSize);
    return true;
}

Future<vector<BlockHandle>> BlockFileAccessor::GetBlockHandles(
    size_t blockInFileIdx, size_t blockCount,  bool async, ReadOption option)
{
    if (blockCount == 0)
    {
        return makeReadyFuture(vector<BlockHandle>());
    }
    typedef vector<Block*> BlockArray;
    std::unique_ptr<BlockArray, std::function<void(BlockArray*)>> exceptionHandler(
          new BlockArray(blockCount, nullptr), [this](BlockArray* blocks) {
            for (Block* b : *blocks)
            {
                mBlockAllocatorPtr->FreeBlock(b);
            }
            delete blocks;
       });
    std::vector<iovec> iov(blockCount);
    for (size_t i = 0; i < blockCount; ++i)
    {
        (*exceptionHandler)[i] = mBlockAllocatorPtr->AllocBlock();
        (*exceptionHandler)[i]->id = util::blockid_t(mFileId, blockInFileIdx + i);
        iov[i].iov_base = (*exceptionHandler)[i]->data;
        iov[i].iov_len = mBlockSize;
    }
    uint64_t begin = autil::TimeUtility::currentTimeInMicroSeconds();
    auto future = mFilePtr->PReadVAsync(
        iov.data(), blockCount, mFileBeginOffset + blockInFileIdx * mBlockSize, option.advice);
    if (!mExecutor || !async)
    {
        // synchronization mode, otherwise callback will execute in fslib(eg: pangu) callback thread pool,
        //  which is not recommanded
        future.wait();
    }
    return std::move(future).via(mExecutor).thenValue(
        [this, begin, blockCount, option, exHandler = std::move(exceptionHandler),
            ioVector = std::move(iov)](size_t readSize) mutable {
            mBlockCache->ReportReadLatency(
                autil::TimeUtility::currentTimeInMicroSeconds() - begin, option.blockCounter);
            std::vector<util::BlockHandle> handles;
            handles.resize(blockCount);

            mBlockCache->ReportReadBlockCount(blockCount);
            mBlockCache->ReportReadSize(blockCount * mBlockSize, option.blockCounter);
            for (size_t i = 0; i < blockCount; ++i)
            {
                util::Cache::Handle* cacheHandle = NULL;
                bool ret = mBlockCache->Put((*exHandler)[i], &cacheHandle);
                assert(ret);
                (void)ret;
                size_t blockDataSize = mBlockSize;
                if (i == blockCount - 1 && readSize % mBlockSize)
                {
                    blockDataSize = readSize % mBlockSize;
                }
                handles[i].Reset(mBlockCache, cacheHandle, (*exHandler)[i], blockDataSize);
            }
            BlockArray* array = exHandler.release();
            delete array;
            return handles;
        });
}

bool BlockFileAccessor::GetBlock(size_t offset, BlockHandle& handle, ReadOption* option)
{
    ReadOption localOption;
    localOption.advice = storage::IO_ADVICE_LOW_LATENCY;

    ReadOption* optionPtr = option ? option : &localOption;

    auto future = DoGetBlock(offset, false, *optionPtr);
    future.wait();
    auto& tryRet = future.result();
    if (tryRet.hasError())
    {
        return false;
    }
    handle = std::move(tryRet.value());
    return true;
}

Future<BlockHandle> BlockFileAccessor::GetBlockAsync(size_t offset, ReadOption option)
{
    return DoGetBlock(offset, true, option);
}

Block* BlockFileAccessor::GetBlock(size_t offset, Cache::Handle** handle)
{
    if (offset >= mFileLength)
    {
        return NULL;
    }
    ReadOption option;
    option.advice = storage::IO_ADVICE_LOW_LATENCY;
    size_t blockOffset = GetBlockOffset(offset);
    blockid_t blockId(mFileId, blockOffset / mBlockSize);
    auto result =  DoGetBlock(blockId, blockOffset, false, option).get();
    *handle = result.second;
    return result.first;
}


Future<BlockHandle> BlockFileAccessor::DoGetBlock(size_t offset, bool async, ReadOption option)
{
    if (offset >= mFileLength)
    {
        return makeReadyFuture<BlockHandle>(
            std::make_exception_ptr(std::runtime_error("offset >= fileLength")));
    }

    size_t blockOffset = GetBlockOffset(offset);
    blockid_t blockId(mFileId, blockOffset / mBlockSize);
    return DoGetBlock(blockId, blockOffset, async, option)
        .thenValue([blockId, this, blockOffset](std::pair<Block*, Cache::Handle*> v) {
            auto block = v.first;
            auto cacheHandle = v.second;
            assert(block && block->id == blockId);
            size_t blockDataSize = min(mFileLength - blockOffset, (size_t)mBlockSize);
            util::BlockHandle handle;
            handle.Reset(mBlockCache, cacheHandle, block, blockDataSize);
            return handle;
        });
}

Future<std::pair<Block*, Cache::Handle*>> BlockFileAccessor::DoGetBlock(
    const blockid_t& blockID, uint64_t blockOffset, bool async, ReadOption option)
{
    assert(blockOffset % mBlockSize == 0);
    Cache::Handle* handle = nullptr;
    Block* block = mBlockCache->Get(blockID, &handle);

    if (NULL == block)
    {
        block = mBlockAllocatorPtr->AllocBlock();
        block->id = blockID;
        mBlockCache->ReportMiss(option.blockCounter);
        FreeBlockWhenException freeBlockWhenException(block, mBlockAllocatorPtr.get());
        return ReadBlockFromFileToCache(block, blockOffset, async, option)
            .thenValue([block, guard = std::move(freeBlockWhenException)]
                       (Cache::Handle* newHandle) mutable {
                guard.block = NULL; // normal return
                return std::make_pair( block, newHandle );
            });
    }
    else
    {
        assert(block->id == blockID);
        mBlockCache->ReportHit(option.blockCounter);
        return makeReadyFuture(std::make_pair(block, handle));
    }
}

Future<Cache::Handle*> BlockFileAccessor::ReadBlockFromFileToCache(
    Block* block, uint64_t blockOffset, bool async, ReadOption option)
{
    assert(blockOffset % mBlockSize == 0);
    assert(mFilePtr);

    uint64_t begin = TimeUtility::currentTimeInMicroSeconds();

    auto future = mFilePtr->PReadAsync(
        block->data, mBlockSize, blockOffset + mFileBeginOffset, option.advice);
    if (!mExecutor || !async)
    {
        future.wait();
    }
    return std::move(future).via(mExecutor).thenValue([this, begin, block, option](
                                                          size_t readSize) {
        mBlockCache->ReportReadLatency(
            TimeUtility::currentTimeInMicroSeconds() - begin, option.blockCounter);
        mBlockCache->ReportReadSize(mBlockSize, option.blockCounter);
        Cache::Handle* handle = nullptr;
        bool ret = mBlockCache->Put(block, &handle);
        assert(ret);
        (void)ret;
        return handle;
    });
}


IE_NAMESPACE_END(file_system);

