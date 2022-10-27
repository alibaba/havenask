#include "indexlib/file_system/block_cache_compress_file_reader.h"
#include "indexlib/util/cache/block_allocator.h"
#include "indexlib/file_system/file_block_cache.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BlockCacheCompressFileReader);

bool BlockCacheCompressFileReader::Init(const FileReaderPtr& fileReader,
                                        const CompressFileInfo& compressInfo, Directory* directory)
{
    if (!CompressFileReader::Init(fileReader, compressInfo, directory))
    {
        return false;
    }

    mBlockFileNode = DYNAMIC_POINTER_CAST(BlockFileNode, fileReader->GetFileNode());
    assert(mBlockFileNode);
    assert(compressInfo.blockSize == (size_t)mBlockFileNode->GetBlockCache()->GetBlockSize());
    string newPath = fileReader->GetPath() + "@decompress_in_cache";
    mFileId = FileBlockCache::GetFileId(newPath);
    return true;
}

BlockCacheCompressFileReader* BlockCacheCompressFileReader::CreateSessionReader(
        Pool* pool)
{
    assert(mDataFileReader);
    assert(mCompressAddrMapper);
    BlockCacheCompressFileReader* cloneReader = IE_POOL_COMPATIBLE_NEW_CLASS(
            pool, BlockCacheCompressFileReader, pool);
    cloneReader->DoInit(mDataFileReader, mCompressAddrMapper, mCompressInfo);
    cloneReader->mBlockFileNode = mBlockFileNode;
    cloneReader->mFileId = mFileId;
    return cloneReader;
}

void BlockCacheCompressFileReader::LoadBuffer(size_t offset, ReadOption option)
{
    assert(!InCurrentBlock(offset));
    assert(mDataFileReader);
    mCompressor->Reset();

    size_t blockIdx = mCompressAddrMapper->OffsetToBlockIdx(offset);
    blockid_t blockId(mFileId, blockIdx);
    BlockCache* blockCache = mBlockFileNode->GetBlockCache();
    size_t blockSize = blockCache->GetBlockSize();
    
    Cache::Handle* handle = NULL;
    Block* block = blockCache->Get(blockId, &handle);
    if (NULL == block)
    {
        // block cache miss
        DecompressBuffer(blockIdx, option);
        PutInCache(blockCache, blockId, &handle);
    }
    else
    {
        // block cache hit
        size_t bufferLen = ((blockIdx + 1) == mCompressAddrMapper->GetBlockCount()) ?
                           (GetUncompressedFileLength() - blockIdx * blockSize) : blockSize;
        assert(bufferLen <= blockSize);
        assert(block->id == blockId);
        DynamicBuf& outBuffer = mCompressor->GetOutBuffer();
        memcpy(outBuffer.getBuffer(), block->data, bufferLen);
        outBuffer.movePtr(bufferLen);
    }
    blockCache->ReleaseHandle(handle);
    mCurBlockIdx = blockIdx;
}

void BlockCacheCompressFileReader::DecompressBuffer(size_t blockIdx, ReadOption option)
{
    size_t compressBlockOffset = mCompressAddrMapper->CompressBlockAddress(blockIdx);
    size_t compressBlockSize = mCompressAddrMapper->CompressBlockLength(blockIdx);
    DynamicBuf& inBuffer = mCompressor->GetInBuffer();
    if (compressBlockSize != mBlockFileNode->Read(
            inBuffer.getBuffer(), compressBlockSize, compressBlockOffset, option))
    {
        INDEXLIB_FATAL_ERROR(FileIO,
                             "read compress file[%s] failed, offset [%lu], compress len [%lu]", 
                             mDataFileReader->GetPath().c_str(),
                             compressBlockOffset, compressBlockSize);
        return;
    }
    
    inBuffer.movePtr(compressBlockSize);
    if (!mCompressor->Decompress(mCompressInfo.blockSize))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "decompress file [%s] failed, offset [%lu], compress len [%lu]", 
                             mDataFileReader->GetPath().c_str(),
                             compressBlockOffset, compressBlockSize);
        return;
    }
}

void BlockCacheCompressFileReader::PutInCache(
        BlockCache* blockCache, const blockid_t& blockId, Cache::Handle **handle)
{
    assert(blockCache);
    assert(mCompressor->GetBufferOutLen() <= blockCache->GetBlockSize());

    const BlockAllocatorPtr& blockAllocator = blockCache->GetBlockAllocator();
    Block* block = blockAllocator->AllocBlock();
    block->id = blockId;
    FreeBlockWhenException freeBlockWhenException(block, blockAllocator.get());
    memcpy(block->data, mCompressor->GetBufferOut(), mCompressor->GetBufferOutLen());
    bool ret = blockCache->Put(block, handle);
    assert(ret); (void)ret;
    freeBlockWhenException.block = NULL; // normal return
}

IE_NAMESPACE_END(file_system);

