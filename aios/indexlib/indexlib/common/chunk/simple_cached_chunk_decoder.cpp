#include <pthread.h>
#include <autil/StringUtil.h>
#include "indexlib/common/chunk/simple_cached_chunk_decoder.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, SimpleCachedChunkDecoder);

SimpleCachedChunkDecoder::SimpleCachedChunkDecoder() 
{
}

SimpleCachedChunkDecoder::~SimpleCachedChunkDecoder() 
{
    Reset();
}

void SimpleCachedChunkDecoder::Read(
        FileReader* fileReader, uint64_t offset, Pool* pool,
        ChunkMeta &meta, autil::ConstString &data)
{
    assert(fileReader && fileReader->GetOpenType() == FSOT_BUFFERED);
    pthread_t tid = pthread_self();
    string cacheKey = fileReader->GetPath() + "@" + StringUtil::toString(tid);

    mRWLock.rdlock();
    ChunkInfoMap::iterator iter = mChunkMap.find(cacheKey);
    ChunkInfo* info = (iter != mChunkMap.end()) ? iter->second : NULL;
    mRWLock.unlock();
    if (info == NULL)
    {
        fileReader->Read(&meta, sizeof(meta), offset);
        char* chunkData = (char*)pool->allocate(meta.length);
        fileReader->Read(chunkData, meta.length, offset + sizeof(meta));

        char *dataBuf = new char[meta.length];
        memcpy(dataBuf, chunkData, meta.length);
        unique_ptr<ChunkInfo> info(new ChunkInfo);
        info->chunkData = ConstString(dataBuf, meta.length);
        info->chunkOffset = offset;
        info->chunkMeta = meta;

        mRWLock.wrlock();
        mChunkMap.insert(make_pair(cacheKey, info.release()));
        mRWLock.unlock();
        data = ConstString(chunkData, meta.length);
        return;
    }

    autil::ScopedLock lock(info->mutex);
    if (offset == info->chunkOffset)
    {
        // cache hit
        meta = info->chunkMeta;
        ConstString cacheData = info->chunkData;
        char* chunkData = (char*)pool->allocate(meta.length);
        memcpy(chunkData, cacheData.data(), meta.length);
        data = ConstString(chunkData, meta.length);
        return;
    }
    
    if (offset < info->chunkOffset)
    {
        IE_LOG(ERROR, "read backward last offset[%lu], current offset [%lu]",
               info->chunkOffset, offset);
    }
    
    fileReader->Read(&meta, sizeof(meta), offset);
    char* chunkData = (char*)pool->allocate(meta.length);
    fileReader->Read(chunkData, meta.length, offset + sizeof(meta));

    char* dataBuf = info->chunkData.data();
    uint32_t dataBufLen = info->chunkData.size();
    if (dataBufLen < meta.length)
    {
        delete[] dataBuf;
        dataBuf = new char[meta.length];
        dataBufLen = meta.length;
    }
    
    memcpy(dataBuf, chunkData, meta.length);
    info->chunkData = ConstString(dataBuf, dataBufLen);
    info->chunkOffset = offset;
    info->chunkMeta = meta;
    data = ConstString(chunkData, meta.length);
}

void SimpleCachedChunkDecoder::Reset()
{
    mRWLock.wrlock();
    ChunkInfoMap::iterator iter = mChunkMap.begin();
    for (; iter != mChunkMap.end(); iter++)
    {
        if (iter->second != NULL)
        {
            delete iter->second;
            iter->second = NULL;
        }
    }
    mChunkMap.clear();
    mRWLock.unlock();
}

IE_NAMESPACE_END(common);

