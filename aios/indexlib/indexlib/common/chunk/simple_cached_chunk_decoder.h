#ifndef __INDEXLIB_SIMPLE_CACHED_CHUNK_DECODER_H
#define __INDEXLIB_SIMPLE_CACHED_CHUNK_DECODER_H

#include <tr1/memory>
#include <tr1/unordered_map>
#include <autil/Lock.h>
#include <autil/ConstString.h>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/common/chunk/chunk_define.h"

IE_NAMESPACE_BEGIN(common);

class SimpleCachedChunkDecoder
{
public:
    SimpleCachedChunkDecoder();
    ~SimpleCachedChunkDecoder();
    
public:
    void Read(file_system::FileReader* fileReader,
              uint64_t offset, autil::mem_pool::Pool* pool,
              ChunkMeta &meta, autil::ConstString &data);

    void Reset();
    
private:
    struct ChunkInfo
    {
        ChunkInfo()
            : chunkOffset(0)
            , chunkData(autil::ConstString::EMPTY_STRING)
        {}

        ~ChunkInfo()
        {
            mutex.lock();
            if (chunkData.data() != NULL)
            {
                char* data = chunkData.data();
                delete[] data;
                chunkData = autil::ConstString::EMPTY_STRING;
            }
            mutex.unlock();
        }
        uint64_t chunkOffset;
        ChunkMeta chunkMeta;
        autil::ConstString chunkData;
        autil::ThreadMutex mutex;
    };

private:
    typedef std::tr1::unordered_map<std::string, ChunkInfo*> ChunkInfoMap;
    ChunkInfoMap mChunkMap;
    autil::ReadWriteLock mRWLock;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SimpleCachedChunkDecoder);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SIMPLE_CACHED_CHUNK_DECODER_H
