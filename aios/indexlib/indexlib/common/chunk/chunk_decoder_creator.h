#ifndef __INDEXLIB_CHUNK_DECODER_CREATOR_H
#define __INDEXLIB_CHUNK_DECODER_CREATOR_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/byte_slice_list/byte_slice_list.h"
#include "indexlib/common/fixed_size_byte_slice_list_reader.h"
#include "indexlib/common/chunk/chunk_decoder.h"
#include "indexlib/common/chunk/integrated_plain_chunk_decoder.h"
#include "indexlib/common/chunk/slice_plain_chunk_decoder.h"
#include "indexlib/common/chunk/simple_cached_chunk_decoder.h"
#include "indexlib/common/chunk/chunk_define.h"
#include "indexlib/file_system/file_reader.h"


IE_NAMESPACE_BEGIN(common);

class ChunkDecoderCreator
{
public:
    ChunkDecoderCreator();
    ~ChunkDecoderCreator();
    
public:
    // recordLen 0 means record has varied length, for integrated reader
    static ChunkDecoder* Create(const char* baseAddr,
                                uint64_t offset, uint32_t recordLen,
                                autil::mem_pool::Pool* pool);

    // recordLen 0 means record has varied length
    static ChunkDecoder* Create(file_system::FileReader* fileReader,
                                file_system::FSOpenType openType,
                                uint64_t offset, uint32_t recordLen,
                                autil::mem_pool::Pool* pool,
                                file_system::ReadOption option);

    static void Reset()
    { mSimpleCacheDecoder.Reset(); }
    
private:
    static SimpleCachedChunkDecoder mSimpleCacheDecoder;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ChunkDecoderCreator);

//////////////////////////////////////////////////////////////////////
inline ChunkDecoder* ChunkDecoderCreator::Create(
        const char* baseAddr, uint64_t offset,
        uint32_t recordLen, autil::mem_pool::Pool* pool)
{
    const ChunkMeta& meta = *(ChunkMeta*)(baseAddr + offset);
    return IE_POOL_COMPATIBLE_NEW_CLASS(pool, IntegratedPlainChunkDecoder,
            baseAddr + offset + sizeof(meta), meta.length, recordLen);
}

inline ChunkDecoder* ChunkDecoderCreator::Create(
        file_system::FileReader* fileReader,
        file_system::FSOpenType openType,
        uint64_t offset, uint32_t recordLen, autil::mem_pool::Pool* pool,
        file_system::ReadOption option)
{
    assert(fileReader && fileReader->GetOpenType() == openType);
    if (openType == file_system::FSOT_BUFFERED)
    {
        // for merge
        ChunkMeta meta;
        autil::ConstString data;
        mSimpleCacheDecoder.Read(fileReader, offset, pool, meta, data);
        if (meta.isEncoded)
        {
            assert(false);
            return NULL;
        }
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, IntegratedPlainChunkDecoder,
                data.data(), meta.length, recordLen);
    }

    ChunkMeta meta;
    fileReader->Read(&meta, sizeof(meta), offset, option);
    if (meta.isEncoded)
    {
        assert(false);
        return NULL;
    }

    assert(fileReader->GetBaseAddress() == NULL);
    char* buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, char, meta.length);
    if (fileReader->Read(buffer, meta.length, offset + sizeof(meta), option) != meta.length)
    {
        INDEXLIB_FATAL_ERROR(FileIO, "read chunk data [offset:%lu, length:%u] failed!",
                             offset + sizeof(meta), meta.length);
    }
    return IE_POOL_COMPATIBLE_NEW_CLASS(pool, IntegratedPlainChunkDecoder,
            buffer, meta.length, recordLen);
    
    // TODO : use pool to allocate ByteSliceList and insure all slices have same length
    util::ByteSliceList* byteSliceList = fileReader->Read(meta.length, offset + sizeof(meta), option);
    return IE_POOL_COMPATIBLE_NEW_CLASS(pool, SlicePlainChunkDecoder, byteSliceList, pool);
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CHUNK_DECODER_CREATOR_H
