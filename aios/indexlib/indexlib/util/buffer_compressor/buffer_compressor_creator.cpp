#include "indexlib/util/buffer_compressor/buffer_compressor_creator.h"
#include "indexlib/util/buffer_compressor/snappy_compressor.h"
#include "indexlib/util/buffer_compressor/lz4_compressor.h"
#include "indexlib/util/buffer_compressor/lz4_hc_compressor.h"
#include "indexlib/util/buffer_compressor/zlib_compressor.h"
#include "indexlib/util/buffer_compressor/zlib_default_compressor.h"
#include "indexlib/util/buffer_compressor/zstd_compressor.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, BufferCompressorCreator);

BufferCompressorCreator::BufferCompressorCreator() 
{
}

BufferCompressorCreator::~BufferCompressorCreator() 
{
}

BufferCompressor* BufferCompressorCreator::CreateBufferCompressor(
        const string& compressorName)
{
    if (compressorName == SnappyCompressor::COMPRESSOR_NAME)
    {
        return new SnappyCompressor();
    }
    else if (compressorName == Lz4Compressor::COMPRESSOR_NAME)
    {
        return new Lz4Compressor();
    }
    else if (compressorName == Lz4HcCompressor::COMPRESSOR_NAME)
    {
        return new Lz4HcCompressor();
    }
    else if (compressorName == ZstdCompressor::COMPRESSOR_NAME)
    {
        return new ZstdCompressor();
    }
    else if (compressorName == ZlibCompressor::COMPRESSOR_NAME)
    {
        return new ZlibCompressor();
    }
    else if (compressorName == ZlibDefaultCompressor::COMPRESSOR_NAME)
    {
        return new ZlibDefaultCompressor();
    }
    else
    {
        IE_LOG(ERROR, "unknown compressor name [%s]", compressorName.c_str());
        return NULL;
    }
    return NULL;
}

BufferCompressor* BufferCompressorCreator::CreateBufferCompressor(
        Pool* pool, const string& compressorName, size_t maxBufferSize)
{
    assert(pool);
    BufferCompressor* compressor = NULL;
    if (compressorName == SnappyCompressor::COMPRESSOR_NAME)
    {
        compressor = IE_POOL_COMPATIBLE_NEW_CLASS(
                pool, SnappyCompressor, pool, maxBufferSize);
    }
    else if (compressorName == Lz4Compressor::COMPRESSOR_NAME)
    {
        compressor = IE_POOL_COMPATIBLE_NEW_CLASS(
                pool, Lz4Compressor, pool, maxBufferSize);
    }
    else if (compressorName == Lz4HcCompressor::COMPRESSOR_NAME)
    {
        compressor = IE_POOL_COMPATIBLE_NEW_CLASS(
                pool, Lz4HcCompressor, pool, maxBufferSize);
    }
    else if (compressorName == ZstdCompressor::COMPRESSOR_NAME)
    {
        compressor = IE_POOL_COMPATIBLE_NEW_CLASS(
                pool, ZstdCompressor, pool, maxBufferSize);
    }
    else if (compressorName == ZlibCompressor::COMPRESSOR_NAME)
    {
        compressor = IE_POOL_COMPATIBLE_NEW_CLASS(
                pool, ZlibCompressor, pool, maxBufferSize);
    }
    else if (compressorName == ZlibDefaultCompressor::COMPRESSOR_NAME)
    {
        compressor = IE_POOL_COMPATIBLE_NEW_CLASS(
                pool, ZlibDefaultCompressor, pool, maxBufferSize);
    }
    else
    {
        IE_LOG(ERROR, "unknown compressor name [%s]", compressorName.c_str());
        return NULL;
    }
    return compressor;
}

IE_NAMESPACE_END(util);

