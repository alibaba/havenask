#ifndef __INDEXLIB_ZSTD_COMPRESSOR_H
#define __INDEXLIB_ZSTD_COMPRESSOR_H

#include <tr1/memory>
#include <zstd.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/buffer_compressor/buffer_compressor.h"

IE_NAMESPACE_BEGIN(util);

class ZstdCompressor : public BufferCompressor
{
public:
    ZstdCompressor(autil::mem_pool::Pool* pool, size_t maxDataSize)
        : BufferCompressor(pool, ZSTD_compressBound(maxDataSize))
        , mDstream(NULL)
    {
        mCompressorName = COMPRESSOR_NAME;
    }
    
    ZstdCompressor()
        : mDstream(NULL)
    {
        mCompressorName = COMPRESSOR_NAME;
    }

    ~ZstdCompressor()
    {
        if (mDstream)
        {
            ZSTD_freeDStream(mDstream);
            mDstream = NULL;
        }
    }
    
public:
    bool Compress() override final;
    bool DecompressToOutBuffer(const char* src, uint32_t srcLen,
                               size_t maxUnCompressLen = 0) override final;

private:
    bool DecompressStream(const char* src, uint32_t srcLen);

public:
    static const std::string COMPRESSOR_NAME;
private:
    static const int DEFAULT_COMPRESS_LEVEL = 1;
    ZSTD_DStream* mDstream;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ZstdCompressor);
/////////////////////////////////////////////////////////////////////////////
inline bool ZstdCompressor::Compress()
{
    _bufferOut->reset();
    if(_bufferIn->getDataLen() == 0)
    {
        return false;
    }

    size_t outBufferCapacity = ZSTD_compressBound(_bufferIn->getDataLen());
    _bufferOut->reserve(outBufferCapacity);
    size_t compressLen = ZSTD_compress(_bufferOut->getPtr(), outBufferCapacity,
            _bufferIn->getBuffer(), _bufferIn->getDataLen(), DEFAULT_COMPRESS_LEVEL);
    if (ZSTD_isError(compressLen))
    {
        return false;
    }
    _bufferOut->movePtr(compressLen);
    return true;
}

inline bool ZstdCompressor::DecompressStream(const char* src, uint32_t srcLen)
{
    // Guarantee to successfully flush at least one complete compressed block in all circumstances. 
    if (_bufferOut->getDataLen() == 0)
    {
        _bufferOut->reserve(ZSTD_DStreamOutSize());
    }
    _bufferOut->reset();

    if (!mDstream)
    {
        mDstream = ZSTD_createDStream();
        if (!mDstream)
        {
            IE_LOG(ERROR, "ZSTD_createDStream() error");
            return false;
        }
    }

    const size_t initResult = ZSTD_initDStream(mDstream);
    if (ZSTD_isError(initResult))
    {
        IE_LOG(ERROR, "ZSTD_initDStream() error, %s", ZSTD_getErrorName(initResult));
        return false;
    }
    size_t toRead = initResult;
    ZSTD_inBuffer input = { src, srcLen, 0 };
    while (input.pos < input.size) {
        uint32_t remain = _bufferOut->remaining();
        ZSTD_outBuffer output = { _bufferOut->getPtr(), remain, 0 };
        // toRead : size of next compressed block
        toRead = ZSTD_decompressStream(mDstream, &output, &input);  
        if (ZSTD_isError(toRead))
        {
            IE_LOG(ERROR, "ZSTD_decompressStream() error, %s",
                   ZSTD_getErrorName(toRead));
            return false;
        }
        _bufferOut->movePtr(output.pos);
        if (output.pos == 0)
        {
            _bufferOut->reserve((_bufferOut->getTotalSize() << 1) - _bufferOut->getDataLen());
        }
    }

    return true;
}

inline bool ZstdCompressor::DecompressToOutBuffer(
        const char* src, uint32_t srcLen, size_t maxUnCompressLen)
{
    if (maxUnCompressLen == 0)
    {
        return DecompressStream(src, srcLen);
    }

    _bufferOut->reserve(maxUnCompressLen);
    _bufferOut->reset();
    size_t len = ZSTD_decompress(_bufferOut->getPtr(), maxUnCompressLen, src, srcLen);
    if (ZSTD_isError(len))
    {
        return false;
    }
    _bufferOut->movePtr(len);
    return true;
}


IE_NAMESPACE_END(util);

#endif //__INDEXLIB_ZSTD_COMPRESSOR_H
