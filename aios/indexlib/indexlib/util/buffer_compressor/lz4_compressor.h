#ifndef __INDEXLIB_LZ4_COMPRESSOR_H
#define __INDEXLIB_LZ4_COMPRESSOR_H

#include <tr1/memory>
#include <lz4.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/buffer_compressor/buffer_compressor.h"

IE_NAMESPACE_BEGIN(util);

class Lz4Compressor : public BufferCompressor
{
public:
    Lz4Compressor(autil::mem_pool::Pool* pool, size_t maxDataSize)
        : BufferCompressor(pool, LZ4_compressBound(maxDataSize))
    {
        mCompressorName = COMPRESSOR_NAME;
    }
    
    Lz4Compressor()
    {
        mCompressorName = COMPRESSOR_NAME;
    }
    
    ~Lz4Compressor() {}
    
public:
    bool Compress() override;
    bool DecompressToOutBuffer(const char* src, uint32_t srcLen,
                               size_t maxUnCompressLen = 0) override final;

public:
    static const std::string COMPRESSOR_NAME;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Lz4Compressor);

/////////////////////////////////////////////////////////////////////////////
inline bool Lz4Compressor::Compress()
{
    _bufferOut->reset();
    if(_bufferIn->getDataLen() == 0)
    {
        return false;
    }

    size_t maxBuffSize = LZ4_compressBound(_bufferIn->getDataLen());
    _bufferOut->reserve(maxBuffSize);
    int compressLen = LZ4_compress_default(_bufferIn->getBuffer(), _bufferOut->getPtr(),
            _bufferIn->getDataLen(), maxBuffSize);
    _bufferOut->movePtr(compressLen);
    return true;
}

inline bool Lz4Compressor::DecompressToOutBuffer(
        const char* src, uint32_t srcLen, size_t maxUnCompressLen)
{
    if (maxUnCompressLen == 0)
    {
        maxUnCompressLen = std::min((size_t)srcLen << 8, 1UL << 26); // min(srcLen * 256, 64M)
    }
    _bufferOut->reset();
    _bufferOut->reserve(maxUnCompressLen);

    int len = LZ4_decompress_safe(src,  _bufferOut->getPtr(),
                                  srcLen, _bufferOut->getTotalSize());
    if (len < 0)
    {
        return false;
    }
    _bufferOut->movePtr(len);
    return true;
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_LZ4_COMPRESSOR_H
