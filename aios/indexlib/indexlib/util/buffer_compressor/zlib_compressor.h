#ifndef __INDEXLIB_ZLIB_COMPRESSOR_H
#define __INDEXLIB_ZLIB_COMPRESSOR_H

#include <tr1/memory>
#include <zlib.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/buffer_compressor/buffer_compressor.h"

IE_NAMESPACE_BEGIN(util);

class ZlibCompressor : public BufferCompressor
{
public:
    ZlibCompressor(autil::mem_pool::Pool* pool, size_t maxDataSize)
        : BufferCompressor(pool, compressBound(maxDataSize))
    {
        mCompressorName = COMPRESSOR_NAME;
    }

    ZlibCompressor()
    {
        mCompressorName = COMPRESSOR_NAME;
    }

    ~ZlibCompressor() {}
    
public:
    bool Compress() override;
    bool DecompressToOutBuffer(const char* src, uint32_t srcLen,
                               size_t maxUncompressLen = 0) override final;

private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 64 * 1024; //64k

private:
    bool DecompressStream(const char* src, uint32_t srcLen);

public:
    static const std::string COMPRESSOR_NAME;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ZlibCompressor);
/////////////////////////////////////////////////////////////////////
inline bool ZlibCompressor::Compress()
{
    _bufferOut->reset();
    if(_bufferIn->getDataLen() == 0)
    {
        return false;
    }

    uLong compressLen = compressBound(_bufferIn->getDataLen());
    _bufferOut->reserve((uint32_t)compressLen);

    if (compress2((Bytef*)_bufferOut->getPtr(), &compressLen,
                  (const Bytef*)_bufferIn->getBuffer(),
                  (uLong)_bufferIn->getDataLen(), Z_BEST_SPEED) != Z_OK)
    {
        return false;
    }
    _bufferOut->movePtr(compressLen);
    return true;
}

inline bool ZlibCompressor::DecompressStream(const char* src, uint32_t srcLen)
{
    int ret;
    unsigned have;
    z_stream strm;

    if (_bufferOut->getDataLen() == 0)
    {
        _bufferOut->reserve(DEFAULT_BUFFER_SIZE);
    }
    _bufferOut->reset();
    
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = inflateInit(&strm);
    if (ret != Z_OK)
    {
        return false;
    }

    strm.avail_in = srcLen;
    strm.next_in = (Bytef*)src;

    do 
    {
        uint32_t remain = _bufferOut->remaining(); 
        strm.avail_out = remain;
        strm.next_out = (Bytef*)_bufferOut->getPtr();

        ret = inflate(&strm, Z_NO_FLUSH);
        switch (ret) 
        {
        case Z_STREAM_ERROR:
        case Z_NEED_DICT:
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
            (void)inflateEnd(&strm);
            return false;
        }
        have = remain - strm.avail_out;
        _bufferOut->movePtr(have);
        if(have == remain)
        {
            _bufferOut->reserve((_bufferOut->getTotalSize() << 1) - _bufferOut->getDataLen());
        }
    } while (strm.avail_out == 0);

    (void)inflateEnd(&strm);
    return true;
}

inline bool ZlibCompressor::DecompressToOutBuffer(
        const char* src, uint32_t srcLen, size_t maxUnCompressLen)
{
    if (maxUnCompressLen == 0)
    {
        return DecompressStream(src, srcLen);
    }
    _bufferOut->reserve(maxUnCompressLen);
    _bufferOut->reset();
    uLong len = std::max((size_t)_bufferOut->getTotalSize(), maxUnCompressLen);

    if (uncompress((Bytef*)_bufferOut->getPtr(), &len,
                   (const Bytef*)src, (uLong)srcLen) != Z_OK)
    {
        return false;
    }
    _bufferOut->movePtr(len);
    return true;
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_ZLIB_COMPRESSOR_H
