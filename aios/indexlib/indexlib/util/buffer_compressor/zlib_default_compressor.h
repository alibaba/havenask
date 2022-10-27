#ifndef __INDEXLIB_ZLIB_DEFAULT_COMPRESSOR_H
#define __INDEXLIB_ZLIB_DEFAULT_COMPRESSOR_H

#include <tr1/memory>
#include <zlib.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/buffer_compressor/zlib_compressor.h"

IE_NAMESPACE_BEGIN(util);

class ZlibDefaultCompressor : public ZlibCompressor
{
public:
    ZlibDefaultCompressor(autil::mem_pool::Pool* pool, size_t maxDataSize)
        : ZlibCompressor(pool, compressBound(maxDataSize))
    {
        mCompressorName = COMPRESSOR_NAME;
    }

    ZlibDefaultCompressor()
    {
        mCompressorName = COMPRESSOR_NAME;
    }

    ~ZlibDefaultCompressor() {}

public:
    static const int NO_COMPRESSION = Z_NO_COMPRESSION;
    static const int BEST_SPEED = Z_BEST_SPEED ;
    static const int BEST_COMPRESSION = Z_BEST_COMPRESSION;
    static const int DEFAULT_COMPRESSION = Z_DEFAULT_COMPRESSION;

private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 64 * 1024; //64k
    
public:
    bool Compress() override final;

public:
    static const std::string COMPRESSOR_NAME;

private:
    int _compressType = DEFAULT_COMPRESSION;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ZlibDefaultCompressor);
/////////////////////////////////////////////////////////////////////
inline bool ZlibDefaultCompressor::Compress()
{
    if(_bufferIn->getDataLen() == 0)
    {
        return false;
    }
    if (_bufferOut->getDataLen() == 0)
    {
        _bufferOut->reserve(DEFAULT_BUFFER_SIZE);
    }

    int ret, flush;
    uint32_t have;
    z_stream strm;

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = deflateInit(&strm, _compressType);
    if (ret != Z_OK)
    {
        return false;
    }

    strm.avail_in = _bufferIn->getDataLen();
    strm.next_in = (Bytef*)_bufferIn->getBuffer();
    flush = Z_FINISH;
    
    do 
    {
        uint32_t remain = _bufferOut->remaining(); 
        strm.avail_out = remain;
        strm.next_out = (Bytef*)_bufferOut->getPtr();
        ret = deflate(&strm, flush);
        if(ret == Z_STREAM_ERROR)
        {
            return false;
        }
        
        have = remain - strm.avail_out;
        _bufferOut->movePtr(have);
        if (strm.avail_out == 0)
        {
            _bufferOut->reserve((_bufferOut->getTotalSize() << 1) - _bufferOut->getDataLen());
        }
    } while (strm.avail_out == 0);

    assert(strm.avail_in == 0);
    (void)deflateEnd(&strm);

    return true;
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_ZLIB_DEFAULT_COMPRESSOR_H
