#ifndef __INDEXLIB_LZ4_HC_COMPRESSOR_H
#define __INDEXLIB_LZ4_HC_COMPRESSOR_H

#include <tr1/memory>
#include <lz4hc.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/buffer_compressor/lz4_compressor.h"

IE_NAMESPACE_BEGIN(util);

class Lz4HcCompressor : public Lz4Compressor
{
public:
    Lz4HcCompressor(autil::mem_pool::Pool* pool, size_t maxDataSize)
        : Lz4Compressor(pool, maxDataSize)
    {
        mCompressorName = COMPRESSOR_NAME;
    }

    Lz4HcCompressor()
    {
        mCompressorName = COMPRESSOR_NAME;
    }
    
    ~Lz4HcCompressor() {}

public:
    bool Compress() override final;

public:
    static const std::string COMPRESSOR_NAME;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Lz4HcCompressor);

/////////////////////////////////////////////////////////////////////////////
inline bool Lz4HcCompressor::Compress()
{
    _bufferOut->reset();
    if(_bufferIn->getDataLen() == 0)
    {
        return false;
    }

    size_t maxBuffSize = LZ4_compressBound(_bufferIn->getDataLen());
    _bufferOut->reserve(maxBuffSize);
    int compressLen = LZ4_compress_HC(_bufferIn->getBuffer(), _bufferOut->getPtr(),
            _bufferIn->getDataLen(), maxBuffSize, LZ4HC_DEFAULT_CLEVEL);
    _bufferOut->movePtr(compressLen);
    return true;
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_LZ4_HC_COMPRESSOR_H
