#ifndef __INDEXLIB_SNAPPY_COMPRESSOR_H
#define __INDEXLIB_SNAPPY_COMPRESSOR_H

#include <tr1/memory>
#include "snappy.h"
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/buffer_compressor/buffer_compressor.h"

IE_NAMESPACE_BEGIN(util);

class SnappyCompressor : public BufferCompressor
{
public:
    SnappyCompressor(autil::mem_pool::Pool* pool, size_t maxDataSize)
        : BufferCompressor(pool, snappy::MaxCompressedLength(maxDataSize))
    {
        mCompressorName = COMPRESSOR_NAME;
    }
    
    SnappyCompressor()
    {
        mCompressorName = COMPRESSOR_NAME;
    }
    
    ~SnappyCompressor() {}

public:
    bool Compress() override final;
    bool DecompressToOutBuffer(const char* src, uint32_t srcLen,
                               size_t maxUncompressLen = 0) override final;

public:
    static const std::string COMPRESSOR_NAME;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SnappyCompressor);
/////////////////////////////////////////////////////////////////////
inline bool SnappyCompressor::Compress()
{
    _bufferOut->reset();
    if(_bufferIn->getDataLen() == 0)
    {
        return false;
    }
    _bufferOut->reserve(snappy::MaxCompressedLength(_bufferIn->getDataLen()));
    size_t compressLen = 0;
    snappy::RawCompress(_bufferIn->getBuffer(), _bufferIn->getDataLen(), 
                        _bufferOut->getPtr(), &compressLen);
    _bufferOut->movePtr(compressLen);
    return true;
}

inline bool SnappyCompressor::DecompressToOutBuffer(
        const char* src, uint32_t srcLen, size_t maxUnCompressLen)
{

    size_t len = 0;
    if (!snappy::GetUncompressedLength(src, srcLen, &len))
    {
        assert(false);
        return false;
    }

    if (maxUnCompressLen > 0)
    {
        _bufferOut->reserve(maxUnCompressLen);
    }
    else
    {
        _bufferOut->reserve(len);
    }
    _bufferOut->reset();
    if (!snappy::RawUncompress(src, srcLen, _bufferOut->getPtr()))
    {
        assert(false);
        return false;
    }
    _bufferOut->movePtr(len);
    return true;
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_SNAPPY_COMPRESSOR_H
