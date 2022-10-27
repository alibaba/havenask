#ifndef __INDEXLIB_BUFFER_COMPRESSOR_H
#define __INDEXLIB_BUFFER_COMPRESSOR_H

#include <tr1/memory>
#include <autil/DynamicBuf.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"


IE_NAMESPACE_BEGIN(util);

class BufferCompressor
{
public:
    BufferCompressor(autil::mem_pool::Pool* pool, size_t maxDataSize)
        : _bufferIn(NULL)
        , _bufferOut(NULL)
        , mPool(pool)
        , mPoolBufferIn(NULL)
        , mPoolBufferOut(NULL)
        , mPoolBufferSize(maxDataSize)
    {
        mPoolBufferIn = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, char, maxDataSize);
        mPoolBufferOut = IE_POOL_COMPATIBLE_NEW_VECTOR(mPool, char, maxDataSize);
        _bufferIn = IE_POOL_COMPATIBLE_NEW_CLASS(mPool,
                autil::DynamicBuf, mPoolBufferIn, maxDataSize);
        _bufferOut = IE_POOL_COMPATIBLE_NEW_CLASS(mPool,
                autil::DynamicBuf, mPoolBufferOut, maxDataSize);
    }
    
    BufferCompressor()
        : _bufferIn(NULL)
        , _bufferOut(NULL)
        , mPool(NULL)
        , mPoolBufferIn(NULL)
        , mPoolBufferOut(NULL)
        , mPoolBufferSize(0)
    {
        _bufferIn = new autil::DynamicBuf;
        _bufferOut = new autil::DynamicBuf;
    }
    
    virtual ~BufferCompressor()
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, _bufferIn);
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, _bufferOut);
        if (mPoolBufferIn)
        {
            IE_POOL_COMPATIBLE_DELETE_VECTOR(mPool, mPoolBufferIn, mPoolBufferSize);
        }
        if (mPoolBufferOut)
        {
            IE_POOL_COMPATIBLE_DELETE_VECTOR(mPool, mPoolBufferOut, mPoolBufferSize);
        }
    }
    
public:
    void SetBufferInLen(const uint32_t bufferInLen)
    {
        _bufferIn->reset();
        _bufferIn->reserve(bufferInLen);
    }

    void SetBufferOutLen(const uint32_t bufferOutLen)
    {
        _bufferOut->reset();
        _bufferOut->reserve(bufferOutLen);
    }

public:
    void AddDataToBufferIn(const char* source, const uint32_t len);
    void AddDataToBufferIn(const std::string& source);

    autil::DynamicBuf& GetInBuffer() 
    { return *_bufferIn; }

    autil::DynamicBuf& GetOutBuffer() 
    { return *_bufferOut; }

    const char* GetBufferOut() const;
    const char* GetBufferIn() const;

    uint32_t GetBufferOutLen() const;
    uint32_t GetBufferInLen() const;
    void Reset();

    virtual bool Compress() = 0;
    
    bool Decompress(size_t maxUncompressLen = 0)
    {
        return DecompressToOutBuffer(_bufferIn->getBuffer(),
                _bufferIn->getDataLen(), maxUncompressLen);
    }
    
    virtual bool DecompressToOutBuffer(
            const char* src, uint32_t srcLen, size_t maxUncompressLen = 0) = 0;
    
    const std::string& GetCompressorName() const
    { return mCompressorName; }

protected:
    autil::DynamicBuf* _bufferIn;
    autil::DynamicBuf* _bufferOut;
    autil::mem_pool::Pool* mPool;
    std::string mCompressorName;

    char* mPoolBufferIn;
    char* mPoolBufferOut;
    size_t mPoolBufferSize;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BufferCompressor);

/////////////////////////////////////////////////////////////////////
inline void BufferCompressor::AddDataToBufferIn(const char* source, const uint32_t len)
{
    _bufferIn->add(source, len);
}

inline void BufferCompressor::AddDataToBufferIn(const std::string& source)
{
    _bufferIn->add(source.c_str(), source.size());
}

inline const char* BufferCompressor::GetBufferOut() const
{
    return _bufferOut->getBuffer();
}

inline uint32_t BufferCompressor::GetBufferOutLen() const
{
    return _bufferOut->getDataLen();
}

inline uint32_t BufferCompressor::GetBufferInLen() const
{
    return _bufferIn->getDataLen();
}

inline const char* BufferCompressor::GetBufferIn() const
{
    return _bufferIn->getBuffer();
}

inline void BufferCompressor::Reset()
{
    _bufferIn->reset();
    _bufferOut->reset();
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BUFFER_COMPRESSOR_H
