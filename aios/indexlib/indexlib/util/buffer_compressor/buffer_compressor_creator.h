#ifndef __INDEXLIB_BUFFER_COMPRESSOR_CREATOR_H
#define __INDEXLIB_BUFFER_COMPRESSOR_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/buffer_compressor/buffer_compressor.h"

IE_NAMESPACE_BEGIN(util);

class BufferCompressorCreator
{
public:
    BufferCompressorCreator();
    ~BufferCompressorCreator();
    
public:
    static BufferCompressor* CreateBufferCompressor(const std::string& compressorName);
    static BufferCompressor* CreateBufferCompressor(autil::mem_pool::Pool* pool,
            const std::string& compressorName, size_t maxBufferSize);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BufferCompressorCreator);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BUFFER_COMPRESSOR_CREATOR_H
