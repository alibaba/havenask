#ifndef __INDEXLIB_BLOCK_FP_ENCODER_H
#define __INDEXLIB_BLOCK_FP_ENCODER_H

#include <cmath>
#include <tr1/memory>
#include <autil/ConstString.h>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class BlockFpEncoder
{
private:
    BlockFpEncoder();
    ~BlockFpEncoder();
public:
    static int32_t Encode(const float* input, size_t count,
                         char* output, size_t outBufferSize);

    static autil::ConstString Encode(const float* input, size_t count,
                                     autil::mem_pool::Pool* pool);
    
    static int32_t Decode(const autil::ConstString& data, char* output,
                          size_t outputLen)
    {
        if (data.empty()) {
            return -1;
        }
        size_t valueCount = GetDecodeCount(data.size());
        if (valueCount * sizeof(float) > outputLen) {
            return -1;
        }
        const char* buffer = data.data();
        char maxExponent = buffer[0] - 127;
        buffer++;
        int16_t* fractionBuffer = (int16_t*)buffer;
        for (size_t i = 0; i < valueCount; i++) {
            ((float*)output)[i] = ((float)(fractionBuffer[i])/FLOAT_BITS_VALUE) *
                std::pow(2, maxExponent);
        }
        return valueCount;
    }

    static size_t GetEncodeBytesLen(size_t count) {
        return (count << 1) + 1;
    }
    
    static size_t GetDecodeCount(size_t dataLen) {
        return dataLen == 0 ? 0 : (dataLen - 1) >> 1;
    }

private:
    typedef struct FpSingle_ {
        uint32_t nFraction : 23;
        uint32_t nExponent :  8;
        uint32_t nSign     :  1;
    } FpSingle;

    typedef struct Int16_ {
        uint16_t nFraction: 15;
	uint16_t nSign: 1;
    } Int16;

private:
    static const int FLOAT_BITS   = 15;
    static const int FLOAT_BITS_VALUE;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BlockFpEncoder);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BLOCK_FP_ENCODER_H
