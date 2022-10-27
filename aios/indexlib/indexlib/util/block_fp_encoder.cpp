#include "indexlib/util/block_fp_encoder.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, BlockFpEncoder);
const int BlockFpEncoder::FLOAT_BITS_VALUE = std::pow(2, FLOAT_BITS - 1);

BlockFpEncoder::BlockFpEncoder() 
{
}

BlockFpEncoder::~BlockFpEncoder() 
{
}

int32_t BlockFpEncoder::Encode(const float* input, size_t count,
                               char* output, size_t outBufferSize)
{
    size_t bufferSize = GetEncodeBytesLen(count);
    if (outBufferSize < bufferSize)
    {
        return -1;
    }
    char* buffer = output;
    FpSingle* cursor = (FpSingle*)input;
    uint8_t maxExponent = 0;
    for (size_t i = 0; i < count; i++) {
        if (cursor[i].nExponent > maxExponent) {
            maxExponent = cursor[i].nExponent;
        }
    }

    buffer[0] = (char)maxExponent;
    int16_t* fractionBuffer = (int16_t*)(buffer + 1);
    for (size_t i = 0; i < count; i++) {
        FpSingle value = cursor[i];
        if (value.nFraction == 0 && value.nExponent == 0) {
            fractionBuffer[i] = 0;
        } else {
	    uint32_t frac = (0x800000 | value.nFraction) >> (maxExponent - value.nExponent);
            fractionBuffer[i] = (1-2*value.nSign) * (0x7FFF & ((uint16_t)(frac >> (23 + 1 - FLOAT_BITS)) + ((frac & 0x0100) >> 8)));
        }
    }
    return (int32_t)bufferSize;
}

ConstString BlockFpEncoder::Encode(const float* input, size_t count,  Pool* pool)
{
    if (!input || !pool || count == 0) {
        return ConstString::EMPTY_STRING;
    }
    
    size_t bufferSize = GetEncodeBytesLen(count);
    char* buffer = (char*)pool->allocate(bufferSize);
    int32_t compLen = Encode(input, count, buffer, bufferSize);
    assert(compLen == (int32_t)bufferSize);
    return ConstString(buffer, compLen);
}

IE_NAMESPACE_END(util);

