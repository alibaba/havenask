#include "indexlib/common/numeric_compress/new_pfordelta_compressor.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, NewPForDeltaCompressor);

int64_t NewPForDeltaCompressor::BASIC_MASK[] = 
{
    0,
    1,
    ( 1 << 2 )  - 1,
    ( 1 << 3 )  - 1,
    ( 1 << 4 )  - 1,
    ( 1 << 5 )  - 1,
    ( 1 << 6 )  - 1,
    ( 1 << 7 )  - 1,
    ( 1 << 8 )  - 1,
    ( 1 << 9 )  - 1,
    ( 1 << 10 ) - 1,
    ( 1 << 11 ) - 1,
    ( 1 << 12 ) - 1,
    ( 1 << 13 ) - 1,
    ( 1 << 14 ) - 1,
    ( 1 << 15 ) - 1,
    ( 1 << 16 ) - 1,
    ( 1 << 17 ) - 1,
    ( 1 << 18 ) - 1,
    ( 1 << 19 ) - 1,
    ( 1 << 20 ) - 1,
    ( 1 << 21 ) - 1,
    ( 1 << 22 ) - 1,
    ( 1 << 23 ) - 1,
    ( 1 << 24 ) - 1,
    ( 1 << 25 ) - 1,
    ( 1 << 26 ) - 1,
    ( 1 << 27 ) - 1,
    ( 1 << 28 ) - 1,
    ( 1 << 29 ) - 1,
    ( 1 << 30 ) - 1,
    ( (int64_t)1 << 31 ) - 1,
    ( (int64_t)1 << 32 ) - 1
};

uint32_t NewPForDeltaCompressor::FRAME_BIT_MAP[] =
{
    0,
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,  
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    32,  //31 map to 32
    32,  //32
};

NewPForDeltaCompressor::NewPForDeltaCompressor() 
{
}

NewPForDeltaCompressor::~NewPForDeltaCompressor() 
{
}

size_t NewPForDeltaCompressor::CompressInt32(uint32_t* dest, size_t destLen,
        const uint32_t* src, size_t srcLen) const
{
    return Compress<uint32_t>(dest, destLen, src, srcLen);
}

size_t NewPForDeltaCompressor::CompressInt16(uint32_t* dest, size_t destLen,
                          const uint16_t* src, size_t srcLen) const
{
    return Compress<uint16_t>(dest, destLen, src, srcLen);    
}

size_t NewPForDeltaCompressor::CompressInt8(uint32_t* dest, size_t destLen,
                          const uint8_t* src, size_t srcLen) const
{
    return Compress<uint8_t>(dest, destLen, src, srcLen);    
}

size_t NewPForDeltaCompressor::DecompressInt32(uint32_t* dest, size_t destLen,
                          const uint32_t* src, size_t srcLen) const
{
    return Decompress<uint32_t>(dest, destLen, src, srcLen);   
}

size_t NewPForDeltaCompressor::DecompressInt16(uint16_t* dest, size_t destLen,
                          const uint32_t* src, size_t srcLen) const
{
    return Decompress<uint16_t>(dest, destLen, src, srcLen);       
}

size_t NewPForDeltaCompressor::DecompressInt8(uint8_t* dest, size_t destLen,
                          const uint32_t* src, size_t srcLen) const
{
    return Decompress<uint8_t>(dest, destLen, src, srcLen);
}

IE_NAMESPACE_END(common);

