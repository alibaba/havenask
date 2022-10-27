#ifndef __INDEXLIB_S16_COMPRESSOR_H
#define __INDEXLIB_S16_COMPRESSOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"

IE_NAMESPACE_BEGIN(common);

class S16Compressor
{
public:
    S16Compressor();
    ~S16Compressor();
public:
    template<typename T>
    static uint32_t Encode(uint32_t* dest, uint32_t* destEnd,
                           const T* src, size_t srcLen);

    template<typename T>
    static uint32_t Decode(T* dest, T* destEnd, const uint32_t* src, 
                           size_t srcLen);

    template<typename T>
    static uint32_t DecodeOneUnit(T* dest, T* destEnd, const uint32_t* src);

private:
    template<typename T>
    static uint32_t EncodeOneUnit(uint32_t* dest, const T* src, 
                                  size_t srcLen);

    template<typename T>
    static void CheckBufferLen(T* dest, T* destEnd, uint32_t k);

    static const uint8_t bitCount[16][28];
    static const uint8_t itemNumPerUnit[16];

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(S16Compressor);

template<typename T>
inline uint32_t S16Compressor::Encode(uint32_t* dest, uint32_t* destEnd, const T* src, 
                                      size_t srcLen)
{
    size_t left = srcLen;
    uint32_t* destCursor = dest;
    const T* srcCursor = src;

    while (left > 0)
    {
        CheckBufferLen(destCursor, destEnd, 1);
        uint32_t encodedNum = EncodeOneUnit(destCursor, srcCursor, left);

        srcCursor += encodedNum;
        left -= encodedNum;
        destCursor++;
    }

    return destCursor - dest;
}

template<typename T>
inline uint32_t S16Compressor::EncodeOneUnit(uint32_t* dest, const T* src, size_t srcLen)
{
    uint32_t codedNum;
    uint32_t numToCode = 0;

    for (uint32_t k = 0; k < 16; k++)
    {
        (*dest) = k << 28;

        if (srcLen < itemNumPerUnit[k])
            continue;
        numToCode = itemNumPerUnit[k];

        uint32_t bitCountSum = 0;
        for (codedNum = 0; 
             (codedNum < numToCode) && (*(src + codedNum) < (uint32_t)(1 << bitCount[k][codedNum])); 
             codedNum++)
        {
            (*dest) += ((*(src + codedNum)) << bitCountSum);
            bitCountSum += bitCount[k][codedNum];
        }

        if (codedNum == numToCode)
        {
            return numToCode;
        }
    }

    INDEXLIB_FATAL_ERROR(Runtime, "Encode posting-list FAILED: " 
                         "[%d] exceeds 28 bit.", (int32_t)(*src));
    return 0;
}

template<typename T>
inline uint32_t S16Compressor::DecodeOneUnit(T* dest, T* destEnd, const uint32_t* src)
{
    uint32_t k = (*src) >> 28;
    T* destCursor = dest;

    switch (k)
    {
    case 0:
        //CheckBufferLen(dest, destEnd, 28);
        *destCursor++ = (*src) & 1;
        *destCursor++ = (*src >> 1) & 1;
        *destCursor++ = (*src >> 2) & 1;
        *destCursor++ = (*src >> 3) & 1;
        *destCursor++ = (*src >> 4) & 1;
        *destCursor++ = (*src >> 5) & 1;
        *destCursor++ = (*src >> 6) & 1;
        *destCursor++ = (*src >> 7) & 1;
        *destCursor++ = (*src >> 8) & 1;
        *destCursor++ = (*src >> 9) & 1;
        *destCursor++ = (*src >> 10) & 1;
        *destCursor++ = (*src >> 11) & 1;
        *destCursor++ = (*src >> 12) & 1;
        *destCursor++ = (*src >> 13) & 1;
        *destCursor++ = (*src >> 14) & 1;
        *destCursor++ = (*src >> 15) & 1;
        *destCursor++ = (*src >> 16) & 1;
        *destCursor++ = (*src >> 17) & 1;
        *destCursor++ = (*src >> 18) & 1;
        *destCursor++ = (*src >> 19) & 1;
        *destCursor++ = (*src >> 20) & 1;
        *destCursor++ = (*src >> 21) & 1;
        *destCursor++ = (*src >> 22) & 1;
        *destCursor++ = (*src >> 23) & 1;
        *destCursor++ = (*src >> 24) & 1;
        *destCursor++ = (*src >> 25) & 1;
        *destCursor++ = (*src >> 26) & 1;
        *destCursor++ = (*src >> 27) & 1;
        break;
    case 1:
        //CheckBufferLen(dest, destEnd, 21);
        *destCursor++ = (*src) & 3;
        *destCursor++ = (*src >> 2) & 3;
        *destCursor++ = (*src >> 4) & 3;
        *destCursor++ = (*src >> 6) & 3;
        *destCursor++ = (*src >> 8) & 3;
        *destCursor++ = (*src >> 10) & 3;
        *destCursor++ = (*src >> 12) & 3;
        *destCursor++ = (*src >> 14) & 1;
        *destCursor++ = (*src >> 15) & 1;
        *destCursor++ = (*src >> 16) & 1;
        *destCursor++ = (*src >> 17) & 1;
        *destCursor++ = (*src >> 18) & 1;
        *destCursor++ = (*src >> 19) & 1;
        *destCursor++ = (*src >> 20) & 1;
        *destCursor++ = (*src >> 21) & 1;
        *destCursor++ = (*src >> 22) & 1;
        *destCursor++ = (*src >> 23) & 1;
        *destCursor++ = (*src >> 24) & 1;
        *destCursor++ = (*src >> 25) & 1;
        *destCursor++ = (*src >> 26) & 1;
        *destCursor++ = (*src >> 27) & 1;
        break;
    case 2:
        //CheckBufferLen(dest, destEnd, 21);
        *destCursor++ = (*src) & 1;
        *destCursor++ = (*src >> 1) & 1;
        *destCursor++ = (*src >> 2) & 1;
        *destCursor++ = (*src >> 3) & 1;
        *destCursor++ = (*src >> 4) & 1;
        *destCursor++ = (*src >> 5) & 1;
        *destCursor++ = (*src >> 6) & 1;
        *destCursor++ = (*src >> 7) & 3;
        *destCursor++ = (*src >> 9) & 3;
        *destCursor++ = (*src >> 11) & 3;
        *destCursor++ = (*src >> 13) & 3;
        *destCursor++ = (*src >> 15) & 3;
        *destCursor++ = (*src >> 17) & 3;
        *destCursor++ = (*src >> 19) & 3;
        *destCursor++ = (*src >> 21) & 1;
        *destCursor++ = (*src >> 22) & 1;
        *destCursor++ = (*src >> 23) & 1;
        *destCursor++ = (*src >> 24) & 1;
        *destCursor++ = (*src >> 25) & 1;
        *destCursor++ = (*src >> 26) & 1;
        *destCursor++ = (*src >> 27) & 1;
        break;
    case 3:
        //CheckBufferLen(dest, destEnd, 21);
        *destCursor++ = (*src) & 1;
        *destCursor++ = (*src >> 1) & 1;
        *destCursor++ = (*src >> 2) & 1;
        *destCursor++ = (*src >> 3) & 1;
        *destCursor++ = (*src >> 4) & 1;
        *destCursor++ = (*src >> 5) & 1;
        *destCursor++ = (*src >> 6) & 1;
        *destCursor++ = (*src >> 7) & 1;
        *destCursor++ = (*src >> 8) & 1;
        *destCursor++ = (*src >> 9) & 1;
        *destCursor++ = (*src >> 10) & 1;
        *destCursor++ = (*src >> 11) & 1;
        *destCursor++ = (*src >> 12) & 1;
        *destCursor++ = (*src >> 13) & 1;
        *destCursor++ = (*src >> 14) & 3;
        *destCursor++ = (*src >> 16) & 3;
        *destCursor++ = (*src >> 18) & 3;
        *destCursor++ = (*src >> 20) & 3;
        *destCursor++ = (*src >> 22) & 3;
        *destCursor++ = (*src >> 24) & 3;
        *destCursor++ = (*src >> 26) & 3;
        break;
    case 4:
        //CheckBufferLen(dest, destEnd, 14);
        *destCursor++ = (*src) & 3;
        *destCursor++ = (*src >> 2) & 3;
        *destCursor++ = (*src >> 4) & 3;
        *destCursor++ = (*src >> 6) & 3;
        *destCursor++ = (*src >> 8) & 3;
        *destCursor++ = (*src >> 10) & 3;
        *destCursor++ = (*src >> 12) & 3;
        *destCursor++ = (*src >> 14) & 3;
        *destCursor++ = (*src >> 16) & 3;
        *destCursor++ = (*src >> 18) & 3;
        *destCursor++ = (*src >> 20) & 3;
        *destCursor++ = (*src >> 22) & 3;
        *destCursor++ = (*src >> 24) & 3;
        *destCursor++ = (*src >> 26) & 3;
        break;
    case 5:
        //CheckBufferLen(dest, destEnd, 9);
        *destCursor++ = (*src) & 15;
        *destCursor++ = (*src >> 4) & 7;
        *destCursor++ = (*src >> 7) & 7;
        *destCursor++ = (*src >> 10) & 7;
        *destCursor++ = (*src >> 13) & 7;
        *destCursor++ = (*src >> 16) & 7;
        *destCursor++ = (*src >> 19) & 7;
        *destCursor++ = (*src >> 22) & 7;
        *destCursor++ = (*src >> 25) & 7;
        break;
    case 6:
        //CheckBufferLen(dest, destEnd, 8);
        *destCursor++ = (*src) & 7;
        *destCursor++ = (*src >> 3) & 15;
        *destCursor++ = (*src >> 7) & 15;
        *destCursor++ = (*src >> 11) & 15;
        *destCursor++ = (*src >> 15) & 15;
        *destCursor++ = (*src >> 19) & 7;
        *destCursor++ = (*src >> 22) & 7;
        *destCursor++ = (*src >> 25) & 7;
        break;
    case 7:
        //CheckBufferLen(dest, destEnd, 7);
        *destCursor++ = (*src) & 15;
        *destCursor++ = (*src >> 4) & 15;
        *destCursor++ = (*src >> 8) & 15;
        *destCursor++ = (*src >> 12) & 15;
        *destCursor++ = (*src >> 16) & 15;
        *destCursor++ = (*src >> 20) & 15;
        *destCursor++ = (*src >> 24) & 15;
        break;
    case 8:
        //CheckBufferLen(dest, destEnd, 6);
        *destCursor++ = (*src) & 31;
        *destCursor++ = (*src >> 5) & 31;
        *destCursor++ = (*src >> 10) & 31;
        *destCursor++ = (*src >> 15) & 31;
        *destCursor++ = (*src >> 20) & 15;
        *destCursor++ = (*src >> 24) & 15;
        break;
    case 9:
        //CheckBufferLen(dest, destEnd, 6);
        *destCursor++ = (*src) & 15;
        *destCursor++ = (*src >> 4) & 15;
        *destCursor++ = (*src >> 8) & 31;
        *destCursor++ = (*src >> 13) & 31;
        *destCursor++ = (*src >> 18) & 31;
        *destCursor++ = (*src >> 23) & 31;
        break;
    case 10:
        //CheckBufferLen(dest, destEnd, 5);
        *destCursor++ = (*src) & 63;
        *destCursor++ = (*src >> 6) & 63;
        *destCursor++ = (*src >> 12) & 63;
        *destCursor++ = (*src >> 18) & 31;
        *destCursor++ = (*src >> 23) & 31;
        break;
    case 11:
        //CheckBufferLen(dest, destEnd, 5);
        *destCursor++ = (*src) & 31;
        *destCursor++ = (*src >> 5) & 31;
        *destCursor++ = (*src >> 10) & 63;
        *destCursor++ = (*src >> 16) & 63;
        *destCursor++ = (*src >> 22) & 63;
        break;
    case 12:
        //CheckBufferLen(dest, destEnd, 4);
        *destCursor++ = (*src) & 127;
        *destCursor++ = (*src >> 7) & 127;
        *destCursor++ = (*src >> 14) & 127;
        *destCursor++ = (*src >> 21) & 127;
        break;
    case 13:
        //CheckBufferLen(dest, destEnd, 3);
        *destCursor++ = (*src) & 1023;
        *destCursor++ = (*src >> 10) & 511;
        *destCursor++ = (*src >> 19) & 511;
        break;
    case 14:
        //CheckBufferLen(dest, destEnd, 2);
        *destCursor++ = (*src) & 16383;
        *destCursor++ = (*src >> 14) & 16383;
        break;
    case 15:
        //CheckBufferLen(dest, destEnd, 1);
        *destCursor++ = (*src) & ((1 << 28)-1);
        break;
    }
    return itemNumPerUnit[k];
}

template<typename T>
inline void S16Compressor::CheckBufferLen(T* dest, T* destEnd, uint32_t k)
{
    if (dest + k > destEnd) 
    {
        INDEXLIB_FATAL_ERROR(BufferOverflow, "decode buffer is too short, expect %d , actual %d",
                             (int)k, (int)(destEnd - dest));
    }
}

template<typename T>
inline uint32_t S16Compressor::Decode(T* dest, T* destEnd,
                                      const uint32_t* src, size_t srcLen)
{
    const uint32_t* srcCursor = src;
    T* destCursor = dest;
    int32_t left = (int32_t)srcLen;

    while ( left > 0 )
    {
        uint32_t decodedNum = DecodeOneUnit(destCursor, destEnd, srcCursor);
        srcCursor++;
        left--;
        destCursor += decodedNum;
    }

    return destCursor - dest;
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_S16_COMPRESSOR_H
