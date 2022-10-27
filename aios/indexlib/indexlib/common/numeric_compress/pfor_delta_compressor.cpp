#include "indexlib/common/numeric_compress/pfor_delta_compressor.h"
#include "indexlib/common/numeric_compress/unpack.h"
#include <algorithm>
#include <assert.h>
#include <iostream>

using namespace std;
IE_NAMESPACE_BEGIN(common);

ssize_t PforDeltaCompressor::Compress32(uint8_t* dest, size_t destLen, 
                                        const uint32_t* src, size_t srcLen)
{
    return Compress(dest, destLen, src, srcLen);
}

ssize_t PforDeltaCompressor::Decompress32(uint32_t* dest, size_t destLen, 
        const uint8_t* src, size_t srcLen)
{
    return Decompress(dest, destLen, src, srcLen);
}

ssize_t PforDeltaCompressor::DecompressBlock32(int32_t* dest, size_t destLen, 
        ByteSliceReader& sliceReader)
{
    return DecompressBlock(dest, destLen, sliceReader);
}

ssize_t PforDeltaCompressor::DecompressBlock32(uint32_t* dest, size_t destLen, 
        const uint8_t* src, size_t srcLen, size_t& blockLen)
{
    return DecompressBlock(dest, destLen, src, srcLen, blockLen);
}

ssize_t PforDeltaCompressor::DecompressBlock32(uint32_t* dest, size_t destLen,
        ByteSliceReader& sliceReader)
{
    return DecompressBlock(dest, destLen, sliceReader);
}

ssize_t PforDeltaCompressor::Compress16(uint8_t* dest, size_t destLen, 
                                        const uint16_t* src, size_t srcLen)
{
    return Compress(dest, destLen, src, srcLen);
}

ssize_t PforDeltaCompressor::Decompress16(uint16_t* dest, size_t destLen, 
        const uint8_t* src, size_t srcLen)
{
    return Decompress(dest, destLen, src, srcLen);
}

ssize_t PforDeltaCompressor::DecompressBlock16(uint16_t* dest, size_t destLen, 
        const uint8_t* src, size_t srcLen, size_t& blockLen)
{
    return DecompressBlock(dest, destLen, src, srcLen, blockLen);
}

ssize_t PforDeltaCompressor::DecompressBlock16(uint16_t* dest, size_t destLen,
        ByteSliceReader& sliceReader)
{
    return DecompressBlock(dest, destLen, sliceReader);
}

ssize_t PforDeltaCompressor::Compress8(uint8_t* dest, size_t destLen, 
                                       const uint8_t* src, size_t srcLen)
{
    return Compress(dest, destLen, src, srcLen);
}

ssize_t PforDeltaCompressor::Decompress8(uint8_t* dest, size_t destLen, 
        const uint8_t* src, size_t srcLen)
{
    return Decompress(dest, destLen, src, srcLen);
}

ssize_t PforDeltaCompressor::DecompressBlock8(uint8_t* dest, size_t destLen, 
        const uint8_t* src, size_t srcLen, size_t& blockLen)
{
    return DecompressBlock(dest, destLen, src, srcLen, blockLen);
}

ssize_t PforDeltaCompressor::DecompressBlock8(uint8_t* dest, size_t destLen,
         ByteSliceReader& sliceReader)
{
    return DecompressBlock(dest, destLen, sliceReader);
}


template <typename Type>
ssize_t PforDeltaCompressor::Compress(uint8_t* dest, size_t destLen, const Type* src, size_t srcLen)
{
    uint8_t* destPtr = dest;
    const Type* srcPtr = src;
    ssize_t totalCompressLen = 0;
    for (size_t i = 0; i < srcLen; i += PFOR_DELTA_BLOCK){
        size_t currLen = ((PFOR_DELTA_BLOCK > (srcLen - i)) ? (srcLen - i) : PFOR_DELTA_BLOCK);
        ssize_t currCompressLen = Encode(destPtr, destLen, srcPtr, currLen);
        if (currCompressLen < 0)
        {
            return -1;
        }

        srcPtr += PFOR_DELTA_BLOCK;
        destPtr += currCompressLen;
        destLen -= currCompressLen;
        totalCompressLen += currCompressLen;
    }

    return totalCompressLen;
}

template <typename Type>
uint8_t PforDeltaCompressor::GetMaxBitNum(const Type* src, size_t len)
{
    uint32_t numBitArray[32] = {0};
    for (uint32_t i = 0; i < len; ++i, ++src)
    {
        ++numBitArray[HighBitIdx(*src) - 1];
    }
    
    int32_t threshold = (int32_t)(len * 0.9 + 0.9);
    uint8_t i;
    for (i = 0; i < 32 && threshold > 0; ++i)
    {
        threshold -= numBitArray[i];
    }
    return i;
}

template <typename Type>
ssize_t PforDeltaCompressor::Encode(uint8_t* dest, size_t destLen, const Type* src, size_t srcLen)
{
    const Type* srcPtr = src;
    uint8_t bitOfNum = GetMaxBitNum(src, srcLen);
    Type maxNum = (bitOfNum >= 32) ? -1 : ((1 << bitOfNum) - 1);
    uint32_t exceptionNum = 0;
    uint32_t exceptions[PFOR_DELTA_BLOCK];
    Type buf[PFOR_DELTA_BLOCK];

    exceptions[0] = 0;
    for (uint32_t i = 0; i < srcLen; ++i, ++srcPtr)
    {
        buf[i] = *srcPtr;
        exceptions[exceptionNum] = i;
        exceptionNum += (*srcPtr > maxNum);
    }
    
    uint32_t maxGap = maxNum;
    for (uint32_t i = 0; i + 1 < exceptionNum; ++i)
    {
        buf[exceptions[i]] = exceptions[i+1] - exceptions[i];
        maxGap = max(maxGap, exceptions[i+1] - exceptions[i]);
    }
    if (exceptionNum > 0)
    {
        buf[exceptions[exceptionNum-1]] = 0;
    }

    bitOfNum = HighBitIdx(maxGap);

    Type exceptionBuffer[exceptionNum];
    for (uint32_t i = 0; i < exceptionNum; ++i)
    {
      exceptionBuffer[i] = src[exceptions[i]];
    }
    
    uint8_t exEncodeBuffer[1024];
    size_t exEncodedLen = 0;
    if (exceptionNum > 0){
        exEncodedLen = EncodeExecptions(exEncodeBuffer, 1024, exceptionBuffer, exceptionNum);
    }
    size_t encodedPartLen = GetEncodedPartLen(srcLen, bitOfNum);
    size_t compressLen = GetHeadLength() + encodedPartLen + exEncodedLen;
    if (compressLen > destLen)
    {
        return -1;
    }

    dest[0] = srcLen;
    dest[1] = bitOfNum;
    dest[2] = exceptionNum;
    dest[3] = exceptionNum == 0 ? PFOR_DELTA_BLOCK : exceptions[0];

    dest +=  GetHeadLength();
    memset(dest, 0, encodedPartLen);
    Pack((uint32_t*)dest, buf, srcLen, bitOfNum);
    
    dest += encodedPartLen;
    memcpy(dest, exEncodeBuffer, exEncodedLen);
    
    return (ssize_t)compressLen;
}


template <typename Type>
void PforDeltaCompressor::Pack(uint32_t* dest, const Type* src, size_t srcLen, uint8_t bitOfNum)
{
    uint32_t offset = 0;
    uint32_t destLoc = 0;
    for (uint32_t i = 0; i < srcLen; ++i)
    {
        dest[destLoc] |= src[i] << offset;
        if (offset + bitOfNum - 1 > 31)
        {
            dest[destLoc + 1] |= src[i] >> (32 - offset);
        }
        
        destLoc += (offset + bitOfNum) >> 5;
        offset = (offset + bitOfNum) & 31;
    }
}

template void PforDeltaCompressor::Pack(uint32_t* dest, const uint8_t* src, size_t srcLen, uint8_t bitOfNum);
template void PforDeltaCompressor::Pack(uint32_t* dest, const uint16_t* src, size_t srcLen, uint8_t bitOfNum);
template void PforDeltaCompressor::Pack(uint32_t* dest, const uint32_t* src, size_t srcLen, uint8_t bitOfNum);


template <typename Type>
ssize_t PforDeltaCompressor::Decompress(Type* dest, size_t destLen, const uint8_t* src, size_t srcLen)
{
    size_t leftSrcLen = srcLen;
    ssize_t leftDestLen = destLen;
    size_t blockLen = 0;

    ssize_t lastDecodeNum = -1;
    ssize_t totalDecodeNum = 0;

    while (leftDestLen > 0 && leftSrcLen > 0 &&
           (lastDecodeNum = DecompressBlock(dest, leftDestLen, src, leftSrcLen, blockLen)) > 0)
    {
        leftDestLen -= lastDecodeNum;
        dest += lastDecodeNum;
        leftSrcLen -= blockLen;
        src += blockLen;

        totalDecodeNum += lastDecodeNum;
    }

    if (leftSrcLen == 0)
        return totalDecodeNum;

    return -1;
}

template <typename Type>
ssize_t PforDeltaCompressor::DecompressBlock(Type* dest, size_t destLen,
        const uint8_t* src, size_t srcLen, size_t& blockLen)
{ 
    return DecompressBlockInternal(dest, destLen, src, src + GetHeadLength(),
                                   srcLen - GetHeadLength(), blockLen);
}

template <typename Type>
ssize_t PforDeltaCompressor::DecompressBlockInternal(Type* dest, size_t destLen,
        const uint8_t* srcHeader, const uint8_t* srcBody, size_t srcBodyLen, size_t& blockLen)
{ 
    uint32_t numOfInt = srcHeader[0];
    uint32_t numOfBit = srcHeader[1];    
    uint32_t numOfException = srcHeader[2];
    uint32_t firstExceptionIdx = srcHeader[3];
    if (numOfInt > PFOR_DELTA_BLOCK || numOfBit > 32 || numOfBit < 1
        || numOfException > PFOR_DELTA_BLOCK || firstExceptionIdx > PFOR_DELTA_BLOCK
        || destLen < numOfInt)
    {
        return -1;
    }
    
    uint32_t codeBytes = GetEncodedPartLen(numOfInt, numOfBit);
    
    // if (blockLen > srcBodyLen + GetHeadLength())
    // {
    //     return -1;
    // }


    Unpack(dest, (uint32_t*)(srcBody), numOfBit, numOfInt);
        
    //Type* exceptions = (Type*)(srcBody + codeBytes);
    uint32_t cur = firstExceptionIdx;    
    Type exceptions[numOfException];
    uint8_t* exceptionsPtr = (uint8_t *)(srcBody + codeBytes);
    
    size_t exEncodedLen = 0;
    if (numOfException)
    {
        exEncodedLen = DecodeExecptions(exceptions, numOfException, exceptionsPtr, numOfException);
    }

    blockLen = GetHeadLength() + codeBytes + exEncodedLen;

    for (uint32_t i = 0; i < numOfException; ++i)
    {
        if (cur >= numOfInt)
        {
            return -1;
        }
        Type next = dest[cur];
        dest[cur] = exceptions[i];
        cur += next;
    }

    return (ssize_t)numOfInt;
}

template <typename Type>
size_t PforDeltaCompressor::EncodeExecptions(uint8_t* dest, uint32_t destLen, 
        Type* src, uint32_t srcLen)
{
//     Type *exceptBuffer = (Type*)(dest);  
//     for (uint32_t i = 0; i < srcLen; i++)
//     {
//       *exceptBuffer = src[i];
//       exceptBuffer++;
//     }
//     return (size_t)(srcLen  * sizeof(Type));

    return GroupVarint::Compress(dest, destLen, (uint32_t *)src, srcLen);
    
    //size_t totalLen = 0;
    //for (uint32_t i = 0; i < srcLen; i++) 
    // {
    //  size_t num = VByteCompressor::EncodeVInt32(dest, destLen, src[i]);
    //dest += num;
    //destLen -= num;
    //totalLen += num;
    //}
    //return totalLen;
}

template <typename Type>
size_t PforDeltaCompressor::DecodeExecptions(Type* dest, uint32_t destLen, 
        uint8_t* src, uint32_t srcLen)
{
//     Type *exceptions = (Type* )src;
//     for(uint32_t i = 0; i < srcLen; ++i)
//     {
//       dest[i] = exceptions[i];
//     }

//     return (size_t)(srcLen * sizeof(Type));

    uint32_t compressLen = 0;
    GroupVarint::Decompress((uint32_t*)dest, destLen, src, srcLen, compressLen);
    return (size_t)compressLen;

  //    uint32_t len = 5 * srcLen; // max vint len
  //  uint32_t* destPtr = (uint32_t*) dest;
  // for(uint32_t i = 0; i < srcLen; ++i)
  //{
  //  *destPtr = VByteCompressor::DecodeVInt32(src, len);
  //  destPtr++;
  //  destLen--;
  //}    
}



template<typename Type>
ssize_t PforDeltaCompressor::DecompressBlock(Type* dest, size_t destLen,
        ByteSliceReader& sliceReader)
{
    uint8_t buf[PFOR_DELTA_BLOCK << 3];
    void* tmpHeaderBuf = buf;
    void* tmpBodyBuf = buf + GetHeadLength();
    uint32_t ret = sliceReader.ReadMayCopy(tmpHeaderBuf, GetHeadLength());
    assert((size_t)ret == GetHeadLength());

    size_t blockLen = GetEncodedPartLen(((uint8_t*)tmpHeaderBuf)[0], ((uint8_t*)tmpHeaderBuf)[1]) 
                      + GetExceptionPartLen<Type>(((uint8_t*)tmpHeaderBuf)[2]);
    ret = sliceReader.ReadMayCopy(tmpBodyBuf, blockLen);
    if ((size_t) ret != blockLen)
    {
        return -1;
    }
    
    size_t actualLen;
    return DecompressBlockInternal(dest, destLen, (uint8_t*)tmpHeaderBuf,
                                   (uint8_t*)tmpBodyBuf, blockLen, actualLen);
}

template <typename Type>
void PforDeltaCompressor::InternalUnpack(Type* dest, const uint32_t* src,
                                 uint32_t bitOfNum, uint32_t intNum)
{
    uint32_t offset = 0;
    uint32_t srcLoc = 0;
    uint32_t lastBit = 0;

    for (uint32_t i = 0; i < intNum; ++i)
    {
        lastBit = offset + bitOfNum;
        if (lastBit <= 32)
        {
            dest[i] = (Type)((src[srcLoc] << (32 - lastBit)) >> (32 - bitOfNum));
        }
        else         
        {
            dest[i] = (Type)(src[srcLoc] >> offset);
            dest[i] |= (Type)((src[srcLoc+1] << (64 - lastBit)) >> (32 - bitOfNum));
        }
        
        srcLoc += lastBit >> 5;
        offset = lastBit & 31;
    }
}

template <typename Type>
void PforDeltaCompressor::Unpack(Type* dest, const uint32_t* src,
                                  uint32_t bitOfNum, uint32_t intNum)
{
    typedef void (*unpack_function)(Type*, const uint32_t*, uint32_t n);
    unpack_function functions[32] =
    {
        unpack_1<Type>,  unpack_2<Type>,  unpack_3<Type>,  unpack_4<Type>,  
        unpack_5<Type>,  unpack_6<Type>,  unpack_7<Type>,  unpack_8<Type>,
        unpack_9<Type>,  unpack_10<Type>, unpack_11<Type>, unpack_12<Type>, 
        unpack_13<Type>, unpack_14<Type>, unpack_15<Type>, unpack_16<Type>,
        unpack_17<Type>, unpack_18<Type>, unpack_19<Type>, unpack_20<Type>, 
        unpack_21<Type>, unpack_22<Type>, unpack_23<Type>, unpack_24<Type>,
        unpack_25<Type>, unpack_26<Type>, unpack_27<Type>, unpack_28<Type>, 
        unpack_29<Type>, unpack_30<Type>, unpack_31<Type>, unpack_32<Type>
    };

    if (!(intNum & 0x3F))
    {
        (*functions[bitOfNum - 1])(dest, src, (uint32_t)intNum);
    }
    else 
    {
        InternalUnpack(dest, src, bitOfNum, intNum);
    }
}

IE_NAMESPACE_END(common);
