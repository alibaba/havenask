#ifndef __INDEXLIB_PFOR_DELTA_COMPRESSOR_H
#define __INDEXLIB_PFOR_DELTA_COMPRESSOR_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/common/numeric_compress/group_varint.h"
#include "indexlib/common/numeric_compress/s9_compressor.h"
#include <tr1/memory>

IE_NAMESPACE_BEGIN(common);

class PforDeltaCompressor
{
public:
    static const uint32_t PFOR_DELTA_BLOCK = 128;
    typedef uint8_t block_len_t;

public:

    static ssize_t Compress32(uint8_t* dest, size_t destLen, const uint32_t* src, size_t srcLen) ;
    static ssize_t Decompress32(uint32_t* dest, size_t destLen, const uint8_t* src, size_t srcLen) ;
    static ssize_t DecompressBlock32(uint32_t* dest, size_t destLen, const uint8_t* src, 
            size_t srcLen, size_t& blockLen);

    static ssize_t DecompressBlock32(int32_t* dest, size_t destLen, 
            ByteSliceReader& sliceReader);
    static ssize_t DecompressBlock32(uint32_t* dest, size_t destLen, 
            ByteSliceReader& sliceReader);

    static ssize_t Compress16(uint8_t* dest, size_t destLen, const uint16_t* src, size_t srcLen) ;
    static ssize_t Decompress16(uint16_t* dest, size_t destLen, const uint8_t* src, size_t srcLen) ;
    static ssize_t DecompressBlock16(uint16_t* dest, size_t destLen, const uint8_t* src, 
            size_t srcLen, size_t& blockLen);
    static ssize_t DecompressBlock16(uint16_t* dest, size_t destLen, 
            ByteSliceReader& sliceReader);

    static ssize_t Compress8(uint8_t* dest, size_t destLen, const uint8_t* src, size_t srcLen) ;
    static ssize_t Decompress8(uint8_t* dest, size_t destLen, const uint8_t* src, size_t srcLen) ;
    static ssize_t DecompressBlock8(uint8_t* dest, size_t destLen, const uint8_t* src, 
                                    size_t srcLen, size_t& blockLen);    
    static ssize_t DecompressBlock8(uint8_t* dest, size_t destLen, 
                                    ByteSliceReader& sliceReader);
  
    template <typename Type> 
    static ssize_t Compress(uint8_t* dest, size_t destLen, const Type* src, size_t srcLen) ;

    template <typename Type> 
    static ssize_t Decompress(Type* dest, size_t destLen, const uint8_t* src, size_t srcLen) ;

    template <typename Type> 
    static ssize_t DecompressBlock(Type* dest, size_t destLen, const uint8_t* src, 
                                   size_t srcLen, size_t& blockLen);
    
    template<typename Type>  
    static ssize_t DecompressBlock(Type* dest, size_t destLen, ByteSliceReader& sliceReader);

    template <typename Type> 
    static void Pack(uint32_t* dest, const Type* src, size_t srcLen, uint8_t bitOfNum);

private:
    template <typename Type> 
    static ssize_t Encode(uint8_t* dest, size_t destLen, const Type* src, size_t srcLen);

    template <typename Type>
    static uint8_t GetMaxBitNum(const Type* src, size_t len);

    static uint32_t HighBitIdx(uint32_t value);

    template <typename Type> 
    static ssize_t DecompressBlockInternal(Type* dest, size_t destLen, const uint8_t* srcHeader,
                                    const uint8_t* srcBody, size_t srcBodyLen, size_t& blockLen);
    template <typename Type> 
    static size_t EncodeExecptions(uint8_t* dest, uint32_t destLen, Type* src, uint32_t srcLen);

    template <typename Type> 
    static size_t DecodeExecptions(Type* dest, uint32_t destLen, uint8_t* src, uint32_t srcLen);
    template <typename Type> 
    static void InternalUnpack(Type* dest, const uint32_t* src,
                       uint32_t bitOfNum, uint32_t intNum);
    
    template <typename Type>
    static void Unpack(Type* dest, const uint32_t* src,uint32_t bitOfNum, uint32_t intNum);

    static size_t GetHeadLength() 
    {
        const size_t headLen = 4;
        return headLen;
    }

    static size_t GetEncodedPartLen(size_t intNum, uint8_t bitOfNum)
    {
        return (((intNum * bitOfNum + 31) >> 5) << 2);
    }
    
    template <typename Type>
    static size_t GetExceptionPartLen(size_t exceptionNum)
    {
        return exceptionNum * sizeof(Type);
    }

private:
    IE_LOG_DECLARE();
};

inline uint32_t PforDeltaCompressor::HighBitIdx(uint32_t value)
{
    int r = 0;

    __asm__("bsrl %1,%0\n\t"
            "cmovzl %2,%0"
            : "=&r" (r) : "rm" (value), "rm" (0));
    return r+1;
}
typedef std::tr1::shared_ptr<PforDeltaCompressor> PforDeltaCompressorPtr;

IE_NAMESPACE_END(common);

#endif //__INDEXENGINEPFOR_DELTA_COMPRESSOR_H
