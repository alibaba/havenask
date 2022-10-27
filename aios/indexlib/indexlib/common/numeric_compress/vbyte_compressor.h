#ifndef __INDEXLIB_VBYTE_COMPRESSOR_H
#define __INDEXLIB_VBYTE_COMPRESSOR_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/exception.h"
#include <tr1/memory>

IE_NAMESPACE_BEGIN(common);

class VByteCompressor
{
public:
    /**
     * encode one int32 value to buffer
     * @return length after encode
     */
    static uint32_t EncodeVInt32(uint8_t* outputByte, uint32_t maxOutputByte, int32_t value);

    /**
     * decode one vint32 to int32
     *
     * @param inputByte input byte buffer, and will be increased by the number of bytes have been decoded
     * @param inputByte size of input byte buffer, and will be decreased by the number of bytes have been decoded. 
     * @return int32 value
     */
    static int32_t DecodeVInt32(uint8_t*& inputByte, uint32_t& inputByteLength);

    /**
     * Get the vint32 length of int32
     */
    static uint32_t GetVInt32Length(int32_t value);

    /* encode one uint32 value to buffer pointed by cursor */
    static void WriteVUInt32(uint32_t value, char*& cursor);

    static uint32_t ReadVUInt32(char*& cursor);

private:
    static uint8_t ReadByte(const uint8_t* inputByte, 
                                   uint32_t& pos, uint32_t maxInputByte);
    static void WriteByte(uint8_t* outputByte, uint32_t& pos,
                                 uint32_t maxOutputByte, uint8_t value);

private:
};

/////////////////////////////////////////////////////
//
inline uint8_t VByteCompressor::ReadByte(const uint8_t* inputByte,
        uint32_t& pos, uint32_t maxInputByte)
{
    if (pos >= maxInputByte)
    {
        INDEXLIB_THROW(misc::BufferOverflowException, "Buffer is overflow");
    }
    return inputByte[pos++];
}

inline void VByteCompressor::WriteByte(uint8_t* outputByte,
        uint32_t& pos, uint32_t maxOutputByte, uint8_t value)
{
    if (pos >= maxOutputByte)
    {
        INDEXLIB_THROW(misc::BufferOverflowException, "Buffer is overflow");
    }
    outputByte[pos++] = value;
}

inline uint32_t VByteCompressor::GetVInt32Length(int32_t value)
{
    uint8_t l = 1;
    uint32_t ui = value;
    while ((ui & ~0x7F) != 0)
    {
        l++;
        ui >>= 7; //doing unsigned shift
    }
    return l;
}

inline void VByteCompressor::WriteVUInt32(uint32_t value, char*& cursor)
{
    while (value > 0x7F)
    {
        *cursor++ = 0x80 | (value & 0x7F);
        value >>= 7;
    }
    *cursor++ = value & 0x7F;
}

inline uint32_t VByteCompressor::ReadVUInt32(char*& cursor)
{
    uint8_t byte = *(uint8_t*)cursor++;
    uint32_t value = byte & 0x7F;
    int shift = 7;
    
    while(byte & 0x80)
    {
        byte = *(uint8_t*)cursor++;
        value |= ((byte & 0x7F) << shift);
        shift += 7;
    }
    return value;
}

inline uint32_t VByteCompressor::EncodeVInt32(uint8_t* outputByte, uint32_t maxOutputByte, int32_t value)
{
    uint32_t l = 0;
    uint32_t ui = value;
    while ((ui & ~0x7F) != 0)
    {
        WriteByte(outputByte, l, maxOutputByte, (uint8_t)((ui & 0x7f) | 0x80));
        ui >>= 7;
    }
    WriteByte(outputByte, l, maxOutputByte, (uint8_t)ui);
    return l;
}

inline int32_t VByteCompressor::DecodeVInt32(uint8_t*& inputByte, uint32_t& inputByteLength)
{
    uint32_t l = 0;
    uint8_t b = ReadByte(inputByte, l, inputByteLength);
    uint32_t i = b & 0x7F;
    for (int32_t shift = 7; (b & 0x80) != 0; shift += 7) 
    {
        b = ReadByte(inputByte, l, inputByteLength);
        i |= (b & 0x7FL) << shift;
    }
    inputByte += l;
    inputByteLength -= l;
    return i;
}

IE_NAMESPACE_END(common);
#endif //__INDEXLIB_VBYTE_COMPRESSOR_H
