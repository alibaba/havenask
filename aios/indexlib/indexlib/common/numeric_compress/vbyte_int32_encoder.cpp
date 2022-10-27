#include "indexlib/common/numeric_compress/vbyte_int32_encoder.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"

using namespace std;

IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, VbyteInt32Encoder);

VbyteInt32Encoder::VbyteInt32Encoder() 
{
}

VbyteInt32Encoder::~VbyteInt32Encoder() 
{
}

uint32_t VbyteInt32Encoder::Encode(ByteSliceWriter& sliceWriter,
                                   const uint32_t* src, uint32_t srcLen) const
{
    // src len using uint8_t.because of MAX_RECORD_SIZE is 128
    uint32_t len = 1;
    sliceWriter.WriteByte((uint8_t)srcLen);
    for (uint32_t i = 0; i < srcLen; ++i)
    {
        len += sliceWriter.WriteVInt(src[i]);
    }
    return len;
}

uint32_t VbyteInt32Encoder::Encode(uint8_t *dest, const uint32_t* src, 
                                   uint32_t srcLen) const
{
    dest[0] = (uint8_t)srcLen;
    uint32_t destLen = 0;
    for (uint32_t i = 0; i < srcLen; ++i)
    {
        destLen += VByteCompressor::EncodeVInt32(dest + destLen, -1, src[i]);
    }
    return destLen;
}

uint32_t VbyteInt32Encoder::Decode(uint32_t* dest, uint32_t destLen,
                                   ByteSliceReader& sliceReader) const
{
    uint32_t len = (uint32_t)sliceReader.ReadByte();
    for (uint32_t i = 0; i < len; ++i)
    {
        dest[i] = sliceReader.ReadVInt32();
    }
    return len;
}


IE_NAMESPACE_END(common);

