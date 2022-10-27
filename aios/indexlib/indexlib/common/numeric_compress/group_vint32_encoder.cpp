#include "indexlib/common/numeric_compress/group_vint32_encoder.h"
#include "indexlib/common/numeric_compress/group_varint.h"
#include "indexlib/misc/exception.h"

using namespace std;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, GroupVint32Encoder);

GroupVint32Encoder::GroupVint32Encoder() 
{
}

GroupVint32Encoder::~GroupVint32Encoder() 
{
}

uint32_t GroupVint32Encoder::Encode(
        common::ByteSliceWriter& sliceWriter,
        const uint32_t* src, uint32_t srcLen) const 
{
    // encode len using int16_t.because of MAX_RECORD_SIZE is 128
    // 4 int32 need max 17 bytes to compress  
    uint8_t buffer[ENCODER_BUFFER_SIZE];
    uint32_t encodeLen = GroupVarint::Compress(buffer, 
            ENCODER_BUFFER_SIZE, src, srcLen);

    uint32_t headLen = sizeof(int16_t);

    if (srcLen != 128)
    {
        sliceWriter.WriteInt16((int16_t)encodeLen | SRC_LEN_FLAG);
        sliceWriter.WriteByte((uint8_t)srcLen);
        headLen = sizeof(int16_t) + sizeof(uint8_t);
    }
    else
    {
        sliceWriter.WriteInt16((int16_t)encodeLen);
    }
    sliceWriter.Write(buffer, encodeLen);
    return encodeLen + headLen;
}

uint32_t GroupVint32Encoder::Encode(uint8_t *dest, 
                                    const uint32_t* src, 
                                    uint32_t srcLen) const
{
    uint32_t destLen = srcLen << 3;
    return GroupVarint::Compress(dest, destLen, src, srcLen);
}
    
uint32_t GroupVint32Encoder::Decode(uint32_t* dest, uint32_t destLen,
                                    common::ByteSliceReader& sliceReader) const
{
    uint32_t compLen = (uint32_t)sliceReader.ReadInt16();
    uint32_t srcLen = 128;
    
    if (compLen & SRC_LEN_FLAG)
    {
        srcLen = (uint32_t)sliceReader.ReadByte(); 
        compLen &= ~SRC_LEN_FLAG;
    }
    
    uint8_t buffer[ENCODER_BUFFER_SIZE];
    void* bufPtr = buffer;
    size_t len = sliceReader.ReadMayCopy(bufPtr, compLen);
    if (len != compLen)
    {
        INDEXLIB_THROW(misc::IndexCollapsedException, "GroupVarint Decode FAILED.");
    }
    return GroupVarint::Decompress(dest, destLen, (uint8_t*)bufPtr, srcLen);
}

IE_NAMESPACE_END(common);

