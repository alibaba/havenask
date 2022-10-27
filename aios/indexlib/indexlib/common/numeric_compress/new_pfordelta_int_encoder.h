#ifndef __INDEXLIB_NEW_PFORDELTA_INT_ENCODER_H
#define __INDEXLIB_NEW_PFORDELTA_INT_ENCODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/common/numeric_compress/int_encoder.h"
#include "indexlib/common/numeric_compress/new_pfordelta_compressor.h"
#include "indexlib/common/numeric_compress/nosse_new_pfordelta_compressor.h"
#include "indexlib/index_define.h"

IE_NAMESPACE_BEGIN(common);

template<typename T, typename Compressor>
class NewPForDeltaIntEncoder : public IntEncoder<T>
{
public:
    const static size_t ENCODER_BUFFER_SIZE = 
        MAX_RECORD_SIZE + MAX_RECORD_SIZE / 2;
    const static size_t ENCODER_BUFFER_BYTE_SIZE = 
        ENCODER_BUFFER_SIZE * sizeof(uint32_t);

public:
    NewPForDeltaIntEncoder() {}
    ~NewPForDeltaIntEncoder() {}
public:
    uint32_t Encode(common::ByteSliceWriter& sliceWriter,
                    const T* src, uint32_t srcLen) const override;
    uint32_t Encode(uint8_t *dest, const T* src, 
                    uint32_t srcLen) const override;
    uint32_t Decode(T* dest, uint32_t destLen,
                    common::ByteSliceReader& sliceReader) const override;

    // size_t GetCompressedLength(uint32_t head)
    // {
    //     return mCompressor.GetCompressedLength(head) * sizeof(uint32_t);
    // }    
private:
    Compressor mCompressor;

private:
    IE_LOG_DECLARE();
};

template<typename T, typename Compressor>
uint32_t NewPForDeltaIntEncoder<T, Compressor>::Encode(common::ByteSliceWriter& sliceWriter,
        const T* src, uint32_t srcLen) const
{
    uint8_t buffer[ENCODER_BUFFER_BYTE_SIZE];
    uint32_t encodeLen = Encode(buffer, src, srcLen);
    sliceWriter.Write((const uint8_t*)buffer, encodeLen);
    return encodeLen;
}

template<typename T, typename Compressor>
uint32_t NewPForDeltaIntEncoder<T, Compressor>::Encode(uint8_t *dest, const T* src,
        uint32_t srcLen) const
{
    return (uint32_t)mCompressor.Compress((uint32_t*)dest,
            ENCODER_BUFFER_SIZE, src, srcLen) * sizeof(uint32_t);
}

template<typename T, typename Compressor>
uint32_t NewPForDeltaIntEncoder<T, Compressor>::Decode(T* dest, uint32_t destLen,
        common::ByteSliceReader& sliceReader) const
{
    uint8_t buffer[ENCODER_BUFFER_BYTE_SIZE];
    uint32_t header = (uint32_t)sliceReader.PeekInt32();
    size_t compLen = mCompressor.GetCompressedLength(header) * sizeof(uint32_t);
    assert(compLen <= ENCODER_BUFFER_BYTE_SIZE);
    void* bufPtr = buffer;
    size_t len = sliceReader.ReadMayCopy(bufPtr, compLen);
    if (len != compLen)
    {
        INDEXLIB_THROW(misc::IndexCollapsedException, "Decode posting FAILED.");
    }
    return (uint32_t)mCompressor.Decompress(dest, destLen, 
            (const uint32_t*)bufPtr, compLen);
}
#define NEW_PFORDELTA_INTENCODER_PTR(type)                              \
    typedef NewPForDeltaIntEncoder<uint##type##_t, common::NewPForDeltaCompressor> NewPForDeltaInt##type##Encoder; \
    typedef std::tr1::shared_ptr<NewPForDeltaInt##type##Encoder> NewPForDeltaInt##type##EncoderPtr;

NEW_PFORDELTA_INTENCODER_PTR(8);
NEW_PFORDELTA_INTENCODER_PTR(16);
NEW_PFORDELTA_INTENCODER_PTR(32);

#define NOSSE_NEW_PFORDELTA_INTENCODER_PTR(type)                        \
    typedef NewPForDeltaIntEncoder<uint##type##_t, common::NosseNewPForDeltaCompressor> NoSseNewPForDeltaInt##type##Encoder; \
    typedef std::tr1::shared_ptr<NoSseNewPForDeltaInt##type##Encoder> NoSseNewPForDeltaInt##type##EncoderPtr;

NOSSE_NEW_PFORDELTA_INTENCODER_PTR(32);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_NEW_PFORDELTA_INT_ENCODER_H
