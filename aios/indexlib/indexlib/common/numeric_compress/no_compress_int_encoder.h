#ifndef __INDEXLIB_NO_COMPRESS_INT_ENCODER_H
#define __INDEXLIB_NO_COMPRESS_INT_ENCODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/numeric_compress/int_encoder.h"

IE_NAMESPACE_BEGIN(common);

template<typename T>
class NoCompressIntEncoder : public IntEncoder<T>
{
public:
    NoCompressIntEncoder(bool hasLength = true) : mHasLength(hasLength)  {}
    ~NoCompressIntEncoder() {}
public:
    uint32_t Encode(common::ByteSliceWriter& sliceWriter,
                    const T* src, uint32_t srcLen) const override;
    uint32_t Decode(T* dest, uint32_t destLen,
                    common::ByteSliceReader& sliceReader) const override;
    uint32_t DecodeMayCopy(T*& dest, uint32_t destLen,
                           common::ByteSliceReader& sliceReader) const override;

private:
    bool mHasLength;

private:
    IE_LOG_DECLARE();
};

template<typename T>
uint32_t NoCompressIntEncoder<T>::Encode(common::ByteSliceWriter& sliceWriter,
        const T* src, uint32_t srcLen) const
{
    // src len using uint8_t, because MAX_RECORD_SIZE is 128
    uint32_t encodeLen = 0;
    if (mHasLength)
    {
        sliceWriter.WriteByte((uint8_t)srcLen);
        encodeLen += sizeof(uint8_t);
    }
    sliceWriter.Write((const void*)src, srcLen * sizeof(T));
    return encodeLen + srcLen * sizeof(T);
}

template<typename T>
uint32_t NoCompressIntEncoder<T>::Decode(T* dest, uint32_t destLen,
        common::ByteSliceReader& sliceReader) const
{
    uint32_t readCount = 0;
    if (mHasLength)
    {
        readCount = sliceReader.ReadByte();
    }
    else
    {
        readCount = destLen;
    }
    uint32_t actualLen = sliceReader.Read((void*)dest, readCount * sizeof(T));
    assert(mHasLength || actualLen / sizeof(T) == destLen);
    (void) actualLen;
    return readCount;
}

template<typename T>
uint32_t NoCompressIntEncoder<T>::DecodeMayCopy(T*& dest, uint32_t destLen,
        common::ByteSliceReader& sliceReader) const
{
    uint32_t readCount = 0;
    if (mHasLength)
    {
        readCount = sliceReader.ReadByte();
    }
    else
    {
        readCount = destLen;
    }
    uint32_t actualLen = sliceReader.ReadMayCopy((void*&)dest, readCount * sizeof(T));
    assert(mHasLength || actualLen / sizeof(T) == destLen);
    (void) actualLen;
    return readCount;
}

#define NO_COMPRESS_INTENCODER_PTR(type)                                \
    typedef NoCompressIntEncoder<uint##type##_t> NoCompressInt##type##Encoder; \
    typedef std::tr1::shared_ptr<NoCompressInt##type##Encoder> NoCompressInt##type##EncoderPtr;

NO_COMPRESS_INTENCODER_PTR(8);
NO_COMPRESS_INTENCODER_PTR(16);
NO_COMPRESS_INTENCODER_PTR(32);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_NO_COMPRESS_INT_ENCODER_H
