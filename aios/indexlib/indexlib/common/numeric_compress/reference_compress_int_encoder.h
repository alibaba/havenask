#ifndef __INDEXLIB_REFERENCE_COMPRESS_ENCODER_H
#define __INDEXLIB_REFERENCE_COMPRESS_ENCODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/numeric_compress/int_encoder.h"

IE_NAMESPACE_BEGIN(common);

template<typename T>
class ReferenceCompressIntReader
{
public:
    ReferenceCompressIntReader()
      : mData(NULL)
      , mFirst(0)
      , mBase(0)
      , mCursor(0)
      , mLen(0)
  {}
    ~ReferenceCompressIntReader() {}
public:
    void Reset(char* buffer)
    {
        int32_t header = *(int32_t*)buffer;
        if (header < 0)
        {
            header = -header;
            mLen = header >> 16;
            mBase = *(T*)(buffer + sizeof(int32_t));
            mFirst = mBase;
            mData = buffer + sizeof(int32_t) + sizeof(T);
            mCursor = 1;
        }
        else
        {
            mLen = sizeof(T);
            mFirst = *(T*)buffer;
            mBase = 0;
            mData = buffer + sizeof(T);
            mCursor = 1;
        }
    }
    template <typename ElementType>
    inline T DoSeek(T value)
    {
        T curValue = T();
        assert(value > mBase);
        T compValue = value - mBase;
        uint32_t cursor = mCursor - 1;
        do
        {
            curValue = ((ElementType*)mData)[cursor++];
        }
        while (curValue < compValue);
        mCursor = cursor + 1;
        return curValue + mBase;
    }

    T Seek(T value)
    {
        switch (mLen)
        {
        case 1:
            return DoSeek<uint8_t>(value);
        case 2:
            return DoSeek<uint16_t>(value);
        case 4:
            return DoSeek<uint32_t>(value);
        default:
          assert(false);
        }
        return T();
    }
    T operator[] (size_t index) const
    {
        if (index == 0)
        {
            return mFirst;
        }
        char* start = mData + mLen * (index - 1);
        switch (mLen)
        {
        case 1:
            return *(uint8_t*)start + mBase;
        case 2:
          return *(uint16_t*)start + mBase;
        case 4:
          return *(uint32_t*)start + mBase;
        default:
          assert(false);
        }
        return T();
    }
    uint32_t GetCursor() const { return mCursor; }
private:
    char* mData;
    T mFirst;
    T mBase;
    uint32_t mCursor;
    uint8_t mLen;
};

template<typename T>
class ReferenceCompressIntEncoder : public IntEncoder<T>
{
public:
    ReferenceCompressIntEncoder() {}
    ~ReferenceCompressIntEncoder() {}
public:
    uint32_t Encode(common::ByteSliceWriter& sliceWriter,
                    const T* src, uint32_t srcLen) const override;
    uint32_t Decode(T* dest, uint32_t destLen,
                    common::ByteSliceReader& sliceReader) const override;
    uint32_t DecodeMayCopy(T*& dest, uint32_t destLen,
                           common::ByteSliceReader& sliceReader) const override;
public:
    static T GetFirstValue(char* buffer);
private:
    uint8_t CalculateLen(size_t maxDelta) const;
    void DecodeHeader(int32_t header, uint32_t& compressedLen, uint32_t& count) const;
private:
    static const uint32_t MAX_ITEM_COUNT = MAX_DOC_PER_RECORD;
private:
    IE_LOG_DECLARE();
};

template<typename T>
uint32_t ReferenceCompressIntEncoder<T>::Encode(common::ByteSliceWriter& sliceWriter,
        const T* src, uint32_t srcLen) const
{
    assert(srcLen);
    T base = src[0];
    size_t maxDelta = (size_t)(src[srcLen - 1] - base);
    uint8_t len = CalculateLen(maxDelta);
    if (len == sizeof(T) && srcLen == MAX_ITEM_COUNT)
    {
        // do not compress
        sliceWriter.Write((const void*)src, srcLen * sizeof(T));
        return srcLen * sizeof(T);
    }
    int32_t header = 0;
    header += len << 16;
    header += srcLen & 0xffff;
    header = -header;
    sliceWriter.Write(header);
    sliceWriter.Write(base);
    for (size_t i = 1; i < srcLen; ++i)
    {
        switch(len)
        {
        case 1:
            sliceWriter.Write((uint8_t)(src[i] - base));
            break;
        case 2:
            sliceWriter.Write((uint16_t)(src[i] - base));
            break;
        case 4:
            sliceWriter.Write((uint32_t)(src[i] - base));
            break;
        case 8:
            sliceWriter.Write((uint64_t)(src[i] - base));
            break;
        default:
            assert(false);
        }
    }
    return sizeof(int32_t) + sizeof(T) + (srcLen - 1) * len;
}

template<typename T>
uint32_t ReferenceCompressIntEncoder<T>::Decode(T* dest, uint32_t destLen,
        common::ByteSliceReader& sliceReader) const
{
    int32_t header = sliceReader.PeekInt32();
    uint32_t compressedLen = 0;
    uint32_t count = 0;
    DecodeHeader(header, compressedLen, count);
    uint32_t actualLen = sliceReader.Read((void*)dest, compressedLen);
    if (actualLen != compressedLen)
    {
        INDEXLIB_THROW(misc::IndexCollapsedException, "Decode posting FAILED.");
    }
    return count;
}

template<typename T>
uint32_t ReferenceCompressIntEncoder<T>::DecodeMayCopy(T*& dest, uint32_t destLen,
        common::ByteSliceReader& sliceReader) const
{
    int32_t header = sliceReader.PeekInt32();
    uint32_t compressedLen = 0;
    uint32_t count = 0;
    DecodeHeader(header, compressedLen, count);
    uint32_t actualLen = sliceReader.ReadMayCopy((void*&)dest, compressedLen);
    if (actualLen != compressedLen)
    {
        INDEXLIB_THROW(misc::IndexCollapsedException, "Decode posting FAILED.");
    }
    return count;
}

template<typename T>
inline T ReferenceCompressIntEncoder<T>::GetFirstValue(char* buffer)
{
    ReferenceCompressIntReader<T> reader;
    reader.Reset(buffer);
    return reader[0];
}

template<typename T>
uint8_t ReferenceCompressIntEncoder<T>::CalculateLen(size_t maxDelta) const
{
    if (maxDelta <= (size_t)0xff)
    {
        return 1;
    }
    else if (maxDelta <= (size_t)0xffff)
    {
        return 2;
    }
    return 4;
}

template<typename T>
void ReferenceCompressIntEncoder<T>::DecodeHeader(int32_t header, uint32_t& compressedLen, uint32_t& count) const
{
    if (header < 0)
    {
        header = -header;
        size_t len = header >> 16;
        count  = header & 0xffff;
        compressedLen = sizeof(int32_t) + sizeof(T) + (count - 1) * len;
    }
    else
    {
        compressedLen = sizeof(T) * MAX_ITEM_COUNT;
        count = MAX_ITEM_COUNT;
    }
}

#define REFERENCE_COMPRESS_INTENCODER_PTR(type)                                \
    typedef ReferenceCompressIntEncoder<uint##type##_t> ReferenceCompressInt##type##Encoder; \
    typedef std::tr1::shared_ptr<ReferenceCompressInt##type##Encoder> ReferenceCompressInt##type##EncoderPtr;

REFERENCE_COMPRESS_INTENCODER_PTR(8);
REFERENCE_COMPRESS_INTENCODER_PTR(16);
REFERENCE_COMPRESS_INTENCODER_PTR(32);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_REFERENCE_COMPRESS_ENCODER_H
