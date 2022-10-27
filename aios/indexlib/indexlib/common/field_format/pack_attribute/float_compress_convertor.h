#ifndef __INDEXLIB_FLOAT_COMPRESS_CONVERTOR_H
#define __INDEXLIB_FLOAT_COMPRESS_CONVERTOR_H

#include <tr1/memory>
#include <autil/MultiValueType.h>
#include <autil/CountedMultiValueType.h>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/block_fp_encoder.h"
#include "indexlib/util/fp16_encoder.h"
#include "indexlib/util/float_int8_encoder.h"
#include "indexlib/config/compress_type_option.h"

IE_NAMESPACE_BEGIN(common);

class FloatCompressConvertor
{
public:
    FloatCompressConvertor(config::CompressTypeOption compressType,
                           int32_t fixedMultiValueCount)
        : mCompressType(compressType)
        , mFixedMultiValueCount(fixedMultiValueCount)
    {}
    
    ~FloatCompressConvertor() {}
    
public:
    inline bool GetValue(const char* buffer,
                         autil::CountedMultiValueType<float>& value, 
                         autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

    inline bool GetValue(const char* buffer,
                         autil::MultiValueType<float>& value, 
                         autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

    inline bool GetStrValue(const char* baseAddr, std::string& value,
                            autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;

    inline bool GetValue(const char* buffer,
                         float& value, 
                         autil::mem_pool::Pool* pool) const __ALWAYS_INLINE;
    
    inline static size_t GetMultiValueCompressLen(const config::CompressTypeOption& compressType,
                                                  int32_t valueCount) __ALWAYS_INLINE;
    
    inline static uint8_t GetSingleValueCompressLen(
        const config::CompressTypeOption& compressType) __ALWAYS_INLINE;
    
    inline static FieldType GetActualFieldType(
        const config::CompressTypeOption& compressType) __ALWAYS_INLINE;
    
private:
    config::CompressTypeOption mCompressType;
    int32_t mFixedMultiValueCount;
};

DEFINE_SHARED_PTR(FloatCompressConvertor);

///////////////////////////////////////////////////////////////////////////////////////////
inline bool FloatCompressConvertor::GetValue(
        const char* buffer, autil::CountedMultiValueType<float>& value, 
        autil::mem_pool::Pool* pool) const
{
    assert(mFixedMultiValueCount > 0);
#define GET_VALUE_FOR_ENCODED_DATA(ENCODER)                             \
    if (!pool)                                                          \
    {                                                                   \
        return false;                                                   \
    }                                                                   \
    size_t tmpBufferSize = mFixedMultiValueCount * sizeof(float);       \
    char* tmpBuffer = (char*)pool->allocate(tmpBufferSize);             \
    if (!tmpBuffer)                                                     \
    {                                                                   \
        return false;                                                   \
    }                                                                   \
    size_t encodedSize = ENCODER::GetEncodeBytesLen(mFixedMultiValueCount); \
    autil::ConstString encodedData(buffer, encodedSize);                \
    int32_t decodedCount = ENCODER::Decode(                             \
            encodedData, (char*)tmpBuffer, tmpBufferSize);              \
    assert(decodedCount == mFixedMultiValueCount);                      \
    value = autil::CountedMultiValueType<float>(tmpBuffer, decodedCount); 
    
    if (mCompressType.HasBlockFpEncodeCompress())
    {
        GET_VALUE_FOR_ENCODED_DATA(util::BlockFpEncoder);
    }
    else if (mCompressType.HasFp16EncodeCompress())
    {
        GET_VALUE_FOR_ENCODED_DATA(util::Fp16Encoder);
    }
    else if (mCompressType.HasInt8EncodeCompress())
    {
        if (!pool)
        {
            return false;
        }
        size_t tmpBufferSize = mFixedMultiValueCount * sizeof(float);
        char* tmpBuffer = (char*)pool->allocate(tmpBufferSize);
        if (!tmpBuffer)
        {
            return false;
        }
        size_t encodedSize = util::FloatInt8Encoder::GetEncodeBytesLen(mFixedMultiValueCount);
        autil::ConstString encodedData(buffer, encodedSize);
        int32_t decodedCount = util::FloatInt8Encoder::Decode(
                mCompressType.GetInt8AbsMax(), encodedData, (char*)tmpBuffer, tmpBufferSize);
        assert(decodedCount == mFixedMultiValueCount);
        value = autil::CountedMultiValueType<float>(tmpBuffer, decodedCount); 
    }
    else
    {
        value = autil::CountedMultiValueType<float>(buffer, mFixedMultiValueCount);
    }
#undef GET_VALUE_FOR_ENCODED_DATA

    return true;                                                    
}

inline bool FloatCompressConvertor::GetValue(
        const char* buffer, autil::MultiValueType<float>& value, 
        autil::mem_pool::Pool* pool) const
{
    assert(mFixedMultiValueCount > 0);
#define GET_MULTI_VALUE_FOR_ENCODED_DATA(ENCODER)                       \
    if (!pool)                                                          \
    {                                                                   \
        return false;                                                   \
    }                                                                   \
    size_t encodeCountLen = autil::MultiValueFormatter::getEncodedCountLength( \
            mFixedMultiValueCount);                                     \
    size_t dataBufferSize = mFixedMultiValueCount * sizeof(float);      \
    char* tmpBuffer = (char*)pool->allocate(encodeCountLen + dataBufferSize); \
    if (!tmpBuffer)                                                     \
    {                                                                   \
        return false;                                                   \
    }                                                                   \
    size_t encodedSize = ENCODER::GetEncodeBytesLen(mFixedMultiValueCount); \
    autil::ConstString encodedData(buffer, encodedSize);                \
    int32_t decodedCount = ENCODER::Decode(                             \
            encodedData, (char*)(tmpBuffer + encodeCountLen), dataBufferSize); \
    assert(decodedCount == mFixedMultiValueCount);                      \
    (void)decodedCount;                                                 \
    autil::MultiValueFormatter::encodeCount(mFixedMultiValueCount, tmpBuffer, encodeCountLen); \
    value = autil::MultiValueType<float>(tmpBuffer);
    
    if (mCompressType.HasBlockFpEncodeCompress())
    {
        GET_MULTI_VALUE_FOR_ENCODED_DATA(util::BlockFpEncoder);
    }
    else if (mCompressType.HasFp16EncodeCompress())
    {
        GET_MULTI_VALUE_FOR_ENCODED_DATA(util::Fp16Encoder);
    }
    else if (mCompressType.HasInt8EncodeCompress())
    {
        if (!pool)
        {
            return false;
        }
        size_t encodeCountLen = autil::MultiValueFormatter::getEncodedCountLength(
                mFixedMultiValueCount);
        size_t dataBufferSize = mFixedMultiValueCount * sizeof(float);
        char* tmpBuffer = (char*)pool->allocate(encodeCountLen + dataBufferSize);
        if (!tmpBuffer)
        {
            return false;
        }
        size_t encodedSize = util::FloatInt8Encoder::GetEncodeBytesLen(mFixedMultiValueCount);
        autil::ConstString encodedData(buffer, encodedSize);
        int32_t decodedCount = util::FloatInt8Encoder::Decode(mCompressType.GetInt8AbsMax(),
                encodedData, (char*)(tmpBuffer + encodeCountLen), dataBufferSize);
        assert(decodedCount == mFixedMultiValueCount);
        (void)decodedCount;
        autil::MultiValueFormatter::encodeCount(mFixedMultiValueCount, tmpBuffer, encodeCountLen);
        value = autil::MultiValueType<float>(tmpBuffer);
    }
    else
    {
        if (!pool)
        {
            return false;
        }
        size_t encodeCountLen =
            autil::MultiValueFormatter::getEncodedCountLength(mFixedMultiValueCount);
        size_t dataBufferSize = mFixedMultiValueCount * sizeof(float);  
        size_t bufferLen = encodeCountLen + dataBufferSize;
        char* copyBuffer = (char*)pool->allocate(bufferLen);
        if (!copyBuffer)
        {
            return false;
        }
        autil::MultiValueFormatter::encodeCount(mFixedMultiValueCount, copyBuffer, encodeCountLen);
        memcpy(copyBuffer + encodeCountLen, buffer, dataBufferSize);
        value = autil::MultiValueType<float>(copyBuffer);
    }
#undef GET_MULTI_VALUE_FOR_ENCODED_DATA

    return true;                                                    

}

inline bool FloatCompressConvertor::GetStrValue(
        const char* buffer, std::string& value, autil::mem_pool::Pool* pool) const
{
#define GET_STR_VALUE_FOR_ENCODED_DATA(ENCODER)                         \
    std::vector<float> tmpBufferVec;                                    \
    tmpBufferVec.resize(mFixedMultiValueCount);                         \
    char* tmpBuffer = (char*)tmpBufferVec.data();                       \
    size_t encodedSize = ENCODER::GetEncodeBytesLen(mFixedMultiValueCount); \
    autil::ConstString encodedData(buffer, encodedSize);                \
    int32_t decodedCount = ENCODER::Decode(                             \
            encodedData, (char*)tmpBuffer, sizeof(float) * mFixedMultiValueCount); \
    assert(decodedCount == mFixedMultiValueCount);                      \
    (void)decodedCount;                                                 \
    value = autil::StringUtil::toString(tmpBufferVec, INDEXLIB_MULTI_VALUE_SEPARATOR_STR); 
    
    if (mCompressType.HasBlockFpEncodeCompress())
    {
        GET_STR_VALUE_FOR_ENCODED_DATA(util::BlockFpEncoder);
    }
    else if (mCompressType.HasFp16EncodeCompress())
    {
        GET_STR_VALUE_FOR_ENCODED_DATA(util::Fp16Encoder);
    }
    else if (mCompressType.HasInt8EncodeCompress())
    {
        std::vector<float> tmpBufferVec;
        tmpBufferVec.resize(mFixedMultiValueCount);
        char* tmpBuffer = (char*)tmpBufferVec.data();
        size_t encodedSize = util::FloatInt8Encoder::GetEncodeBytesLen(mFixedMultiValueCount);
        autil::ConstString encodedData(buffer, encodedSize);
        int32_t decodedCount = util::FloatInt8Encoder::Decode(mCompressType.GetInt8AbsMax(),
                encodedData, (char*)tmpBuffer, sizeof(float) * mFixedMultiValueCount);
        assert(decodedCount == mFixedMultiValueCount);
        (void)decodedCount;
        value = autil::StringUtil::toString(tmpBufferVec, INDEXLIB_MULTI_VALUE_SEPARATOR_STR);
    }
    else
    {
        autil::CountedMultiValueType<float> typedValue(buffer, mFixedMultiValueCount);
        value.clear(); 
        for (size_t i= 0; i < typedValue.size(); ++i) 
        {
            std::string item = autil::StringUtil::toString<float>(typedValue[i]); 
            if (i != 0) 
            {                                                           
                value += MULTI_VALUE_SEPARATOR;                         
            }                                                           
            value += item;                                              
        }                                                               
    }
#undef GET_STR_VALUE_FOR_ENCODED_DATA
    return true;
}

inline bool FloatCompressConvertor::GetValue(const char* buffer,
                                             float& value, 
                                             autil::mem_pool::Pool* pool) const
{
    if (mCompressType.HasFp16EncodeCompress()) {
        autil::ConstString input(buffer, sizeof(int16_t));
        if (util::Fp16Encoder::Decode(input, (char*)&value) != 1) {
            return false;
        }
        return true;
    }
    if (mCompressType.HasInt8EncodeCompress()) {
        autil::ConstString input(buffer, sizeof(int8_t));
        if (util::FloatInt8Encoder::Decode(
                mCompressType.GetInt8AbsMax(), input, (char*)&value) != 1) {
            return false;
        }
        return true;
    }
    // no compress
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
    value = *(float*)(buffer);
#pragma GCC diagnostic pop
    return true;
}

inline size_t FloatCompressConvertor::GetMultiValueCompressLen(
    const config::CompressTypeOption& compressType, int32_t valueCount) 
{
    return config::CompressTypeOption::GetMultiValueCompressLen(compressType, valueCount);
}

inline uint8_t FloatCompressConvertor::GetSingleValueCompressLen(
    const config::CompressTypeOption& compressType) 
{
    return config::CompressTypeOption::GetSingleValueCompressLen(compressType);
}

inline FieldType FloatCompressConvertor::GetActualFieldType(
    const config::CompressTypeOption& compressType) 
{
    if (compressType.HasFp16EncodeCompress())
    {
        return ft_int16;
    }
    if (compressType.HasInt8EncodeCompress())
    {
        return ft_int8;
    }
    return ft_float;
}

IE_NAMESPACE_END(common);


#endif //__INDEXLIB_FLOAT_COMPRESS_CONVERTOR_H
