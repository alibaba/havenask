#ifndef __INDEXLIB_ATTRIBUTE_REFERENCE_TYPED_H
#define __INDEXLIB_ATTRIBUTE_REFERENCE_TYPED_H

#include <tr1/memory>
#include <autil/StringUtil.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/common/field_format/pack_attribute/attribute_reference.h"
#include "indexlib/common/field_format/pack_attribute/float_compress_convertor.h"

IE_NAMESPACE_BEGIN(common);

template <typename T>
class AttributeReferenceTyped : public AttributeReference
{
public:
    AttributeReferenceTyped(size_t offset, const std::string& attrName,
                            config::CompressTypeOption compressType,
                            int32_t fixedMultiValueCount)
        : AttributeReference(offset, attrName, compressType, fixedMultiValueCount)
    {}

    ~AttributeReferenceTyped() {}
public:
    
    size_t SetValue(char* baseAddr, size_t dataCursor,
                    const autil::ConstString& value) override;
    
    
    bool GetStrValue(const char* baseAddr, std::string& value,
                     autil::mem_pool::Pool* pool) const override;

    
    autil::ConstString GetDataValue(const char* baseAddr) const override;

    template <typename ValueType>
    bool GetValue(const char* buffer, ValueType& value,
                  autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE;
    
    bool LessThan(const char* lftAddr, const char* rhtAddr) const override;
    
private:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////

template <typename T>
template <typename ValueType>
inline bool AttributeReferenceTyped<T>::GetValue(
        const char* buffer, ValueType& value,
        autil::mem_pool::Pool* pool) const
{
    assert(mFixedMultiValueCount == -1);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
    // PackAttributeIteratorTypedTest::CheckSingleIterator(...) will report this warn
    // cause by autil::MultiChar::data() in PackAttributeIterator::GetBaseAddress
    value = *(ValueType*)(buffer + mOffset);
#pragma GCC diagnostic pop
    return true;
}

template <>
template <>
inline bool AttributeReferenceTyped<float>::GetValue(
        const char* buffer, float& value, 
        autil::mem_pool::Pool* pool) const
{
    FloatCompressConvertor convertor(mCompressType, 1);
    return convertor.GetValue(buffer + mOffset, value, pool);
}

template <typename T>
inline bool AttributeReferenceTyped<T>::GetStrValue(
        const char* baseAddr, std::string& value,
        autil::mem_pool::Pool* pool) const
{
    T typedValue;
    if (!GetValue(baseAddr, typedValue, pool))
    {
        return false;
    }
    value = autil::StringUtil::toString<T>(typedValue);
    return true;
}

#define GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                \
    template <>                                                         \
    template <>                                                         \
    inline bool AttributeReferenceTyped<autil::MultiValueType<T>>::GetValue( \
            const char* buffer, autil::MultiValueType<T>& value,        \
            autil::mem_pool::Pool* pool) const                          \
    {                                                                   \
        assert(mFixedMultiValueCount == -1);                            \
        value = *(autil::MultiValueType<T>*)(buffer + mOffset);         \
        return true;                                                    \
    }                                                                   \
    template <>                                                         \
    template <>                                                         \
    inline bool AttributeReferenceTyped<autil::MultiValueType<T>>::GetValue( \
            const char* buffer, autil::CountedMultiValueType<T>& value, \
            autil::mem_pool::Pool* pool) const                          \
    {                                                                   \
        assert(mFixedMultiValueCount == -1);                            \
        autil::MultiValueType<T> iValue;                                \
        iValue = *(autil::MultiValueType<T>*)(buffer + mOffset);        \
        value = autil::CountedMultiValueType<T>(iValue);                \
        return true;                                                    \
    }

GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int8_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int16_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int32_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int64_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint8_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint16_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint32_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint64_t);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(float);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(double);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(char);
GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(autil::MultiChar);

#undef GET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE

#define GET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(T)        \
    template <>                                                         \
    template <>                                                         \
    inline bool AttributeReferenceTyped<autil::CountedMultiValueType<T>>::GetValue( \
            const char* buffer, autil::MultiValueType<T>& value,        \
            autil::mem_pool::Pool* pool) const                          \
    {                                                                   \
        if (!pool)                                                      \
        {                                                               \
            return false;                                               \
        }                                                               \
        assert(mFixedMultiValueCount > 0);                              \
        autil::CountedMultiValueType<T> iValue((void*)(buffer + mOffset), mFixedMultiValueCount); \
        size_t encodeCountLen = autil::MultiValueFormatter::getEncodedCountLength(iValue.size()); \
        size_t bufferLen = encodeCountLen + iValue.length();            \
        char* copyBuffer = (char*)pool->allocate(bufferLen);            \
        if (!copyBuffer)                                                \
        {                                                               \
            return false;                                               \
        }                                                               \
        autil::MultiValueFormatter::encodeCount(iValue.size(), copyBuffer, bufferLen); \
        memcpy(copyBuffer + encodeCountLen, iValue.getBaseAddress(), bufferLen - encodeCountLen); \
        autil::MultiValueType<T> retValue(copyBuffer);                  \
        value = retValue;                                               \
        return true;                                                    \
    }                                                                   \
    template <>                                                         \
    template <>                                                         \
    inline bool AttributeReferenceTyped<autil::CountedMultiValueType<T>>::GetValue( \
            const char* buffer, autil::CountedMultiValueType<T>& value, \
            autil::mem_pool::Pool* pool) const                          \
    {                                                                   \
        assert(mFixedMultiValueCount > 0);                              \
        value = autil::CountedMultiValueType<T>((void*)(buffer + mOffset), mFixedMultiValueCount); \
        return true;                                                    \
    }                                                                   \


/*  specialization for counted multi float : support compress */
template <>
template <>
inline bool AttributeReferenceTyped<autil::CountedMultiValueType<float>>::GetValue(
        const char* buffer, autil::CountedMultiValueType<float>& value, 
        autil::mem_pool::Pool* pool) const
{
    FloatCompressConvertor convertor(mCompressType, mFixedMultiValueCount);
    return convertor.GetValue(buffer + mOffset, value, pool);
}

template <>
template <>
inline bool AttributeReferenceTyped<autil::CountedMultiValueType<float>>::GetValue(
        const char* buffer, autil::MultiValueType<float>& value,
        autil::mem_pool::Pool* pool) const
{
    FloatCompressConvertor convertor(mCompressType, mFixedMultiValueCount);
    return convertor.GetValue(buffer + mOffset, value, pool);
}

GET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int8_t);
GET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int16_t);
GET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int32_t);
GET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int64_t);
GET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint8_t);
GET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint16_t);
GET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint32_t);
GET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint64_t);
GET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(double);
GET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(char);
GET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(autil::MultiChar);

#undef GET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE

template <>
inline bool AttributeReferenceTyped<autil::MultiChar>::GetStrValue(
        const char* baseAddr, std::string& value,
        autil::mem_pool::Pool* pool) const
{
    autil::MultiChar typedValue;
    if (!GetValue(baseAddr, typedValue, pool))
    {
        return false;
    }
    value = std::string(typedValue.data(), typedValue.size() * sizeof(char));
    return true;
}

#define GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)            \
    template <>                                                         \
    inline bool AttributeReferenceTyped<autil::MultiValueType<T> >::GetStrValue( \
            const char* baseAddr, std::string& value,                   \
            autil::mem_pool::Pool* pool) const                          \
    {                                                                   \
        autil::MultiValueType<T> typedValue;                            \
        if (!GetValue(baseAddr, typedValue, pool))                      \
        {                                                               \
            return false;                                               \
        }                                                               \
        value.clear();                                                  \
        for (size_t i= 0; i < typedValue.size(); ++i)                   \
        {                                                               \
            std::string item = autil::StringUtil::toString<T>(typedValue[i]); \
            if (i != 0)                                                 \
            {                                                           \
                value += MULTI_VALUE_SEPARATOR;                         \
            }                                                           \
            value += item;                                              \
        }                                                               \
        return true;                                                    \
    }

template <>
inline bool AttributeReferenceTyped<autil::CountedMultiChar>::GetStrValue(
        const char* baseAddr, std::string& value,
        autil::mem_pool::Pool* pool) const
{
    autil::CountedMultiChar typedValue;
    if (!GetValue(baseAddr, typedValue))
    {
        return false;
    }
    value = std::string(typedValue.data(), typedValue.size() * sizeof(char));
    return true;
}

template <>
inline bool AttributeReferenceTyped<autil::CountedMultiValueType<float>>::GetStrValue(
        const char* baseAddr, std::string& value,
        autil::mem_pool::Pool* pool) const
{
    FloatCompressConvertor convertor(mCompressType, mFixedMultiValueCount);
    return convertor.GetStrValue(baseAddr + mOffset, value, pool);
}

template <>
inline bool AttributeReferenceTyped<float>::GetStrValue(
        const char* baseAddr, std::string& value,
        autil::mem_pool::Pool* pool) const
{
    float typedValue;
    FloatCompressConvertor convertor(mCompressType, 1);
    if (!convertor.GetValue(baseAddr + mOffset, typedValue, pool))
    {
        return false;
    }
    value = autil::StringUtil::toString<float>(typedValue);
    return true;
}

#define GET_STR_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(T)    \
    template <>                                                         \
    inline bool AttributeReferenceTyped<autil::CountedMultiValueType<T> >::GetStrValue( \
            const char* baseAddr, std::string& value,                   \
            autil::mem_pool::Pool* pool) const                          \
    {                                                                   \
        autil::CountedMultiValueType<T> typedValue;                     \
        if (!GetValue(baseAddr, typedValue))                            \
        {                                                               \
            return false;                                               \
        }                                                               \
        value.clear();                                                  \
        for (size_t i= 0; i < typedValue.size(); ++i)                   \
        {                                                               \
            std::string item = autil::StringUtil::toString<T>(typedValue[i]); \
            if (i != 0)                                                 \
            {                                                           \
                value += MULTI_VALUE_SEPARATOR;                         \
            }                                                           \
            value += item;                                              \
        }                                                               \
        return true;                                                    \
    }

GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int8_t);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int16_t);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int32_t);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int64_t);

GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint8_t);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint16_t);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint32_t);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint64_t);

GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(float);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(double);
GET_STR_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(autil::MultiChar);

GET_STR_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int8_t);
GET_STR_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int16_t);
GET_STR_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int32_t);
GET_STR_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int64_t);

GET_STR_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint8_t);
GET_STR_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint16_t);
GET_STR_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint32_t);
GET_STR_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint64_t);

GET_STR_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(double);

///////////////////////////////////////////////////////////////////////


template <typename T>
inline size_t AttributeReferenceTyped<T>::SetValue(
    char* baseAddr, size_t dataCursor, const autil::ConstString& value)
{
    assert(dataCursor == mOffset);
    T* pvalue = (T*)(baseAddr + mOffset);
    *pvalue = *(T*)value.data();
    return sizeof(T);
}

template <>
inline size_t AttributeReferenceTyped<float>::SetValue(
    char* baseAddr, size_t dataCursor, const autil::ConstString& value)
{
    assert(dataCursor == mOffset);
    assert(FloatCompressConvertor::GetSingleValueCompressLen(mCompressType) == value.length());
    memcpy(baseAddr + dataCursor, value.data(), value.length());
    return value.length();
}

#define SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                            \
    template <>                                                         \
    inline size_t AttributeReferenceTyped<autil::MultiValueType<T> >::SetValue( \
        char* baseAddr, size_t dataCursor, const autil::ConstString& value) \
    {                                                                   \
        typedef int64_t MultiValueOffsetType;                           \
        memcpy(baseAddr + dataCursor, value.data(), value.length());    \
        MultiValueOffsetType* pOffset = (MultiValueOffsetType*)(baseAddr + mOffset); \
        assert(dataCursor >= mOffset);                                  \
        *pOffset = (MultiValueOffsetType)(dataCursor - mOffset);        \
        return value.length();                                          \
    }

#define SET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(T)        \
    template <>                                                         \
    inline size_t AttributeReferenceTyped<autil::CountedMultiValueType<T> >::SetValue( \
            char* baseAddr, size_t dataCursor, const autil::ConstString& value) \
    {                                                                   \
        assert((value.size() == sizeof(T) * mFixedMultiValueCount) ||   \
               mCompressType.HasBlockFpEncodeCompress() ||              \
               mCompressType.HasFp16EncodeCompress() ||                 \
               mCompressType.HasInt8EncodeCompress());                  \
        memcpy(baseAddr + dataCursor, value.data(), value.size());      \
        assert(dataCursor >= mOffset);                                  \
        return value.size();                                            \
    }


SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int8_t);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int16_t);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int32_t);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int64_t);

SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint8_t);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint16_t);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint32_t);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint64_t);

SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(float);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(double);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(char);
SET_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(autil::MultiChar);

SET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int8_t);
SET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int16_t);
SET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int32_t);
SET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int64_t);

SET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint8_t);
SET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint16_t);
SET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint32_t);
SET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint64_t);

SET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(float);
SET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(double);
SET_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(char);


/////////////////////////////////////////////////////////////////////////////
template <typename T>
inline autil::ConstString AttributeReferenceTyped<T>::GetDataValue(
    const char* baseAddr) const
{
    return autil::ConstString(baseAddr + mOffset, sizeof(T));
}

template <>
inline autil::ConstString AttributeReferenceTyped<float>::GetDataValue(
    const char* baseAddr) const
{
    uint8_t dataLen = FloatCompressConvertor::GetSingleValueCompressLen(mCompressType);
    return autil::ConstString(baseAddr + mOffset, dataLen);
}

#define GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)           \
    template <>                                                         \
    inline autil::ConstString AttributeReferenceTyped<autil::MultiValueType<T> >::GetDataValue( \
            const char* baseAddr) const                                 \
    {                                                                   \
        autil::MultiValueType<T> value;                                 \
        GetValue(baseAddr, value);                                      \
        return autil::ConstString(value.getBaseAddress(), value.length()); \
    }

#define GET_DATA_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(T)   \
    template <>                                                         \
    inline autil::ConstString AttributeReferenceTyped<autil::CountedMultiValueType<T> >::GetDataValue( \
            const char* baseAddr) const                                 \
    {                                                                   \
        autil::CountedMultiValueType<T> value;                          \
        GetValue(baseAddr, value);                                      \
        return autil::ConstString(value.getBaseAddress(), value.length()); \
    }


template <>                                                         
inline autil::ConstString AttributeReferenceTyped<autil::CountedMultiValueType<float> >::GetDataValue( 
    const char* baseAddr) const                                 
{
    size_t dataLen = sizeof(float) * mFixedMultiValueCount;
    if (mCompressType.HasBlockFpEncodeCompress())
    {
        dataLen = util::BlockFpEncoder::GetEncodeBytesLen(mFixedMultiValueCount);
    }
    else if (mCompressType.HasFp16EncodeCompress())
    {
        dataLen = util::Fp16Encoder::GetEncodeBytesLen(mFixedMultiValueCount);
    }
    else if (mCompressType.HasInt8EncodeCompress())
    {
        dataLen = util::FloatInt8Encoder::GetEncodeBytesLen(mFixedMultiValueCount);
    }
    return autil::ConstString(baseAddr + mOffset, dataLen);
}

GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int8_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int16_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int32_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int64_t);

GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint8_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint16_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint32_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint64_t);

GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(float);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(double);
GET_DATA_VALUE_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(char);

template <>
inline autil::ConstString AttributeReferenceTyped<autil::MultiValueType<autil::MultiChar>>::GetDataValue(
    const char* baseAddr) const
{
    autil::MultiValueType<autil::MultiChar> value;
    GetValue(baseAddr, value);
    if (value.size() == 0)
    {
        // * empty multiString only store count in index format *
        // attention:
        // MultiValueType<MultiChar>::length = count + 1byte(offset), not consistent with index
        // so autil::MultiString seralize & deserialize with 1 more byte for compitablity
        return autil::ConstString(value.getBaseAddress(), autil::MultiValueFormatter::getEncodedCountLength(0));
    }
    else
    {
        return autil::ConstString(value.getBaseAddress(), value.length());
    }
}

GET_DATA_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int8_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int16_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int32_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int64_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint8_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint16_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint32_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint64_t);
GET_DATA_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(double);
GET_DATA_VALUE_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(char);


/////////////////////////////////////////////////////////////////////////////////////
template <typename T>
inline bool AttributeReferenceTyped<T>::LessThan(
        const char* lftAddr, const char* rhtAddr) const
{ 
    return *(T*)(lftAddr + mOffset) < *(T*)(rhtAddr + mOffset);
}

#define LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(T)                            \
    template <>                                                         \
    inline bool AttributeReferenceTyped<autil::MultiValueType<T> >::LessThan( \
            const char* lftAddr, const char* rhtAddr) const             \
    {                                                                   \
        INDEXLIB_THROW(misc::UnSupportedException, "not support LessThan"); \
        return false;                                                   \
    }


#define LESS_THAN_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(T)                            \
    template <>                                                         \
    inline bool AttributeReferenceTyped<autil::CountedMultiValueType<T> >::LessThan( \
            const char* lftAddr, const char* rhtAddr) const             \
    {                                                                   \
        INDEXLIB_THROW(misc::UnSupportedException, "not support LessThan"); \
        return false;                                                   \
    }

LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int8_t);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int16_t);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int32_t);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(int64_t);

LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint8_t);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint16_t);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint32_t);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(uint64_t);

LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(float);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(double);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(char);
LESS_THAN_SPECIALIZATION_FOR_MULTI_VALUE_TYPE(autil::MultiChar);

LESS_THAN_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int8_t);
LESS_THAN_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int16_t);
LESS_THAN_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int32_t);
LESS_THAN_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(int64_t);

LESS_THAN_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint8_t);
LESS_THAN_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint16_t);
LESS_THAN_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint32_t);
LESS_THAN_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(uint64_t);

LESS_THAN_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(float);
LESS_THAN_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(double);
LESS_THAN_SPECIALIZATION_FOR_COUNTED_MULTI_VALUE_TYPE(char);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_ATTRIBUTE_REFERENCE_TYPED_H
