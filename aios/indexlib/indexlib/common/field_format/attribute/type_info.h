#ifndef __INDEXLIB_TYPE_INFO_H
#define __INDEXLIB_TYPE_INFO_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include <autil/LongHashValue.h>
#include <autil/StringUtil.h>
#include <autil/HashAlgorithm.h>
#include <math.h>
#include <autil/MultiValueType.h>

IE_NAMESPACE_BEGIN(common);

template <typename T>
class TypeInfo
{
public:
    static FieldType GetFieldType();
    static AttrType GetAttrType();
};

template <typename T>
FieldType TypeInfo<T>::GetFieldType()
{
    assert(false);
    return ft_unknown;
}

template <>
inline FieldType TypeInfo<uint32_t>::GetFieldType() { return ft_uint32;}


template <typename T>
AttrType TypeInfo<T>::GetAttrType()
{
    assert(false);
    return AT_UNKNOWN;
}

template<typename T>
uint32_t StrToT(const std::string& attributeValue, uint8_t* buf);

template<typename T>
FieldType GetHashFieldType(T)
{
    return ft_unknown;
}


template<typename T>
void StrToHash(const std::string& attributeValue, T& value)
{
    assert(false);
}

template<typename T>
bool CheckT(T a, T b)
{
    return a == b;
}

//////////////////////////// Field Type ///////////////////////
template <>
inline FieldType TypeInfo<int8_t>::GetFieldType() { return ft_int8;}

template <>
inline FieldType TypeInfo<uint8_t>::GetFieldType() { return ft_uint8;}

template <>
inline FieldType TypeInfo<int16_t>::GetFieldType() { return ft_int16;}

template <>
inline FieldType TypeInfo<uint16_t>::GetFieldType() { return ft_uint16;}

template <>
inline FieldType TypeInfo<int32_t>::GetFieldType() { return ft_integer;}

template <>
inline FieldType TypeInfo<int64_t>::GetFieldType() { return ft_long;}

template <>
inline FieldType TypeInfo<uint64_t>::GetFieldType() { return ft_uint64;}

template <>
inline FieldType TypeInfo<autil::uint128_t>::GetFieldType() { return ft_hash_128;}

template <>
inline FieldType TypeInfo<float>::GetFieldType() { return ft_float;}

template <>
inline FieldType TypeInfo<double>::GetFieldType() { return ft_double;}

template <>
inline FieldType TypeInfo<autil::MultiChar>::GetFieldType() { return ft_string;}

//////////////////////////// Attr Type ///////////////////////

template <>
inline AttrType TypeInfo<int32_t>::GetAttrType()
{
    return AT_INT32;
}

template <>
inline AttrType TypeInfo<int64_t>::GetAttrType()
{
    return AT_INT64;
}


template <>
inline AttrType TypeInfo<float>::GetAttrType()
{
    return AT_FLOAT;
}

template <> 
inline AttrType TypeInfo<uint32_t>::GetAttrType()
{
    return AT_UINT32;
}

template <> 
inline AttrType TypeInfo<uint64_t>::GetAttrType()
{
    return AT_UINT64;
}

template <>
inline AttrType TypeInfo<int16_t>::GetAttrType()
{
    return AT_INT16;
}

template <>
inline AttrType TypeInfo<uint16_t>::GetAttrType()
{
    return AT_UINT16;
}

template <>
inline AttrType TypeInfo<int8_t>::GetAttrType()
{
    return AT_INT8;
}

template <>
inline AttrType TypeInfo<uint8_t>::GetAttrType()
{
    return AT_UINT8;
}

template <>
inline AttrType TypeInfo<double>::GetAttrType()
{
    return AT_DOUBLE;
}

template <>
inline AttrType TypeInfo<autil::uint128_t>::GetAttrType()
{
    return AT_HASH_128;
}

template <>
inline AttrType TypeInfo<autil::MultiChar>::GetAttrType()
{
    return AT_STRING;
}

//////////////////////////// StrToT ///////////////////////
template <typename T>
inline bool StrToT(const char *str, size_t size, T &value)
{
    assert(false);
    return false;
}

template <typename T>
inline bool StrToT(const autil::ConstString &str, T& value) {

    #define MAX_NUMERIC_VALUE_LEN 4096
    if (str.size() >= MAX_NUMERIC_VALUE_LEN)
    {
        return false;
    }
    #undef MAX_NUMERIC_VALUE_LEN

    char buffer[str.size() + 1];
    memcpy(buffer, str.data(), str.size());
    buffer[str.size()] = '\0';
    return StrToT(buffer, str.size(), value);
}

template <typename T>
inline bool StrToT(const std::string &str, T& value) {
    return StrToT(str.data(), str.size(), value);
}

#define STRING_TO_T_FUNCTION_TEMPLATE(type, convertFunc)                \
    template <>                                                         \
    inline bool StrToT(const char *str, size_t size, type& value)       \
    {                                                                   \
        if (!str)                                                       \
        {                                                               \
            value = 0;                                                  \
        }                                                               \
        else if (!convertFunc(str, value))                              \
        {                                                               \
            value = 0;                                                  \
            return false;                                               \
        }                                                               \
        return true;                                                    \
    }

STRING_TO_T_FUNCTION_TEMPLATE(int8_t, autil::StringUtil::strToInt8);
STRING_TO_T_FUNCTION_TEMPLATE(uint8_t, autil::StringUtil::strToUInt8);
STRING_TO_T_FUNCTION_TEMPLATE(int16_t, autil::StringUtil::strToInt16);
STRING_TO_T_FUNCTION_TEMPLATE(uint16_t, autil::StringUtil::strToUInt16);
STRING_TO_T_FUNCTION_TEMPLATE(int32_t, autil::StringUtil::strToInt32);
STRING_TO_T_FUNCTION_TEMPLATE(uint32_t, autil::StringUtil::strToUInt32);
STRING_TO_T_FUNCTION_TEMPLATE(int64_t, autil::StringUtil::strToInt64);
STRING_TO_T_FUNCTION_TEMPLATE(uint64_t, autil::StringUtil::strToUInt64);
STRING_TO_T_FUNCTION_TEMPLATE(float, autil::StringUtil::strToFloat);
STRING_TO_T_FUNCTION_TEMPLATE(double, autil::StringUtil::strToDouble);

#undef STRING_TO_T_FUNCTION_TEMPLATE

//////////////////////////// StrToT /////////////////////////////////////
template <>
inline uint32_t StrToT<uint8_t>(const std::string& str, uint8_t* buf)
{
    uint8_t value;
    if (str.empty() || !autil::StringUtil::strToUInt8(str.data(), value))
    {
        return 0;
    }
    *buf = value;
    return sizeof(uint8_t);
}

template <>
inline uint32_t StrToT<int8_t>(const std::string& str, uint8_t* buf)
{
    int8_t value;
    if (str.empty() || !autil::StringUtil::strToInt8(str.data(), value))
    {
        return 0;
    }
    *(int8_t *)buf = value;
    return sizeof(int8_t);
}

template <>
inline uint32_t StrToT<uint16_t>(const std::string& str, uint8_t* buf)
{
    uint16_t value;
    if (str.empty() || !autil::StringUtil::strToUInt16(str.data(), value))
    {
        return 0;
    }
    *(uint16_t *)buf = value;
    return sizeof(uint16_t);
}

template <>
inline uint32_t StrToT<int16_t>(const std::string& str, uint8_t* buf)
{
    int16_t value;
    if (str.empty() || !autil::StringUtil::strToInt16(str.data(), value))
    {
        return 0;
    }
    *(int16_t *)buf = value;
    return sizeof(int16_t);
}

template <>
inline uint32_t StrToT<uint32_t>(const std::string& str, uint8_t* buf)
{
    uint32_t value;
    if (str.empty() || !autil::StringUtil::strToUInt32(str.data(), value))
    {
        return 0;
    }
    *(uint32_t *)buf = value;
    return sizeof(uint32_t);
}

template <>
inline uint32_t StrToT<int32_t>(const std::string& str, uint8_t* buf)
{
    int32_t value;
    if (str.empty() || !autil::StringUtil::strToInt32(str.data(), value))
    {
        return 0;
    }
    *(int32_t *)buf = value;
    return sizeof(int32_t);
}

template <>
inline uint32_t StrToT<uint64_t>(const std::string& str, uint8_t* buf)
{
    uint64_t value;
    if (str.empty() || !autil::StringUtil::strToUInt64(str.data(), value))
    {
        return 0;
    }
    *(uint64_t *)buf = value;
    return sizeof(uint64_t);
}

template <>
inline uint32_t StrToT<int64_t>(const std::string& str, uint8_t* buf)
{
    int64_t value;
    if (str.empty() || !autil::StringUtil::strToInt64(str.data(), value))
    {
        return 0;
    }
    *(int64_t *)buf = value;
    return sizeof(int64_t);
}

template <>
inline uint32_t StrToT<float>(const std::string& str, uint8_t* buf)
{
    float value;
    if (str.empty() || !autil::StringUtil::strToFloat(str.data(), value))
    {
        return 0;
    }
    *(float *)buf = value;
    return sizeof(float);
}

template <>
inline uint32_t StrToT<double>(const std::string& str, uint8_t* buf)
{
    double value;
    if (str.empty() || !autil::StringUtil::strToDouble(str.data(), value))
    {
        return 0;
    }
    *(double *)buf = value;
    return sizeof(double);
}

//////////////////////////// StrToHash //////////////////////////////////

template <> 
inline void StrToHash(const std::string& str, uint64_t& value)
{
    value = autil::HashAlgorithm::hashString64(str.data());
}

template <> 
inline void StrToHash(const std::string& str, autil::uint128_t& value)
{
    value = autil::HashAlgorithm::hashString128(str.data());
}

//////////////////////////// Hash Field Type //////////////////////////////////

template <> 
inline FieldType GetHashFieldType(uint64_t)
{
    return ft_hash_64;
}

template <> 
inline FieldType GetHashFieldType(autil::uint128_t)
{
    return ft_hash_128;
}

/////////////////////////////// checkT

template <>
inline bool CheckT(float a, float b)
{
    return fabs(a - b) < 1E-6;
}

template <>
inline bool CheckT(double a, double b)
{
    return fabs(a - b) < 1E-6;
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_TYPE_INFO_H
