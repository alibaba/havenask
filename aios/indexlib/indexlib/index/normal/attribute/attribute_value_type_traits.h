#ifndef __INDEXLIB_ATTRIBUTE_VALUE_TYPE_TRAITS_H
#define __INDEXLIB_ATTRIBUTE_VALUE_TYPE_TRAITS_H

#include <tr1/memory>
#include <autil/MultiValueType.h>
#include <autil/CountedMultiValueType.h>
#include <autil/LongHashValue.h>
#include <autil/StringUtil.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

template<class T, bool isFixMultiValue = false>
struct AttributeValueTypeTraits
{
public:
    typedef T UnMountValueType;
    typedef T IndexValueType;
};

#define DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(type)         \
    template<>                                                          \
    struct AttributeValueTypeTraits<autil::MultiValueType<type>, false> \
    {                                                                   \
    public:                                                             \
        typedef autil::CountedMultiValueType<type> UnMountValueType;    \
        typedef autil::MultiValueType<type> IndexValueType;             \
    };                                                                  \
    template<>                                                          \
    struct AttributeValueTypeTraits<autil::CountedMultiValueType<type>, false> \
    {                                                                   \
    public:                                                             \
        typedef autil::CountedMultiValueType<type> UnMountValueType;    \
        typedef autil::MultiValueType<type> IndexValueType;             \
    };                                                                  \

DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(char)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(int8_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(uint8_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(int16_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(uint16_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(int32_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(uint32_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(int64_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(uint64_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(float)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(double)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(autil::MultiChar)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE(autil::uint128_t)

#undef DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_VAR_MULTI_VALUE

#define DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(type)         \
    template<>                                                          \
    struct AttributeValueTypeTraits<autil::MultiValueType<type>, true>  \
    {                                                                   \
    public:                                                             \
        typedef autil::CountedMultiValueType<type> UnMountValueType;    \
        typedef autil::CountedMultiValueType<type> IndexValueType;      \
    };                                                                  \
    template<>                                                          \
    struct AttributeValueTypeTraits<autil::CountedMultiValueType<type>, true> \
    {                                                                   \
    public:                                                             \
        typedef autil::CountedMultiValueType<type> UnMountValueType;    \
        typedef autil::CountedMultiValueType<type> IndexValueType;      \
    };                                                                  \


DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(char)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(int8_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(uint8_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(int16_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(uint16_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(int32_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(uint32_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(int64_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(uint64_t)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(float)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(double)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(autil::MultiChar)
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE(autil::uint128_t)

#undef DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_FIX_MULTI_VALUE


template <typename T>
class AttributeValueTypeToString
{
public:
    static std::string ToString(T value)
    {
        return autil::StringUtil::toString(value);
    }
};

#define DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(type)          \
    template <>                                                         \
    class AttributeValueTypeToString<autil::MultiValueType<type> >      \
    {                                                                   \
    public:                                                             \
        static std::string ToString(autil::MultiValueType<type> value)  \
        {                                                               \
            std::vector<std::string> strVec;                            \
            strVec.reserve(value.size());                               \
            for (size_t i = 0; i < value.size(); ++i) {                 \
                strVec.push_back(autil::StringUtil::toString(value[i])); \
            }                                                           \
            return autil::StringUtil::toString(strVec, INDEXLIB_MULTI_VALUE_SEPARATOR_STR); \
        }                                                               \
    };                                                                  \
    template <>                                                         \
    class AttributeValueTypeToString<autil::CountedMultiValueType<type> > \
    {                                                                   \
    public:                                                             \
        static std::string ToString(autil::CountedMultiValueType<type> value) \
        {                                                               \
            std::vector<std::string> strVec;                            \
            strVec.reserve(value.size());                               \
            for (size_t i = 0; i < value.size(); ++i) {                 \
                strVec.push_back(autil::StringUtil::toString(value[i])); \
            }                                                           \
            return autil::StringUtil::toString(strVec, INDEXLIB_MULTI_VALUE_SEPARATOR_STR); \
        }                                                               \
    };                                                                  \

DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(int8_t)
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(int16_t)
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(int32_t)
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(int64_t)
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(uint8_t)
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(uint16_t)
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(uint32_t)
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(uint64_t)
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(float)
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(double)

template <>                                                         
class AttributeValueTypeToString<autil::MultiChar>      
{                                                                   
public:                                                             
    static std::string ToString(autil::MultiChar value) 
    {                                                               
        return std::string(value.data(), value.size());
    }                                                               
};                                                                  

template <>                                                         
class AttributeValueTypeToString<autil::CountedMultiChar>      
{                                                                   
public:                                                             
    static std::string ToString(autil::CountedMultiChar value) 
    {                                                               
        return std::string(value.data(), value.size());
    }                                                               
};                                                                  

template <>                                                         
class AttributeValueTypeToString<autil::MultiString>
{                                                                   
public:                                                             
    static std::string ToString(autil::MultiString value) 
    {
        std::vector<std::string> strVec;
        strVec.reserve(value.size());
        for (size_t i = 0; i < value.size(); ++i) {
            strVec.push_back(std::string(value[i].data(), value[i].size()));
        }                                                           
        return autil::StringUtil::toString(strVec, INDEXLIB_MULTI_VALUE_SEPARATOR_STR);
    }                                                               
};                                                                  

template <>                                                         
class AttributeValueTypeToString<autil::CountedMultiString>      
{                                                                   
public:                                                             
    static std::string ToString(autil::CountedMultiString value) 
    {
        std::vector<std::string> strVec;
        strVec.reserve(value.size());
        for (size_t i = 0; i < value.size(); ++i) {
            strVec.push_back(std::string(value[i].data(), value[i].size()));
        }                                                           
        return autil::StringUtil::toString(strVec, INDEXLIB_MULTI_VALUE_SEPARATOR_STR);
    }                                                               
};                                                                  

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_VALUE_TYPE_TRAITS_H
