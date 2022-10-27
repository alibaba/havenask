#ifndef __INDEXLIB_ATTRIBUTE_READER_TRAITS_H
#define __INDEXLIB_ATTRIBUTE_READER_TRAITS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/in_mem_var_num_attribute_reader.h"

IE_NAMESPACE_BEGIN(index);

template<class T>
struct AttributeReaderTraits
{
public:
    typedef SingleValueAttributeSegmentReader<T> SegmentReader;
    typedef InMemSingleValueAttributeReader<T> InMemSegmentReader;
};

#define DECLARE_READER_TRAITS_FOR_MULTI_VALUE(type)                     \
    template<>                                                          \
    struct AttributeReaderTraits<autil::MultiValueType<type> >          \
    {                                                                   \
    public:                                                             \
        typedef MultiValueAttributeSegmentReader<type> SegmentReader;   \
        typedef InMemVarNumAttributeReader<type> InMemSegmentReader;    \
    };                                                                  \

DECLARE_READER_TRAITS_FOR_MULTI_VALUE(char)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(int8_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(uint8_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(int16_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(uint16_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(int32_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(uint32_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(int64_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(uint64_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(float)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(double)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(autil::MultiChar)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(autil::uint128_t)

#undef DECLARE_READER_TRAITS_FOR_MULTI_VALUE


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_READER_TRAITS_H
