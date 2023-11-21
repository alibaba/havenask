/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>

#include "autil/LongHashValue.h"
#include "autil/MultiValueType.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

template <AttrType at>
struct AttrTypeTraits {
    struct UnknownType {
    };
    typedef UnknownType AttrItemType;
};

#define ATTR_TYPE_TRAITS(at, type)                                                                                     \
    template <>                                                                                                        \
    struct AttrTypeTraits<at> {                                                                                        \
        typedef type AttrItemType;                                                                                     \
    }
ATTR_TYPE_TRAITS(AT_INT8, int8_t);
ATTR_TYPE_TRAITS(AT_INT16, int16_t);
ATTR_TYPE_TRAITS(AT_INT32, int32_t);
ATTR_TYPE_TRAITS(AT_INT64, int64_t);
ATTR_TYPE_TRAITS(AT_UINT8, uint8_t);
ATTR_TYPE_TRAITS(AT_UINT16, uint16_t);
ATTR_TYPE_TRAITS(AT_UINT32, uint32_t);
ATTR_TYPE_TRAITS(AT_UINT64, uint64_t);
ATTR_TYPE_TRAITS(AT_FLOAT, float);
ATTR_TYPE_TRAITS(AT_DOUBLE, double);
ATTR_TYPE_TRAITS(AT_STRING, autil::MultiChar);
ATTR_TYPE_TRAITS(AT_HASH_64, uint64_t);
ATTR_TYPE_TRAITS(AT_HASH_128, autil::uint128_t);
#undef ATTR_TYPE_TRAITS

template <FieldType ft>
struct FieldTypeTraits {
    struct UnknownType {
    };
    typedef UnknownType AttrItemType;
    static std::string TypeName;
};

#define FIELD_TYPE_TRAITS(ft, type)                                                                                    \
    template <>                                                                                                        \
    struct FieldTypeTraits<ft> {                                                                                       \
        typedef type AttrItemType;                                                                                     \
        static std::string GetTypeName() { return std::string(#type); }                                                \
    };

FIELD_TYPE_TRAITS(ft_int8, int8_t);
FIELD_TYPE_TRAITS(ft_int16, int16_t);
FIELD_TYPE_TRAITS(ft_int32, int32_t);
FIELD_TYPE_TRAITS(ft_int64, int64_t);

FIELD_TYPE_TRAITS(ft_uint8, uint8_t);
FIELD_TYPE_TRAITS(ft_uint16, uint16_t);
FIELD_TYPE_TRAITS(ft_uint32, uint32_t);
FIELD_TYPE_TRAITS(ft_uint64, uint64_t);
FIELD_TYPE_TRAITS(ft_hash_64, uint64_t);

FIELD_TYPE_TRAITS(ft_float, float);
FIELD_TYPE_TRAITS(ft_fp8, int8_t);
FIELD_TYPE_TRAITS(ft_fp16, int16_t);
FIELD_TYPE_TRAITS(ft_double, double);

FIELD_TYPE_TRAITS(ft_string, autil::MultiChar);

FIELD_TYPE_TRAITS(ft_location, double);
FIELD_TYPE_TRAITS(ft_line, double);
FIELD_TYPE_TRAITS(ft_polygon, double);

FIELD_TYPE_TRAITS(ft_hash_128, autil::uint128_t)

FIELD_TYPE_TRAITS(ft_byte1, byte1_t);
FIELD_TYPE_TRAITS(ft_byte2, byte2_t);
FIELD_TYPE_TRAITS(ft_byte3, byte3_t);
FIELD_TYPE_TRAITS(ft_byte4, byte4_t);
FIELD_TYPE_TRAITS(ft_byte5, byte5_t);
FIELD_TYPE_TRAITS(ft_byte6, byte6_t);
FIELD_TYPE_TRAITS(ft_byte7, byte7_t);
FIELD_TYPE_TRAITS(ft_byte8, byte8_t);

FIELD_TYPE_TRAITS(ft_time, uint32_t);
FIELD_TYPE_TRAITS(ft_date, uint32_t);
FIELD_TYPE_TRAITS(ft_timestamp, uint64_t);

#define GET_FIELD_TYPE_SIZE(ft_type)                                                                                   \
    case ft_type: {                                                                                                    \
        FieldTypeTraits<ft_type>::AttrItemType T;                                                                      \
        return sizeof(T);                                                                                              \
    } break

inline uint32_t SizeOfFieldType(FieldType type)
{
    switch (type) {
        GET_FIELD_TYPE_SIZE(ft_int32);
        GET_FIELD_TYPE_SIZE(ft_uint32);
        GET_FIELD_TYPE_SIZE(ft_float);
        GET_FIELD_TYPE_SIZE(ft_fp8);
        GET_FIELD_TYPE_SIZE(ft_fp16);
        GET_FIELD_TYPE_SIZE(ft_int64);
        GET_FIELD_TYPE_SIZE(ft_uint64);
        GET_FIELD_TYPE_SIZE(ft_hash_64);
        GET_FIELD_TYPE_SIZE(ft_hash_128);
        GET_FIELD_TYPE_SIZE(ft_int8);
        GET_FIELD_TYPE_SIZE(ft_uint8);
        GET_FIELD_TYPE_SIZE(ft_int16);
        GET_FIELD_TYPE_SIZE(ft_uint16);
        GET_FIELD_TYPE_SIZE(ft_double);
        GET_FIELD_TYPE_SIZE(ft_location);
        GET_FIELD_TYPE_SIZE(ft_line);
        GET_FIELD_TYPE_SIZE(ft_polygon);
        GET_FIELD_TYPE_SIZE(ft_time);
        GET_FIELD_TYPE_SIZE(ft_date);
        GET_FIELD_TYPE_SIZE(ft_timestamp);
    default:
        assert(false);
        break;
    }
    return 0;
}

#define FIX_LEN_FIELD_MACRO_HELPER(MACRO)                                                                              \
    MACRO(ft_int8);                                                                                                    \
    MACRO(ft_int16);                                                                                                   \
    MACRO(ft_int32);                                                                                                   \
    MACRO(ft_int64);                                                                                                   \
    MACRO(ft_uint8);                                                                                                   \
    MACRO(ft_uint16);                                                                                                  \
    MACRO(ft_uint32);                                                                                                  \
    MACRO(ft_uint64);                                                                                                  \
    MACRO(ft_float);                                                                                                   \
    MACRO(ft_fp8);                                                                                                     \
    MACRO(ft_fp16);                                                                                                    \
    MACRO(ft_location);                                                                                                \
    MACRO(ft_line);                                                                                                    \
    MACRO(ft_polygon);                                                                                                 \
    MACRO(ft_double);                                                                                                  \
    MACRO(ft_byte1);                                                                                                   \
    MACRO(ft_byte2);                                                                                                   \
    MACRO(ft_byte3);                                                                                                   \
    MACRO(ft_byte4);                                                                                                   \
    MACRO(ft_byte5);                                                                                                   \
    MACRO(ft_byte6);                                                                                                   \
    MACRO(ft_byte7);                                                                                                   \
    MACRO(ft_byte8);                                                                                                   \
    MACRO(ft_timestamp);                                                                                               \
    MACRO(ft_time);                                                                                                    \
    MACRO(ft_date);

#define NUMBER_FIELD_MACRO_HELPER(MACRO)                                                                               \
    MACRO(ft_int8);                                                                                                    \
    MACRO(ft_int16);                                                                                                   \
    MACRO(ft_int32);                                                                                                   \
    MACRO(ft_int64);                                                                                                   \
    MACRO(ft_uint8);                                                                                                   \
    MACRO(ft_uint16);                                                                                                  \
    MACRO(ft_uint32);                                                                                                  \
    MACRO(ft_uint64);                                                                                                  \
    MACRO(ft_float);                                                                                                   \
    MACRO(ft_fp8);                                                                                                     \
    MACRO(ft_fp16);                                                                                                    \
    MACRO(ft_location);                                                                                                \
    MACRO(ft_line);                                                                                                    \
    MACRO(ft_polygon);                                                                                                 \
    MACRO(ft_double);                                                                                                  \
    MACRO(ft_time);                                                                                                    \
    MACRO(ft_date);                                                                                                    \
    MACRO(ft_timestamp);

#define BASIC_NUMBER_FIELD_MACRO_HELPER(MACRO)                                                                         \
    MACRO(ft_int8);                                                                                                    \
    MACRO(ft_int16);                                                                                                   \
    MACRO(ft_int32);                                                                                                   \
    MACRO(ft_int64);                                                                                                   \
    MACRO(ft_uint8);                                                                                                   \
    MACRO(ft_uint16);                                                                                                  \
    MACRO(ft_uint32);                                                                                                  \
    MACRO(ft_uint64);                                                                                                  \
    MACRO(ft_float);                                                                                                   \
    MACRO(ft_double);

#define NUMBER_ATTRIBUTE_MACRO_HELPER(MACRO)                                                                           \
    MACRO(AT_INT8);                                                                                                    \
    MACRO(AT_INT16);                                                                                                   \
    MACRO(AT_INT32);                                                                                                   \
    MACRO(AT_INT64);                                                                                                   \
    MACRO(AT_UINT8);                                                                                                   \
    MACRO(AT_UINT16);                                                                                                  \
    MACRO(AT_UINT32);                                                                                                  \
    MACRO(AT_UINT64);                                                                                                  \
    MACRO(AT_FLOAT);                                                                                                   \
    MACRO(AT_DOUBLE);

static inline AttrType TransFieldTypeToAttrType(FieldType fieldType)
{
    switch (fieldType) {
    case ft_int8:
        return AT_INT8;
    case ft_int16:
        return AT_INT16;
    case ft_int32:
        return AT_INT32;
    case ft_int64:
        return AT_INT64;
    case ft_uint8:
        return AT_UINT8;
    case ft_uint16:
        return AT_UINT16;
    case ft_uint32:
        return AT_UINT32;
    case ft_uint64:
        return AT_UINT64;
    case ft_hash_64:
        return AT_HASH_64;
    case ft_hash_128:
        return AT_HASH_128;
    case ft_float:
        return AT_FLOAT;
    case ft_double:
        return AT_DOUBLE;
    case ft_string:
        return AT_STRING;
    default:
        return AT_UNKNOWN;
    }
}
}} // namespace indexlib::config
