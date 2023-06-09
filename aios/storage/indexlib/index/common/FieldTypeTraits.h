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
#include "indexlib/base/FieldType.h"

namespace indexlib { namespace index {

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

FIELD_TYPE_TRAITS(ft_byte1, indexlib::byte1_t);
FIELD_TYPE_TRAITS(ft_byte2, indexlib::byte2_t);
FIELD_TYPE_TRAITS(ft_byte3, indexlib::byte3_t);
FIELD_TYPE_TRAITS(ft_byte4, indexlib::byte4_t);
FIELD_TYPE_TRAITS(ft_byte5, indexlib::byte5_t);
FIELD_TYPE_TRAITS(ft_byte6, indexlib::byte6_t);
FIELD_TYPE_TRAITS(ft_byte7, indexlib::byte7_t);
FIELD_TYPE_TRAITS(ft_byte8, indexlib::byte8_t);

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

}} // namespace indexlib::index
