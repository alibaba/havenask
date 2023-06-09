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

#include "autil/LongHashValue.h"
#include "autil/MultiValueType.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/base/proto/query.pb.h"
#include "indexlib/index/common/Types.h"

namespace indexlib::index {

// AttrType --> AttrItemType
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

// FieldType --> AttrItemType, ValueType, AttrMultiType
template <FieldType ft>
struct AttrTypeTraits2 {
    using ValueType = indexlibv2::base::ValueType;
    struct UnknownType {
    };
    static constexpr ValueType valueType = ValueType::STRING;
    typedef UnknownType AttrItemType;
    typedef UnknownType AttrMultiType;
};

#define ATTR_TYPE_TRAIT(ft, vtype, multitype)                                                                          \
    template <>                                                                                                        \
    struct AttrTypeTraits2<ft> {                                                                                       \
        using ValueType = indexlibv2::base::ValueType;                                                                 \
        static constexpr ValueType valueType = indexlibv2::base::vtype;                                                \
        typedef indexlibv2::base::multitype AttrMultiType;                                                             \
    };

ATTR_TYPE_TRAIT(ft_string, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_int32, ValueType::INT_32, MultiInt32Value)
ATTR_TYPE_TRAIT(ft_float, ValueType::FLOAT, MultiFloatValue)
ATTR_TYPE_TRAIT(ft_int64, ValueType::INT_64, MultiInt64Value)
ATTR_TYPE_TRAIT(ft_time, ValueType::UINT_32, MultiUInt32Value)
ATTR_TYPE_TRAIT(ft_location, ValueType::DOUBLE, MultiDoubleValue)
ATTR_TYPE_TRAIT(ft_polygon, ValueType::DOUBLE, MultiDoubleValue)
ATTR_TYPE_TRAIT(ft_line, ValueType::DOUBLE, MultiDoubleValue)
ATTR_TYPE_TRAIT(ft_uint32, ValueType::UINT_32, MultiUInt32Value)
ATTR_TYPE_TRAIT(ft_uint64, ValueType::UINT_64, MultiUInt64Value)
ATTR_TYPE_TRAIT(ft_hash_64, ValueType::UINT_64, MultiUInt64Value)
ATTR_TYPE_TRAIT(ft_int8, ValueType::INT_8, MultiInt32Value)
ATTR_TYPE_TRAIT(ft_uint8, ValueType::UINT_8, MultiUInt32Value)
ATTR_TYPE_TRAIT(ft_int16, ValueType::INT_16, MultiInt32Value)
ATTR_TYPE_TRAIT(ft_uint16, ValueType::UINT_16, MultiUInt32Value)
ATTR_TYPE_TRAIT(ft_double, ValueType::DOUBLE, MultiDoubleValue)
ATTR_TYPE_TRAIT(ft_fp8, ValueType::INT_8, MultiInt32Value)
ATTR_TYPE_TRAIT(ft_fp16, ValueType::INT_16, MultiInt32Value)
ATTR_TYPE_TRAIT(ft_date, ValueType::INT_32, MultiInt32Value)
ATTR_TYPE_TRAIT(ft_timestamp, ValueType::UINT_64, MultiUInt64Value)
ATTR_TYPE_TRAIT(ft_text, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_enum, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_online, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_property, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_hash_128, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_raw, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_unknown, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_byte1, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_byte2, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_byte3, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_byte4, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_byte5, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_byte6, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_byte7, ValueType::STRING, MultiBytesValue)
ATTR_TYPE_TRAIT(ft_byte8, ValueType::STRING, MultiBytesValue)
#undef ATTR_TYPE_TRAIT

// FieldType --> AttrType
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

} // namespace indexlib::index
