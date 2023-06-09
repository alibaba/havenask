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

#include "autil/MultiValueType.h"
#include "indexlib/base/FieldType.h"

namespace indexlib {

extern bool IsIntegerField(FieldType ft);
extern bool IsNumericField(FieldType ft);

// indexlib field type --> cpp type utility: IndexlibFieldType2CppType
template <FieldType ft, bool isMulti>
struct IndexlibFieldType2CppType {
    struct UnknownType {
    };
    typedef UnknownType CppType;
};

#define IndexlibFieldType2CppTypeTraits(indexlibFieldType, isMulti, cppType)                                           \
    template <>                                                                                                        \
    struct IndexlibFieldType2CppType<indexlibFieldType, isMulti> {                                                     \
        typedef cppType CppType;                                                                                       \
    };

IndexlibFieldType2CppTypeTraits(ft_int8, false, int8_t);
IndexlibFieldType2CppTypeTraits(ft_int16, false, int16_t);
IndexlibFieldType2CppTypeTraits(ft_int32, false, int32_t);
IndexlibFieldType2CppTypeTraits(ft_int64, false, int64_t);
IndexlibFieldType2CppTypeTraits(ft_uint8, false, uint8_t);
IndexlibFieldType2CppTypeTraits(ft_uint16, false, uint16_t);
IndexlibFieldType2CppTypeTraits(ft_uint32, false, uint32_t);
IndexlibFieldType2CppTypeTraits(ft_uint64, false, uint64_t);
IndexlibFieldType2CppTypeTraits(ft_float, false, float);
IndexlibFieldType2CppTypeTraits(ft_double, false, double);
IndexlibFieldType2CppTypeTraits(ft_string, false, autil::MultiChar);
IndexlibFieldType2CppTypeTraits(ft_hash_64, false, uint64_t);
IndexlibFieldType2CppTypeTraits(ft_hash_128, false, autil::uint128_t);
IndexlibFieldType2CppTypeTraits(ft_time, false, uint32_t);
IndexlibFieldType2CppTypeTraits(ft_date, false, uint32_t);
IndexlibFieldType2CppTypeTraits(ft_timestamp, false, uint64_t);

IndexlibFieldType2CppTypeTraits(ft_int8, true, autil::MultiInt8);
IndexlibFieldType2CppTypeTraits(ft_int16, true, autil::MultiInt16);
IndexlibFieldType2CppTypeTraits(ft_int32, true, autil::MultiInt32);
IndexlibFieldType2CppTypeTraits(ft_int64, true, autil::MultiInt64);
IndexlibFieldType2CppTypeTraits(ft_uint8, true, autil::MultiUInt8);
IndexlibFieldType2CppTypeTraits(ft_uint16, true, autil::MultiUInt16);
IndexlibFieldType2CppTypeTraits(ft_uint32, true, autil::MultiUInt32);
IndexlibFieldType2CppTypeTraits(ft_uint64, true, autil::MultiUInt64);
IndexlibFieldType2CppTypeTraits(ft_float, true, autil::MultiFloat);
IndexlibFieldType2CppTypeTraits(ft_double, true, autil::MultiDouble);
IndexlibFieldType2CppTypeTraits(ft_string, true, autil::MultiString);
IndexlibFieldType2CppTypeTraits(ft_hash_64, true, autil::MultiUInt64);
IndexlibFieldType2CppTypeTraits(ft_time, true, autil::MultiUInt32);
IndexlibFieldType2CppTypeTraits(ft_date, true, autil::MultiUInt32);
IndexlibFieldType2CppTypeTraits(ft_timestamp, true, autil::MultiUInt64);
#undef IndexlibFieldType2CppTypeTraits

// cpp type --> indexlib field type utility: CppType2IndexlibFieldType
template <typename T>
struct CppType2IndexlibFieldType {
    static FieldType GetFieldType() { return ft_unknown; }
    static bool IsMultiValue() { return false; }
};

#define CppType2IndexlibFieldTypeTraits(indexlibFieldType, isMulti, cppType)                                           \
    template <>                                                                                                        \
    struct CppType2IndexlibFieldType<cppType> {                                                                        \
        static FieldType GetFieldType() { return indexlibFieldType; }                                                  \
        static bool IsMultiValue() { return isMulti; }                                                                 \
    };

CppType2IndexlibFieldTypeTraits(ft_int8, false, int8_t);
CppType2IndexlibFieldTypeTraits(ft_int16, false, int16_t);
CppType2IndexlibFieldTypeTraits(ft_int32, false, int32_t);
CppType2IndexlibFieldTypeTraits(ft_int64, false, int64_t);
CppType2IndexlibFieldTypeTraits(ft_uint8, false, uint8_t);
CppType2IndexlibFieldTypeTraits(ft_uint16, false, uint16_t);
CppType2IndexlibFieldTypeTraits(ft_uint32, false, uint32_t);
CppType2IndexlibFieldTypeTraits(ft_uint64, false, uint64_t);
CppType2IndexlibFieldTypeTraits(ft_float, false, float);
CppType2IndexlibFieldTypeTraits(ft_double, false, double);
CppType2IndexlibFieldTypeTraits(ft_string, false, autil::MultiChar);
CppType2IndexlibFieldTypeTraits(ft_hash_128, false, autil::uint128_t);

CppType2IndexlibFieldTypeTraits(ft_int8, true, autil::MultiInt8);
CppType2IndexlibFieldTypeTraits(ft_int16, true, autil::MultiInt16);
CppType2IndexlibFieldTypeTraits(ft_int32, true, autil::MultiInt32);
CppType2IndexlibFieldTypeTraits(ft_int64, true, autil::MultiInt64);
CppType2IndexlibFieldTypeTraits(ft_uint8, true, autil::MultiUInt8);
CppType2IndexlibFieldTypeTraits(ft_uint16, true, autil::MultiUInt16);
CppType2IndexlibFieldTypeTraits(ft_uint32, true, autil::MultiUInt32);
CppType2IndexlibFieldTypeTraits(ft_uint64, true, autil::MultiUInt64);
CppType2IndexlibFieldTypeTraits(ft_float, true, autil::MultiFloat);
CppType2IndexlibFieldTypeTraits(ft_double, true, autil::MultiDouble);
CppType2IndexlibFieldTypeTraits(ft_string, true, autil::MultiString);
#undef CppType2IndexlibFieldTypeTraits

#define NUMERIC_FIELD_TYPE_MACRO_HELPER(MACRO)                                                                         \
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

} // namespace indexlib
