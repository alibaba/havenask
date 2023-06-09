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

#include <cstdint>

// global_enum_namespace will public to global namespace by indexlib.h, todo: rm it
namespace indexlib::global_enum_namespace {
enum FieldType {
    ft_text = 0,
    ft_string = 1,
    ft_enum = 2,
    ft_integer = 3,
    ft_int32 = ft_integer,
    ft_float = 4,
    ft_long = 5,
    ft_int64 = ft_long,
    ft_time = 6,
    ft_location = 7, // only used for spatial
    ft_polygon = 8,  // only used for spatial
    ft_line = 9,
    ft_online = 10,
    ft_property = 11,
    ft_uint32 = 12,
    ft_uint64 = 13,
    ft_hash_64 = 14,  // uint64,  only used for primary key
    ft_hash_128 = 15, // uint128, only used for primary key
    ft_int8 = 16,
    ft_uint8 = 17,
    ft_int16 = 18,
    ft_uint16 = 19,
    ft_double = 20,
    ft_raw = 21,
    ft_unknown = 22,
    ft_byte1 = 23,
    ft_byte2 = 24,
    ft_byte3 = 25,
    ft_byte4 = 26,
    ft_byte5 = 27,
    ft_byte6 = 28,
    ft_byte7 = 29,
    ft_byte8 = 30,
    ft_fp8 = 31,
    ft_fp16 = 32,
    ft_date = 33,
    ft_timestamp = 34
};

} // namespace indexlib::global_enum_namespace

namespace indexlib {
#pragma pack(push)
#pragma pack(1)
struct byte1_t {
    uint64_t data : 1 * 8;
};
struct byte2_t {
    uint64_t data : 2 * 8;
};
struct byte3_t {
    uint64_t data : 3 * 8;
};
struct byte4_t {
    uint64_t data : 4 * 8;
};
struct byte5_t {
    uint64_t data : 5 * 8;
};
struct byte6_t {
    uint64_t data : 6 * 8;
};
struct byte7_t {
    uint64_t data : 7 * 8;
};
struct byte8_t {
    uint64_t data : 8 * 8;
};
#pragma pack(pop)

using namespace ::indexlib::global_enum_namespace;
} // namespace indexlib

// TODO: rm, publish to global namespace
using namespace ::indexlib::global_enum_namespace;
