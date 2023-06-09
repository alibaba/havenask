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
#include <stdint.h>

namespace indexlib::index {
} // namespace indexlib::index

namespace indexlib::enum_namespace {
enum CompressTypeFlag {
    ct_other = 0,
    ct_int8 = 1,
    ct_fp16 = 2,
};
} // namespace indexlib::enum_namespace

// global_enum_namespace will public to global namespace by indexlib.h, todo: rm it
namespace indexlib::global_enum_namespace {
enum AttrType {
    AT_INT32 = 1,
    AT_UINT32,
    AT_INT64,
    AT_UINT64,
    AT_INT8,
    AT_UINT8,
    AT_INT16,
    AT_UINT16,
    AT_FLOAT,
    AT_DOUBLE,
    AT_STRING,
    AT_HASH_64,
    AT_HASH_128,
    AT_VIRTUAL_INT16,
    AT_VIRTUAL_INT32,
    AT_VIRTUAL_INT64,
    AT_VIRTUAL_UINT16,
    AT_VIRTUAL_UINT32,
    AT_VIRTUAL_UINT64,
    AT_UNKNOWN
};
enum InvertedIndexType {
    it_text,
    it_pack,
    it_expack,
    it_string,
    it_enum,
    it_property,
    it_number,      // use external -- legacy
    it_number_int8, // 8 - 64 use internal, type transform in InitIndexWriters
    it_number_uint8,
    it_number_int16,
    it_number_uint16,
    it_number_int32,
    it_number_uint32,
    it_number_int64,
    it_number_uint64,
    it_primarykey64,
    it_primarykey128,
    it_trie,
    it_spatial,
    it_kv,
    it_kkv,
    it_customized,
    it_date,
    it_datetime = it_date,
    it_range,
    it_unknown
};
} // namespace indexlib::global_enum_namespace

namespace indexlib {
typedef uint32_t indexid_t;
typedef int64_t dictvalue_t;
typedef uint8_t section_fid_t;
typedef uint16_t section_len_t;
typedef uint16_t section_weight_t;
typedef uint64_t dictkey_t;

enum IndexStatus {
    is_normal = 0x01,
    is_disable = 0x02,
    is_deleted = 0x04,

    is_normal_or_disable = is_normal | is_disable,
    is_disable_or_deleted = is_disable | is_deleted,
    is_all = is_normal | is_disable | is_deleted,
};
enum HashFunctionType {
    hft_int64,
    hft_uint64,
    hft_murmur,
    hft_unknown,
};

using namespace ::indexlib::enum_namespace;
using namespace ::indexlib::global_enum_namespace;
} // namespace indexlib

//////////////////////////////////////////////////////////////////////
// TODO: rm
namespace indexlibv2::index {
} // namespace indexlibv2::index

namespace indexlibv2 {
using indexlib::indexid_t;

using namespace ::indexlib::enum_namespace;
using namespace ::indexlib::global_enum_namespace;
} // namespace indexlibv2
