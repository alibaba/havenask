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

#include <limits>
#include <string>

namespace indexlib::index {
static constexpr const char KKV_INDEX_PATH[] = "index";
inline const std::string KKV_INDEX_TYPE_STR = "kkv";
static constexpr const char PREFIX_KEY_FILE_NAME[] = "pkey";
static constexpr const char SUFFIX_KEY_FILE_NAME[] = "skey";
static constexpr const char KKV_VALUE_FILE_NAME[] = "value";
static constexpr const char KKV_INDEX_FORMAT_FILE[] = "kkv_index_format";
static constexpr const char KKV_RAW_KEY_INDEX_NAME[] = "raw_key";
static constexpr const char KKV_DROP_DELETE_KEY[] = "drop_delete_key";
static constexpr const char KKV_CURRENT_TIME_IN_SECOND[] = "current_time_in_sec";
} // namespace indexlib::index

namespace indexlib {
constexpr uint32_t UNINITIALIZED_EXPIRE_TIME = 0;
} // namespace indexlib

//////////////////////////////////////////////////////////////////////
// TODO: rm

namespace indexlibv2::index {
using indexlib::index::KKV_CURRENT_TIME_IN_SECOND;
using indexlib::index::KKV_DROP_DELETE_KEY;
using indexlib::index::KKV_INDEX_FORMAT_FILE;
using indexlib::index::KKV_INDEX_PATH;
using indexlib::index::KKV_INDEX_TYPE_STR;
using indexlib::index::KKV_RAW_KEY_INDEX_NAME;
using indexlib::index::KKV_VALUE_FILE_NAME;
using indexlib::index::PREFIX_KEY_FILE_NAME;
using indexlib::index::SUFFIX_KEY_FILE_NAME;

static constexpr uint32_t INVALID_SKEY_OFFSET = std::numeric_limits<uint32_t>::max();
static constexpr uint32_t KKV_DEFAULT_SKIPLIST_THRESHOLD = 100;
static constexpr uint64_t INVALID_VALUE_OFFSET = std::numeric_limits<uint64_t>::max();
static constexpr uint64_t SKEY_ALL_DELETED_OFFSET = std::numeric_limits<uint64_t>::max() - 1;
static constexpr uint64_t ON_DISK_INVALID_SKEY_OFFSET = std::numeric_limits<uint64_t>::max();
static constexpr uint64_t MAX_CHUNK_DATA_LEN = (uint64_t)(1UL << 24) - 1;
static constexpr uint64_t KKV_CHUNK_SIZE_THRESHOLD = (uint64_t)4 * 1024;
static constexpr uint64_t MAX_KKV_VALUE_LEN = MAX_CHUNK_DATA_LEN - KKV_CHUNK_SIZE_THRESHOLD;
static constexpr const char KKV_SEGMENT_MEM_USE[] = "kkv_segment_mem_use";
static constexpr const char KKV_PKEY_MEM_USE[] = "kkv_pkey_mem_use";
static constexpr const char KKV_PKEY_MEM_RATIO[] = "kkv_pkey_mem_ratio";
static constexpr const char KKV_PKEY_COUNT[] = "pkey_count";
static constexpr const char KKV_SKEY_COUNT[] = "skey_count";
static constexpr const char KKV_MAX_VALUE_LEN[] = "max_value_len";
static constexpr const char KKV_MAX_SKEY_COUNT[] = "max_skey_count";
static constexpr const char KKV_COMPRESS_RATIO_GROUP_NAME[] = "kkv_compress_ratio";
static constexpr const char KKV_SKEY_COMPRESS_RATIO_NAME[] = "kkv_skey_compress_ratio";
static constexpr const char KKV_VALUE_COMPRESS_RATIO_NAME[] = "kkv_value_compress_ratio";
static constexpr const char KKV_SKEY_MEM_USE[] = "kkv_skey_mem_use";
static constexpr const char KKV_VALUE_MEM_USE[] = "kkv_value_mem_use";
static constexpr const char KKV_SKEY_VALUE_MEM_RATIO[] = "kkv_skey_value_mem_ratio";

} // namespace indexlibv2::index
