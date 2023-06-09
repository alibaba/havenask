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
#include "indexlib/index/kv/kv_define.h"

namespace indexlib { namespace index {

const std::string KV_KEY_COUNT = "key_count";
const std::string KV_SEGMENT_MEM_USE = "kv_segment_mem_use";
const std::string KV_HASH_MEM_USE = "kv_hash_mem_use";
const std::string KV_HASH_OCCUPANCY_PCT = "kv_hash_occupancy_pct";
const std::string KV_VALUE_MEM_USE = "kv_value_mem_use";
const std::string KV_KEY_VALUE_MEM_RATIO = "kv_key_value_mem_ratio";
const std::string KV_WRITER_TYPEID = "kv_writer_typeid";
const std::string KV_KEY_DELETE_COUNT = "kv_key_delete_count";

const std::string KV_COMPRESS_RATIO_GROUP_NAME = "kv_compress_ratio";

}} // namespace indexlib::index
