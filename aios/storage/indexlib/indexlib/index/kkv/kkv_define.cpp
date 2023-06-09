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
#include "indexlib/index/kkv/kkv_define.h"

namespace indexlib { namespace index {

const std::string KKV_SEGMENT_MEM_USE = "kkv_segment_mem_use";
const std::string KKV_PKEY_MEM_USE = "kkv_pkey_mem_use";
const std::string KKV_PKEY_COUNT = "pkey_count";
const std::string KKV_SKEY_COUNT = "skey_count";
const std::string KKV_MAX_VALUE_LEN = "max_value_len";
const std::string KKV_MAX_SKEY_COUNT = "max_skey_count";
const std::string KKV_COMPRESS_RATIO_GROUP_NAME = "kkv_compress_ratio";

const std::string KKV_SKEY_MEM_USE = "kkv_skey_mem_use";
const std::string KKV_VALUE_MEM_USE = "kkv_value_mem_use";
const std::string KKV_SKEY_VALUE_MEM_RATIO = "kkv_skey_value_mem_ratio";

const std::string KKV_PKEY_DENSE_TABLE_NAME = "dense";
const std::string KKV_PKEY_CUCKOO_TABLE_NAME = "cuckoo";
const std::string KKV_PKEY_SEPARATE_CHAIN_TABLE_NAME = "separate_chain";
const std::string KKV_PKEY_ARRAR_TABLE_NAME = "array";

}} // namespace indexlib::index
