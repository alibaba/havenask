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
#include <assert.h>
#include <functional>
#include <limits>
#include <sstream>
#include <stdint.h>
#include <vector>

#include "autil/MultiValueType.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/MatchValue.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kv/Constant.h"
#include "indexlib/index/operation_log/Constant.h"
#include "indexlib/index/primary_key/Types.h"
#include "indexlib/index/source/Constant.h"
#include "indexlib/index/summary/Constant.h"

// indexlib namespace
namespace indexlib {
typedef int32_t regionid_t;
inline const std::string DEFAULT_HASH_ID_FIELD_NAME = "_doc_hash_id_";
inline const std::string DEFAULT_REGIONNAME = "__DEFAULT_INDEX_REGION__";
inline const std::string OPTIMIZE_MERGE_STRATEGY_STR = "optimize";
constexpr FieldType VIRTUAL_TIMESTAMP_FIELD_TYPE = ft_int64;
constexpr InvertedIndexType VIRTUAL_TIMESTAMP_INDEX_TYPE = it_number_int64;
static constexpr regionid_t INVALID_REGIONID = (regionid_t)-1;
static constexpr regionid_t DEFAULT_REGIONID = (regionid_t)0;
enum TableType {
    tt_index,
    tt_kv,
    tt_kkv,
    tt_rawfile,
    tt_linedata,
    tt_customized,
    tt_orc,
};

} // namespace indexlib

namespace indexlib::index {
inline const std::string TIME_TO_NOW_FUNCTION = "time_to_now";
inline const std::string VIRTUAL_TIMESTAMP_FIELD_NAME = "virtual_timestamp_field";
inline const std::string VIRTUAL_TIMESTAMP_INDEX_NAME = "virtual_timestamp_index";
} // namespace indexlib::index
