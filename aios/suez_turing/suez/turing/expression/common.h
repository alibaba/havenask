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
#include <functional>
#include <iosfwd>
#include <map>
#include <string>
#include <system_error>

#include "autil/HashAlgorithm.h"
#include "autil/LongHashValue.h"
#include "autil/MultiValueType.h"
#include "matchdoc/ValueType.h"

namespace suez {
namespace turing {

constexpr char CAVA_INNER_METHOD_NAME[] = "inner_process";
constexpr char CAVA_INNER_METHOD_SEP[] = "_";
constexpr char CAVA_SCORER_CODE_KEY[] = "scorer_code_key";
constexpr char CAVA_DEFAULT_SCORER_CODE_KEY[] = "scorer_code";
constexpr char CAVA_DEFAULT_SCORER_CLASS_NAME[] = "default_class_name";
constexpr char CAVA_SCORER_NAME_KEY[] = "scorer_name_key";

constexpr char BUILD_IN_REFERENCE_PREFIX[] = "_@_build_in_";
constexpr char PROVIDER_VAR_NAME_PREFIX[] = "_@_user_data_";
constexpr char IS_EVALUATED_REFERENCE[] = "_@_build_in_is_evaluated_reference";
constexpr char SCORE_REF[] = "_@_build_in_score";
constexpr char BUILD_IN_JOIN_DOCID_REF_PREIX[] = "_@_join_docid_";
constexpr char BUILD_IN_JOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX[] = "_@_join_docid_";
constexpr char BUILD_IN_SUBJOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX[] = "_@_subjoin_docid_";
constexpr char RAW_PRIMARYKEY_REF[] = "_@_build_in_rawpk";
constexpr char PRIMARYKEY_REF[] = "_@_build_in_pkattr";

constexpr uint32_t INVALID_REFERENCE_ID = (uint32_t)-1;

typedef autil::uint128_t primarykey_t;
typedef double score_t;
typedef std::map<std::string, std::string> KeyValueMap;
typedef matchdoc::BuiltinType VariableType;

// TODO: remove
#define SUEZ_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;

#define JSONIZE(json, configName, value) json.Jsonize(configName, value, value)

} // namespace turing
} // namespace suez

#define TURING_LOG_AND_TRACE(level, format, args...)                                                                   \
    {                                                                                                                  \
        REQUEST_TRACE(level, format, ##args);                                                                          \
        AUTIL_LOG(level, format, ##args);                                                                              \
    }

#define TYPE_CONVERT_HELPER(type) constexpr matchdoc::BuiltinType vt_##type = matchdoc::bt_##type;

TYPE_CONVERT_HELPER(unknown);
TYPE_CONVERT_HELPER(int8);
TYPE_CONVERT_HELPER(int16);
TYPE_CONVERT_HELPER(int32);
TYPE_CONVERT_HELPER(int64);
TYPE_CONVERT_HELPER(uint8);
TYPE_CONVERT_HELPER(uint16);
TYPE_CONVERT_HELPER(uint32);
TYPE_CONVERT_HELPER(uint64);
TYPE_CONVERT_HELPER(float);
TYPE_CONVERT_HELPER(double);
TYPE_CONVERT_HELPER(string);
TYPE_CONVERT_HELPER(bool);
TYPE_CONVERT_HELPER(hash_128);

static const matchdoc::BuiltinType vt_func_template = matchdoc::bt_user_type1;
static const matchdoc::BuiltinType vt_unknown_max_type = matchdoc::bt_user_type2;
static const matchdoc::BuiltinType vt_hamming_type = matchdoc::bt_user_type3;
static const matchdoc::BuiltinType vt_type_count = matchdoc::bt_max;
