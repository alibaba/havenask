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

namespace isearch {
namespace common {

constexpr char INDEXVERSION_REF[] = "_@_build_in_indexversion";
constexpr char FULLVERSION_REF[] = "_@_build_in_fullversion";
constexpr char IP_REF[] = "_@_build_in_ip";

constexpr char IS_EVALUATED_REFERENCE[] = "_@_build_in_is_evaluated_reference";
constexpr char HASH_ID_REF[] = "_@_build_in_hashid";
constexpr char SUB_HASH_ID_REF[] = "_@_build_in_sub_hashid";
constexpr char CLUSTER_ID_REF[] = "_@_build_in_clusterid";
constexpr char SUB_SIMPLE_MATCH_DATA_REF[] = "_@_build_in_sub_simple_match_data";
constexpr char DISTINCT_INFO_REF[] = "_@_build_in_distinct_info";

constexpr char SUB_DOC_REF_PREFIX[] = "_@_sub_doc_";

constexpr char GROUP_KEY_REF[] = "group_key";

constexpr char HA3_DEFAULT_GROUP[] = "ha3_default_group";
constexpr char HA3_GLOBAL_INFO_GROUP[] = "ha3_global_info_group";
constexpr char CLUSTER_ID_REF_GROUP[] = "_@_build_in_clusterid_group";
constexpr char HA3_MATCHVALUE_GROUP[] = "ha3_matchvalue_group";
constexpr char HA3_MATCHDATA_GROUP[] = "ha3_matchdata_group";
constexpr char HA3_SUBMATCHDATA_GROUP[] = "ha3_submatchdata_group";
constexpr char HA3_RESULT_TENSOR_NAME[] = "ha3_result";
constexpr char HA3_MATCHDATAFIELD_MATCHEDROWINFO[] = "matchedRowId";
constexpr char HA3_MATCHDATAFIELD_MATCHEDROWINFOTYPE[] = "MultiUint64";

constexpr char OP_DEBUG_KEY[] = "op_debug";

constexpr char SPECIAL_CATALOG_LIST[] = "specialCatalogList";

constexpr char HA_RESERVED_METHOD[] =  "ha_reserved_method";
constexpr char HA_RESERVED_PID[] =  "ha_reserved_pid";
constexpr char HA_RESERVED_APPNAME[] =  "ha3:";
constexpr char HA_RESERVED_BENCHMARK_KEY[] =  "t";
constexpr char HA_RESERVED_BENCHMARK_VALUE_1[] =  "1";
constexpr char HA_RESERVED_BENCHMARK_VALUE_2[] =  "2";

constexpr char QINFO_KEY_FG_USER[] = "fg_user";
constexpr char QINFO_KEY_RN[] = "rn";
constexpr char INNER_DOCID_FIELD_NAME[] = "__inner_docid";

} // namespace common
} // namespace isearch
