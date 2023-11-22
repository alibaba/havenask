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
#include "suez/turing/common/CommonDefine.h"

const std::string DEFAULT_SRC = "_proxy_internal";
const std::string kReturnFieldsSep = ";";
const std::string kAliasSep = ":";
const std::string kDefaultValuesNode = "default_values";
const std::string kOrderByNode = "orderby";
const std::string kDistinctNode = "distinct";
const std::string kReturnFieldsNode = "return_fields";
const std::string kFilterNode = "filter";
const std::string kMaxReturnCountNode = "max_return_count";
const std::string kTableNameNode = "table_name";
const std::string kPkNode = "pk";
const std::string kPkeySplitNode = "pkey_split";
const std::string kLeftQueryNode = "left_query";
const std::string kRightQueryNode = "right_query";
const std::string kJoinKeyNode = "join_key";
const std::string kDefaultMultiConfig = "__default_multicall_config__";
const std::string kSuezTuringDefaultServerName = "suez_turing";

namespace suez {
namespace turing {

const std::string ITEM_ID_GROUP_NAME = "__@_item_id_@__";
const std::string ITEM_ID_FIELD_NAME = "__@_item_id_@__";
const std::string MATCHDOC_POS_FIELD_NAME = "__@_matchdoc_pos_@__";

// multi graph key definitions, for rtp
const std::string SEARCH_GRAPH = "search";
const std::string DEBUG_GRAPH = "debug";
const std::string PROXY_GRAPH = "proxy";

const std::string RUNMETA_PREFIX = "RUN_METADATA_";

} // namespace turing
} // namespace suez
